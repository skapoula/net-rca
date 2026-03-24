#!/usr/bin/env python3
"""Generate LangSmith performance reports from trace.json artifacts.

Reads a traces-phaseN.json file (array of trace envelopes written by
LocalTraceCallbackHandler) and produces per-incident and phase-level
performance reports in both JSON and Markdown formats.

Usage:
    python3 scripts/generate_perf_report.py \\
        --traces /path/to/traces-phase4.json \\
        --results-dir /path/to/test-results/... \\
        --phase 4

Output files (all written to --results-dir):
    lang_perf-report-{incident_id}.json
    lang_perf-report-{incident_id}.md
    lang_perf-report-phase{N}.json
    lang_perf-report-phase{N}.md
"""

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

# ── Token budget constants (mirrors config.py TriageAgentConfig) ─────────────
_SECTION_BUDGETS: dict[str, int] = {
    "infra": 250,
    "dag": 500,
    "metrics": 300,
    "logs": 800,
    "traces": 300,
}
_TOTAL_PROMPT_BUDGET: int = sum(_SECTION_BUDGETS.values())  # 2150
_LLM_RESPONSE_BUDGET: int = 400  # matches config.llm_max_tokens default


# ── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class NodeTiming:
    """One matched chain_start / chain_end span for a LangGraph node."""
    node_name: str
    run_id: str
    parent_run_id: str | None
    latency_ms: float | None
    status: str  # "ok" | "error" | "incomplete"


@dataclass
class LLMCall:
    """One matched llm_start / llm_end span."""
    run_id: str
    parent_run_id: str | None
    model_name: str
    latency_ms: float | None
    token_count_estimate: int  # response tokens only (4-char heuristic)
    prompt_excerpt: str
    response_excerpt: str


@dataclass
class ErrorEvent:
    """One chain_error event."""
    run_id: str
    node_name: str | None
    error_message: str


@dataclass
class IncidentReport:
    """Per-incident performance data extracted from one trace envelope."""
    incident_id: str
    workflow_start_iso: str
    workflow_end_iso: str
    total_latency_ms: float | None
    event_count: int
    node_timings: list[NodeTiming] = field(default_factory=list)
    llm_calls: list[LLMCall] = field(default_factory=list)
    errors: list[ErrorEvent] = field(default_factory=list)
    total_llm_response_tokens: int = 0
    llm_call_count: int = 0
    error_count: int = 0


@dataclass
class IncidentComparison:
    """Flat row for the cross-incident comparison table."""
    incident_id: str
    total_latency_ms: float | None
    total_llm_response_tokens: int
    llm_call_count: int
    error_count: int
    node_count: int


@dataclass
class PhaseReport:
    """Phase-level rollup across all incidents."""
    phase_label: str
    generated_iso: str
    incident_count: int
    incidents: list[IncidentReport] = field(default_factory=list)
    comparison_table: list[IncidentComparison] = field(default_factory=list)
    phase_total_latency_ms: float | None = None
    phase_avg_latency_ms: float | None = None
    phase_total_llm_response_tokens: int = 0
    phase_avg_llm_response_tokens: float = 0.0
    phase_error_count: int = 0
    slowest_incident_id: str | None = None
    fastest_incident_id: str | None = None
    slowest_node_across_phase: str | None = None
    slowest_node_latency_ms: float | None = None


# ── Parse layer ──────────────────────────────────────────────────────────────

def _parse_event_list(
    events: list[dict],
) -> tuple[list[NodeTiming], list[LLMCall], list[ErrorEvent]]:
    """Build typed event objects from a flat events array in a single pass."""
    chain_starts: dict[str, dict] = {}
    chain_ends: dict[str, dict] = {}
    chain_error_ids: set[str] = set()
    llm_starts: dict[str, dict] = {}
    llm_ends: dict[str, dict] = {}

    for ev in events:
        etype = ev.get("event_type", "")
        rid = ev.get("run_id", "")
        if etype == "chain_start":
            chain_starts[rid] = ev
        elif etype == "chain_end":
            chain_ends[rid] = ev
        elif etype == "chain_error":
            chain_error_ids.add(rid)
        elif etype == "llm_start":
            llm_starts[rid] = ev
        elif etype == "llm_end":
            llm_ends[rid] = ev

    node_timings: list[NodeTiming] = []
    for rid, start_ev in chain_starts.items():
        end_ev = chain_ends.get(rid, {})
        latency = end_ev.get("latency_ms")
        if rid in chain_error_ids:
            status = "error"
        elif rid not in chain_ends:
            status = "incomplete"
        else:
            status = "ok"
        node_timings.append(NodeTiming(
            node_name=start_ev.get("name") or "unknown",
            run_id=rid,
            parent_run_id=start_ev.get("parent_run_id"),
            latency_ms=latency,
            status=status,
        ))

    llm_calls: list[LLMCall] = []
    for rid, start_ev in llm_starts.items():
        end_ev = llm_ends.get(rid, {})
        llm_calls.append(LLMCall(
            run_id=rid,
            parent_run_id=start_ev.get("parent_run_id"),
            model_name=start_ev.get("name") or "LLM",
            latency_ms=end_ev.get("latency_ms"),
            token_count_estimate=end_ev.get("token_count_estimate") or 0,
            prompt_excerpt=start_ev.get("prompt_excerpt") or "",
            response_excerpt=end_ev.get("response_excerpt") or "",
        ))

    errors: list[ErrorEvent] = []
    for ev in events:
        if ev.get("event_type") == "chain_error" and ev.get("error"):
            rid = ev.get("run_id", "")
            errors.append(ErrorEvent(
                run_id=rid,
                node_name=chain_starts.get(rid, {}).get("name"),
                error_message=ev["error"],
            ))

    return node_timings, llm_calls, errors


def parse_trace(envelope: dict, index: int = 0) -> IncidentReport:
    """Convert one trace envelope dict into a typed IncidentReport."""
    incident_id = envelope.get("incident_id") or f"unknown-{index}"
    raw_events = envelope.get("events")
    if not isinstance(raw_events, list):
        raw_events = []

    node_timings, llm_calls, errors = _parse_event_list(raw_events)
    node_timings.sort(key=lambda n: n.latency_ms or 0, reverse=True)

    return IncidentReport(
        incident_id=incident_id,
        workflow_start_iso=envelope.get("workflow_start_iso") or "",
        workflow_end_iso=envelope.get("workflow_end_iso") or "",
        total_latency_ms=envelope.get("total_latency_ms"),
        event_count=envelope.get("event_count") or len(raw_events),
        node_timings=node_timings,
        llm_calls=llm_calls,
        errors=errors,
        total_llm_response_tokens=sum(c.token_count_estimate for c in llm_calls),
        llm_call_count=len(llm_calls),
        error_count=len(errors),
    )


# ── Analyze layer ─────────────────────────────────────────────────────────────

def build_comparison_table(incidents: list[IncidentReport]) -> list[IncidentComparison]:
    """Build the cross-incident comparison rows."""
    return [
        IncidentComparison(
            incident_id=i.incident_id,
            total_latency_ms=i.total_latency_ms,
            total_llm_response_tokens=i.total_llm_response_tokens,
            llm_call_count=i.llm_call_count,
            error_count=i.error_count,
            node_count=len(i.node_timings),
        )
        for i in incidents
    ]


def build_phase_report(phase_label: str, incidents: list[IncidentReport]) -> PhaseReport:
    """Aggregate all incidents into a phase-level rollup."""
    generated_iso = datetime.now(UTC).isoformat()
    comparison = build_comparison_table(incidents)
    latencies = [i.total_latency_ms for i in incidents if i.total_latency_ms is not None]

    slowest = max(incidents, key=lambda i: i.total_latency_ms or 0, default=None)
    fastest = min(
        [i for i in incidents if i.total_latency_ms is not None],
        key=lambda i: i.total_latency_ms,  # type: ignore[arg-type]
        default=None,
    )

    all_nodes = [(n, i) for i in incidents for n in i.node_timings if n.latency_ms is not None]
    slowest_node = max(all_nodes, key=lambda x: x[0].latency_ms or 0, default=None)

    total_tokens = sum(i.total_llm_response_tokens for i in incidents)

    return PhaseReport(
        phase_label=phase_label,
        generated_iso=generated_iso,
        incident_count=len(incidents),
        incidents=incidents,
        comparison_table=comparison,
        phase_total_latency_ms=sum(latencies) if latencies else None,
        phase_avg_latency_ms=sum(latencies) / len(latencies) if latencies else None,
        phase_total_llm_response_tokens=total_tokens,
        phase_avg_llm_response_tokens=total_tokens / len(incidents) if incidents else 0.0,
        phase_error_count=sum(i.error_count for i in incidents),
        slowest_incident_id=slowest.incident_id if slowest else None,
        fastest_incident_id=fastest.incident_id if fastest else None,
        slowest_node_across_phase=slowest_node[0].node_name if slowest_node else None,
        slowest_node_latency_ms=slowest_node[0].latency_ms if slowest_node else None,
    )


# ── Render layer — JSON ───────────────────────────────────────────────────────

def render_incident_json(report: IncidentReport) -> dict:
    """Serialize an IncidentReport to the lang_perf-report-{id}.json schema."""
    within_budget = report.total_llm_response_tokens <= _LLM_RESPONSE_BUDGET
    return {
        "report_type": "incident",
        "incident_id": report.incident_id,
        "workflow_start_iso": report.workflow_start_iso,
        "workflow_end_iso": report.workflow_end_iso,
        "total_latency_ms": report.total_latency_ms,
        "event_count": report.event_count,
        "llm_call_count": report.llm_call_count,
        "error_count": report.error_count,
        "total_llm_response_tokens": report.total_llm_response_tokens,
        "node_timings": [
            {
                "node_name": n.node_name,
                "run_id": n.run_id,
                "parent_run_id": n.parent_run_id,
                "latency_ms": n.latency_ms,
                "status": n.status,
            }
            for n in report.node_timings
        ],
        "llm_calls": [
            {
                "run_id": c.run_id,
                "parent_run_id": c.parent_run_id,
                "model_name": c.model_name,
                "latency_ms": c.latency_ms,
                "token_count_estimate": c.token_count_estimate,
                "prompt_excerpt": c.prompt_excerpt,
                "response_excerpt": c.response_excerpt,
            }
            for c in report.llm_calls
        ],
        "errors": [
            {"run_id": e.run_id, "node_name": e.node_name, "error_message": e.error_message}
            for e in report.errors
        ],
        "token_budget_reference": {
            **{f"{k}_budget": v for k, v in _SECTION_BUDGETS.items()},
            "total_prompt_budget": _TOTAL_PROMPT_BUDGET,
            "llm_response_budget": _LLM_RESPONSE_BUDGET,
            "actual_llm_response_tokens": report.total_llm_response_tokens,
            "response_within_budget": within_budget,
        },
    }


def render_phase_json(report: PhaseReport) -> dict:
    """Serialize a PhaseReport to the lang_perf-report-phaseN.json schema."""
    return {
        "report_type": "phase",
        "phase_label": report.phase_label,
        "generated_iso": report.generated_iso,
        "incident_count": report.incident_count,
        "phase_total_latency_ms": report.phase_total_latency_ms,
        "phase_avg_latency_ms": report.phase_avg_latency_ms,
        "phase_total_llm_response_tokens": report.phase_total_llm_response_tokens,
        "phase_avg_llm_response_tokens": report.phase_avg_llm_response_tokens,
        "phase_error_count": report.phase_error_count,
        "slowest_incident_id": report.slowest_incident_id,
        "fastest_incident_id": report.fastest_incident_id,
        "slowest_node_across_phase": report.slowest_node_across_phase,
        "slowest_node_latency_ms": report.slowest_node_latency_ms,
        "comparison_table": [
            {
                "incident_id": r.incident_id,
                "total_latency_ms": r.total_latency_ms,
                "total_llm_response_tokens": r.total_llm_response_tokens,
                "llm_call_count": r.llm_call_count,
                "error_count": r.error_count,
                "node_count": r.node_count,
            }
            for r in report.comparison_table
        ],
        "incidents": [render_incident_json(i) for i in report.incidents],
    }


# ── Render layer — Markdown ───────────────────────────────────────────────────

def _fmt_ms(ms: float | None) -> str:
    return "n/a" if ms is None else f"{ms:.0f}"


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    sep = ["-" * max(len(h), 4) for h in headers]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(sep) + " |",
        *("| " + " | ".join(row) + " |" for row in rows),
    ]
    return "\n".join(lines)


def render_incident_md(report: IncidentReport) -> str:
    """Build the Markdown string for lang_perf-report-{id}.md."""
    within = report.total_llm_response_tokens <= _LLM_RESPONSE_BUDGET
    budget_status = "WITHIN BUDGET" if within else "OVER BUDGET"

    # Agent timings table (sorted slowest-first by parse_trace)
    timing_rows = [
        [n.node_name, _fmt_ms(n.latency_ms), n.status]
        for n in report.node_timings
    ] or [["(no nodes recorded)", "—", "—"]]

    # LLM summary table
    llm_rows = [
        [str(i), c.model_name, _fmt_ms(c.latency_ms),
         str(c.token_count_estimate) if c.token_count_estimate else "—"]
        for i, c in enumerate(report.llm_calls, 1)
    ] or [["(no LLM calls recorded)", "—", "—", "—"]]

    # Error table
    error_rows = [
        [e.node_name or "unknown", e.error_message[:120]]
        for e in report.errors
    ]

    lines = [
        f"# Performance Report — Incident {report.incident_id}",
        "",
        f"**Workflow:** {report.workflow_start_iso or 'n/a'} → {report.workflow_end_iso or 'n/a'}  ",
        f"**Total latency:** {_fmt_ms(report.total_latency_ms)} ms  ",
        f"**Events captured:** {report.event_count}  ",
        f"**LLM calls:** {report.llm_call_count}  ",
        f"**Errors:** {report.error_count}",
        "",
        "---",
        "",
        "## Agent Node Timings",
        "",
        _md_table(["Node", "Latency (ms)", "Status"], timing_rows),
        "",
        "---",
        "",
        "## LLM Calls",
        "",
        _md_table(["#", "Model", "Latency (ms)", "Response Tokens"], llm_rows),
    ]

    for i, c in enumerate(report.llm_calls, 1):
        if c.prompt_excerpt:
            lines += [
                "",
                f"### Call {i} — Prompt Excerpt",
                "",
                "```",
                c.prompt_excerpt[:500],
                "```",
            ]
        if c.response_excerpt:
            lines += [
                "",
                f"### Call {i} — Response Excerpt",
                "",
                "```",
                c.response_excerpt[:1000],
                "```",
            ]

    lines += [
        "",
        "---",
        "",
        "## Token Budget Reference",
        "",
        "> Note: `token_count_estimate` measures LLM **response** tokens only (4-char heuristic).",
        "> Prompt-side per-section budgets are shown as reference only.",
        "",
        _md_table(
            ["Section", "Budget (tokens)"],
            [[k, str(v)] for k, v in _SECTION_BUDGETS.items()]
            + [["**Total prompt budget**", f"**{_TOTAL_PROMPT_BUDGET}**"]],
        ),
        "",
        f"| LLM response budget | {_LLM_RESPONSE_BUDGET} |",
        f"| Actual response tokens | **{report.total_llm_response_tokens}** |",
        f"| Status | **{budget_status}** |",
        "",
        "---",
        "",
        "## Errors",
        "",
    ]
    if error_rows:
        lines += [_md_table(["Node", "Error"], error_rows)]
    else:
        lines += ["No errors recorded."]

    return "\n".join(lines) + "\n"


def render_phase_md(report: PhaseReport) -> str:
    """Build the Markdown string for lang_perf-report-phaseN.md."""
    comparison_rows = [
        [
            r.incident_id[:12] + "…",
            _fmt_ms(r.total_latency_ms),
            str(r.total_llm_response_tokens),
            str(r.llm_call_count),
            str(r.error_count),
            str(r.node_count),
        ]
        for r in report.comparison_table
    ] or [["(no incidents)", "—", "—", "—", "—", "—"]]

    lines = [
        f"# Performance Report — Phase {report.phase_label}",
        "",
        f"**Generated:** {report.generated_iso}  ",
        f"**Incidents:** {report.incident_count}",
        "",
        "---",
        "",
        "## Phase Summary",
        "",
        _md_table(
            ["Metric", "Value"],
            [
                ["Total latency (all incidents)", f"{_fmt_ms(report.phase_total_latency_ms)} ms"],
                ["Average latency per incident", f"{_fmt_ms(report.phase_avg_latency_ms)} ms"],
                ["Total LLM response tokens", str(report.phase_total_llm_response_tokens)],
                ["Avg LLM response tokens / incident",
                 f"{report.phase_avg_llm_response_tokens:.0f}"],
                ["Total errors", str(report.phase_error_count)],
                ["Slowest incident", report.slowest_incident_id or "n/a"],
                ["Fastest incident", report.fastest_incident_id or "n/a"],
                ["Slowest node (across phase)",
                 f"{report.slowest_node_across_phase or 'n/a'} "
                 f"({_fmt_ms(report.slowest_node_latency_ms)} ms)"],
            ],
        ),
        "",
        "---",
        "",
        "## Cross-Incident Comparison",
        "",
        _md_table(
            ["Incident ID", "Latency (ms)", "LLM Tokens", "LLM Calls", "Errors", "Nodes"],
            comparison_rows,
        ),
        "",
        "---",
        "",
        "## Per-Incident Detail",
        "",
    ]

    for inc in report.incidents:
        lines += [render_incident_md(inc), "", "---", ""]

    return "\n".join(lines) + "\n"


# ── I/O helpers ───────────────────────────────────────────────────────────────

def load_traces(path: Path) -> list[dict]:
    """Load and validate a traces-phaseN.json file."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"ERROR: failed to read {path}: {exc}", file=sys.stderr)
        sys.exit(1)
    if not isinstance(data, list):
        print(f"ERROR: {path} is not a JSON array", file=sys.stderr)
        sys.exit(1)
    return data


def write_json(path: Path, data: dict) -> None:
    """Write a JSON file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    """Write a text file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate LangSmith performance reports from trace artifacts."
    )
    parser.add_argument("--traces", required=True, type=Path,
                        help="Path to traces-phaseN.json")
    parser.add_argument("--results-dir", required=True, type=Path,
                        help="Directory to write report files into")
    parser.add_argument("--phase", required=True,
                        help="Phase label (e.g. '2', '3', '4')")
    args = parser.parse_args()

    envelopes = load_traces(args.traces)
    results_dir: Path = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    incidents: list[IncidentReport] = []
    for idx, envelope in enumerate(envelopes):
        try:
            report = parse_trace(envelope, idx)
        except Exception as exc:
            print(f"WARNING: skipping envelope {idx}: {exc}", file=sys.stderr)
            continue

        incidents.append(report)
        write_json(results_dir / f"lang_perf-report-{report.incident_id}.json",
                   render_incident_json(report))
        write_text(results_dir / f"lang_perf-report-{report.incident_id}.md",
                   render_incident_md(report))

    phase_report = build_phase_report(args.phase, incidents)
    write_json(results_dir / f"lang_perf-report-phase{args.phase}.json",
               render_phase_json(phase_report))
    write_text(results_dir / f"lang_perf-report-phase{args.phase}.md",
               render_phase_md(phase_report))

    print(
        f"perf report: {len(incidents)} incident(s) → {results_dir} "
        f"[lang_perf-report-phase{args.phase}.{{json,md}}]"
    )


if __name__ == "__main__":
    main()
