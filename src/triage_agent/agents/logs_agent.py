"""NfLogsAgent: Per-NF log collection from Loki via MCP.

No LLM. Queries Loki for ERROR/WARN/FATAL logs and phase-specific patterns
from the candidate NF list provided by the DAG.

Two-path architecture:
  1. Upfront health check — probe MCP server /ready endpoint.
  2. If MCP reachable → fetch all logs via MCP client.
  3. If MCP unreachable → fetch all logs via direct Loki HTTP API.
"""

import asyncio
import logging
import re
from typing import Any

import httpx
from langsmith import traceable

from triage_agent.config import get_config
from triage_agent.mcp.client import MCPClient
from triage_agent.state import TriageState
from triage_agent.utils import count_tokens, extract_nf_from_pod_name, parse_loki_response, parse_timestamp, save_artifact

logger = logging.getLogger(__name__)



def wildcard_match(text: str, pattern: str) -> bool:
    """Case-insensitive wildcard matching. '*' matches any characters."""
    regex_pattern = pattern.replace("*", ".*")
    return bool(re.search(f"(?i){regex_pattern}", text))


def build_loki_queries(dag: dict[str, Any], core_namespace: str) -> list[str]:
    """Build LogQL queries for each NF: base ERROR/WARN/FATAL + phase-specific.

    Args:
        dag: DAG dict with 'all_nfs' and 'phases'.
        core_namespace: K8s namespace where 5G core NF pods run.

    Returns:
        List of LogQL query strings.
    """
    queries: list[str] = []
    for nf in dag["all_nfs"]:
        nf_lower = nf.lower()
        label_sel = f'{{k8s_namespace_name="{core_namespace}",k8s_pod_name=~".*{nf_lower}.*"}}'

        # Base query: all ERROR/WARN/FATAL logs
        queries.append(f'{label_sel} |~ "ERROR|WARN|FATAL"')

        # Phase-specific pattern queries
        for phase in dag["phases"]:
            if nf in phase.get("actors", []):
                queries.append(f'{label_sel} |~ "{phase["success_log"]}"')
                for pattern in phase.get("failure_patterns", []):
                    loki_pattern = pattern.replace("*", ".*")
                    queries.append(f'{label_sel} |~ "(?i){loki_pattern}"')

    return queries


def organize_and_annotate_logs(logs: list[dict[str, Any]], dag: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Organize logs by NF and annotate with matched phase/pattern."""
    organized: dict[str, list[dict[str, Any]]] = {}

    for log_entry in logs:
        nf = extract_nf_from_pod_name(log_entry["pod"])
        message = log_entry["message"]

        if nf not in organized:
            organized[nf] = []

        matched_phase: str | None = None
        matched_pattern: str | None = None

        for phase in dag["phases"]:
            for pattern in phase.get("failure_patterns", []):
                if wildcard_match(message, pattern):
                    matched_phase = phase["phase_id"]
                    matched_pattern = pattern
                    break
            if matched_phase:
                break

        organized[nf].append({
            "level": log_entry["level"],
            "message": message,
            "timestamp": log_entry["timestamp"],
            "matched_phase": matched_phase,
            "matched_pattern": matched_pattern,
        })

    return organized


# --- MCP health check ---


async def _check_mcp_available() -> bool:
    """Lightweight MCP health check: probe Loki /ready via MCP server.

    Returns True if MCP server is reachable and Loki reports ready.
    Returns False on any error (connection refused, timeout, etc.).
    """
    try:
        async with MCPClient() as client:
            return await client.health_check_loki()
    except Exception:
        return False


# --- Two fetch paths: MCP and direct Loki HTTP ---


async def _fetch_loki_logs(
    queries: list[str],
    start: int,
    end: int,
) -> list[dict[str, Any]]:
    """Fetch logs from Loki via MCP client.

    Args:
        queries: LogQL query strings to execute.
        start: Unix epoch seconds for window start.
        end: Unix epoch seconds for window end.

    Returns:
        Flat list of log entries across all successful queries.

    Raises:
        Exception: Any MCP/connection/timeout failure.
    """
    if not queries:
        return []

    results: list[dict[str, Any]] = []
    async with MCPClient() as client:
        for query in queries:
            logs = await client.query_loki(query, start=start, end=end)
            results.extend(logs)

    # Normalize pod field: MCPClient reads labels["pod"] which may be empty
    # when Loki uses k8s-style labels (k8s_pod_name).
    for entry in results:
        if not entry.get("pod"):
            entry["pod"] = entry.get("labels", {}).get("k8s_pod_name", "")

    return results


async def _fetch_loki_logs_direct(
    queries: list[str],
    start: int,
    end: int,
) -> list[dict[str, Any]]:
    """Fetch logs directly from Loki HTTP API.

    Queries Loki's query_range endpoint via httpx, bypassing the MCP server.
    Uses the same response parsing as MCPClient to produce identical output.

    Args:
        queries: LogQL query strings to execute.
        start: Unix epoch seconds for window start.
        end: Unix epoch seconds for window end.

    Returns:
        Flat list of log entries across all successful queries.
    """
    if not queries:
        return []

    config = get_config()
    results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=config.mcp_timeout) as client:
        for query in queries:
            try:
                response = await client.get(
                    f"{config.loki_url}/loki/api/v1/query_range",
                    params={
                        "query": query,
                        "start": start * 1_000_000_000,
                        "end": end * 1_000_000_000,
                        "limit": config.loki_query_limit,
                    },
                )
                response.raise_for_status()
                results.extend(parse_loki_response(response.json()))
            except httpx.TimeoutException:
                logger.warning("Loki direct query timed out: %s", query)
            except httpx.HTTPStatusError as exc:
                logger.warning("Loki direct query HTTP error: %s — %s", query, exc)
            except Exception:
                logger.warning("Loki direct query failed: %s", query, exc_info=True)

    return results


def build_loki_queries_from_dags(
    dags: list[dict[str, Any]], core_namespace: str
) -> list[str]:
    """Build LogQL queries from the union of NFs and phases across all matched DAGs."""
    all_nfs: list[str] = []
    seen_nfs: set[str] = set()
    all_phases: list[dict[str, Any]] = []
    for dag in dags:
        for nf in dag.get("all_nfs", []):
            if nf not in seen_nfs:
                seen_nfs.add(nf)
                all_nfs.append(nf)
        all_phases.extend(dag.get("phases", []))

    combined = {"all_nfs": all_nfs, "phases": all_phases}
    return build_loki_queries(combined, core_namespace)


def _is_qualifying_with_noise_filter(
    entry: dict[str, Any],
    noise_patterns: list[str],
) -> bool:
    """Return True if entry qualifies as evidence (not noise).

    An entry is disqualified if its message matches any noise pattern,
    regardless of level. Otherwise, it qualifies if it has a matched
    phase/pattern OR is ERROR/WARN/FATAL level.

    Args:
        entry: Log entry dict with 'message', 'level', 'matched_phase', 'matched_pattern'.
        noise_patterns: Wildcard patterns (case-insensitive) to suppress.

    Returns:
        True if the entry should be included as evidence.
    """
    message = entry.get("message", "")
    for pattern in noise_patterns:
        if wildcard_match(message, pattern):
            return False
    if entry.get("matched_phase") or entry.get("matched_pattern"):
        return True
    return str(entry.get("level", "")).upper() in ("ERROR", "WARN", "FATAL")


# --- Agent entry point ---


def compress_nf_logs(
    logs: dict[str, Any],
    nf_union: list[str],
    token_budget: int,
) -> dict[str, Any]:
    """Compress NF logs with DAG-NF protection and noise filtering.

    DAG-flow NFs (lowercase match against nf_union) — qualifying entries kept,
    noise-matching entries stripped, messages are NEVER truncated.

    Non-DAG NFs — keep only entries that qualify (ERROR/WARN/FATAL level
    or matched against a DAG phase/pattern) AND do not match any noise pattern.
    NFs with no qualifying entries are omitted entirely.

    Budget enforcement: non-DAG entries are evicted entry-by-entry (partial NF
    inclusion is allowed); DAG NF keys are never removed from the result even
    if the total is over budget (a WARNING is logged in that case).

    Messages are truncated to cfg.rca_log_max_message_chars if they exceed that length.
    """
    if not logs:
        return {}

    cfg = get_config()
    max_chars = cfg.rca_log_max_message_chars
    noise_patterns = cfg.log_noise_patterns

    def _truncate(entry: dict[str, Any]) -> dict[str, Any]:
        msg = entry.get("message", "")
        if len(msg) > max_chars:
            return {**entry, "message": msg[:max_chars] + "…"}
        return entry

    nf_union_lower: set[str] = {nf.lower() for nf in nf_union}

    dag_nf_logs: dict[str, list[dict[str, Any]]] = {}
    non_dag_nf_logs: dict[str, list[dict[str, Any]]] = {}

    for nf, entries in logs.items():
        if not isinstance(entries, list):
            continue
        if nf.lower() in nf_union_lower:
            # DAG NFs: keep all non-noise entries (truncate messages)
            dag_nf_logs[nf] = [
                _truncate(e) for e in entries
                if not any(wildcard_match(e.get("message", ""), p) for p in noise_patterns)
            ]
        else:
            qualifying = [
                _truncate(e) for e in entries
                if _is_qualifying_with_noise_filter(e, noise_patterns)
            ]
            if qualifying:
                non_dag_nf_logs[nf] = qualifying

    # DAG NFs are prioritised but must fit within budget.
    # If DAG NFs together exceed the budget, trim entries per-NF (keep most recent).
    result: dict[str, Any] = {}
    dag_tokens = count_tokens(str(dag_nf_logs))
    if dag_tokens > token_budget:
        logger.warning(
            "DAG NF logs exceed token budget (%d tokens), trimming to budget",
            dag_tokens,
        )
        # Distribute budget evenly across DAG NFs; keep tail (most recent) entries.
        per_nf_budget = max(token_budget // max(len(dag_nf_logs), 1), 50)
        for nf, entries in dag_nf_logs.items():
            kept: list[dict[str, Any]] = []
            for entry in reversed(entries):
                trial = {**result, nf: [entry] + kept}
                if count_tokens(str(trial)) <= per_nf_budget + count_tokens(str(result)):
                    kept.insert(0, entry)
                else:
                    break
            result[nf] = kept  # always include DAG NF key, even if empty after trimming
    else:
        result = dict(dag_nf_logs)

    # Add non-DAG NFs with entry-level eviction (partial inclusion allowed)
    for nf, entries in non_dag_nf_logs.items():
        # Try to add the full NF block
        candidate = {**result, nf: entries}
        if count_tokens(str(candidate)) <= token_budget:
            result = candidate
            continue
        # Partial: add entries one-by-one until budget is reached
        partial: list[dict[str, Any]] = []
        for entry in entries:
            trial = {**result, nf: partial + [entry]}
            if count_tokens(str(trial)) <= token_budget:
                partial.append(entry)
            else:
                break
        if partial:
            result[nf] = partial

    return result


@traceable(name="NfLogsAgent")
def logs_agent(state: TriageState) -> dict[str, Any]:
    """NfLogsAgent entry point. Pure MCP/HTTP query, no LLM.

    Two-path architecture:
      1. Probe MCP server availability (lightweight /ready check).
      2. If reachable → MCP path for all queries.
      3. If unreachable → direct Loki HTTP path for all queries.
    """
    dags = state.get("dags") or []
    if not dags:
        return {"logs": {}}
    cfg = get_config()
    nf_union = state.get("nf_union") or []
    incident_id = state["incident_id"]
    alert_time = parse_timestamp(state["alert"]["startsAt"])
    start = int(alert_time - cfg.alert_lookback_seconds)
    end = int(alert_time + cfg.alert_lookahead_seconds)

    queries = build_loki_queries_from_dags(dags, cfg.core_namespace)

    logs_raw: list[dict[str, Any]] = []
    if queries:
        # Step 1: Determine which path to use
        try:
            use_mcp = asyncio.run(_check_mcp_available())
        except Exception:
            logger.warning(
                "MCP health check failed, defaulting to direct Loki",
                exc_info=True,
            )
            use_mcp = False

        # Step 2: Execute queries on the chosen path
        if use_mcp:
            try:
                logs_raw = asyncio.run(
                    _fetch_loki_logs(queries, start=start, end=end)
                )
            except Exception:
                logger.warning(
                    "MCP queries failed despite passing health check,"
                    " proceeding with empty logs",
                    exc_info=True,
                )
        else:
            logger.info("MCP server unavailable, using direct Loki connection")
            try:
                logs_raw = asyncio.run(
                    _fetch_loki_logs_direct(queries, start=start, end=end)
                )
            except Exception:
                logger.warning(
                    "Direct Loki query failed, proceeding with empty logs",
                    exc_info=True,
                )

    # Build combined dag for annotation (union of all phases)
    combined_dag: dict[str, Any] = {
        "all_nfs": [],
        "phases": [p for dag in dags for p in dag.get("phases", [])],
    }
    raw_logs = organize_and_annotate_logs(logs_raw, combined_dag)

    # Save pre-filter snapshot (non-blocking, non-fatal)
    save_artifact(incident_id, "pre_filter_logs.json", raw_logs, cfg.artifacts_dir)

    compressed = compress_nf_logs(raw_logs, nf_union, cfg.rca_token_budget_logs)

    # Save post-filter snapshot
    save_artifact(incident_id, "post_filter_logs.json", compressed, cfg.artifacts_dir)

    return {"logs": compressed}
