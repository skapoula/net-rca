"""Local file-based trace callback handler for the TriageAgent LangGraph workflow.

Captures all LangGraph node events and LLM calls during a triage workflow
invocation, then writes two files per incident to the artifacts directory:

- trace.json       — structured event log (timestamps, latencies, inputs/outputs)
- trace-summary.txt — human-readable timing table and LLM call excerpts
"""

import json
import logging
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.outputs import LLMResult

from triage_agent.utils import _artifact_executor, count_tokens

logger = logging.getLogger(__name__)

# Maximum characters for truncated fields in the event log.
_MAX_INPUT_CHARS = 2000
_MAX_PROMPT_CHARS = 500
_MAX_RESPONSE_CHARS = 1000


def _safe_truncate(obj: object, max_chars: int) -> str:
    """Serialize obj to JSON and truncate to max_chars characters."""
    try:
        s = json.dumps(obj, default=str)
    except Exception:
        s = str(obj)
    return s[:max_chars]


def _write_json_sync(path: Path, data: dict[str, Any]) -> None:
    """Write a JSON file synchronously. Called from a background thread."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        logger.warning("LocalTraceCallbackHandler: failed to write %s: %s", path, exc)


def _write_text_sync(path: Path, text: str) -> None:
    """Write a plain-text file synchronously. Called from a background thread."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    except Exception as exc:
        logger.warning("LocalTraceCallbackHandler: failed to write %s: %s", path, exc)


class LocalTraceCallbackHandler(BaseCallbackHandler):
    """Captures LangGraph workflow events and writes them to per-incident trace files.

    Writes two files to {artifacts_dir}/{incident_id}/:
      - trace.json        — structured event list with timestamps and latencies
      - trace-summary.txt — human-readable agent timing table + LLM call excerpts

    Thread-safe: a threading.Lock guards shared state across LangGraph's parallel
    node branches, which execute in separate threads during sync graph invocation.

    Usage::

        handler = LocalTraceCallbackHandler(incident_id=incident_id, artifacts_dir=cfg.artifacts_dir)
        workflow.invoke(state, {"callbacks": [handler]})
        # trace files are written automatically when the root chain ends
    """

    raise_error: bool = False  # never let tracing errors crash the workflow

    def __init__(self, incident_id: str, artifacts_dir: str) -> None:
        super().__init__()
        self.incident_id = incident_id
        self.artifacts_dir = artifacts_dir
        self._events: list[dict[str, Any]] = []
        self._root_run_id: UUID | None = None
        self._workflow_start: float | None = None
        self._workflow_start_iso: str | None = None
        self._run_start_times: dict[str, float] = {}
        self._lock = threading.Lock()
        self._flushed = False

    # ── Chain events ────────────────────────────────────────────────────────────

    def on_chain_start(
        self,
        serialized: dict[str, Any],
        inputs: dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401 — BaseCallbackHandler interface requires Any
    ) -> None:
        """Record chain/node start; identify the root run on first call."""
        try:
            now = time.monotonic()
            now_iso = datetime.now(UTC).isoformat()
            ser = serialized or {}
            name = ser.get("name") or (ser.get("id") or ["unknown"])[-1]
            with self._lock:
                self._run_start_times[str(run_id)] = now
                if parent_run_id is None and self._root_run_id is None:
                    self._root_run_id = run_id
                    self._workflow_start = now
                    self._workflow_start_iso = now_iso
                self._events.append({
                    "event_type": "chain_start",
                    "name": name,
                    "run_id": str(run_id),
                    "parent_run_id": str(parent_run_id) if parent_run_id else None,
                    "timestamp_iso": now_iso,
                    "latency_ms": None,
                    "inputs": _safe_truncate(inputs, _MAX_INPUT_CHARS) if inputs else None,
                    "outputs": None,
                    "error": None,
                    "tags": tags,
                    "metadata": metadata,
                })
        except Exception as exc:
            logger.warning("LocalTraceCallbackHandler.on_chain_start error: %s", exc)

    def on_chain_end(
        self,
        outputs: dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        **kwargs: Any,  # noqa: ANN401 — BaseCallbackHandler interface requires Any
    ) -> None:
        """Record chain/node end; flush trace files when the root chain completes."""
        try:
            now = time.monotonic()
            now_iso = datetime.now(UTC).isoformat()
            with self._lock:
                start = self._run_start_times.pop(str(run_id), None)
                latency_ms = round((now - start) * 1000, 1) if start is not None else None
                self._events.append({
                    "event_type": "chain_end",
                    "name": None,
                    "run_id": str(run_id),
                    "parent_run_id": str(parent_run_id) if parent_run_id else None,
                    "timestamp_iso": now_iso,
                    "latency_ms": latency_ms,
                    "inputs": None,
                    "outputs": _safe_truncate(outputs, _MAX_INPUT_CHARS) if outputs else None,
                    "error": None,
                    "tags": None,
                    "metadata": None,
                })
                is_root = self._root_run_id is not None and run_id == self._root_run_id
            if is_root:
                self._submit_flush(end_iso=now_iso, total_ms=latency_ms)
        except Exception as exc:
            logger.warning("LocalTraceCallbackHandler.on_chain_end error: %s", exc)

    def on_chain_error(
        self,
        error: BaseException,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        **kwargs: Any,  # noqa: ANN401 — BaseCallbackHandler interface requires Any
    ) -> None:
        """Record chain/node error."""
        try:
            now = time.monotonic()
            with self._lock:
                start = self._run_start_times.pop(str(run_id), None)
                latency_ms = round((now - start) * 1000, 1) if start is not None else None
                self._events.append({
                    "event_type": "chain_error",
                    "name": None,
                    "run_id": str(run_id),
                    "parent_run_id": str(parent_run_id) if parent_run_id else None,
                    "timestamp_iso": datetime.now(UTC).isoformat(),
                    "latency_ms": latency_ms,
                    "inputs": None,
                    "outputs": None,
                    "error": str(error),
                    "tags": None,
                    "metadata": None,
                })
        except Exception as exc:
            logger.warning("LocalTraceCallbackHandler.on_chain_error error: %s", exc)

    # ── LLM events ──────────────────────────────────────────────────────────────

    def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: list[str],
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401 — BaseCallbackHandler interface requires Any
    ) -> None:
        """Record LLM call start with prompt excerpt."""
        try:
            now = time.monotonic()
            now_iso = datetime.now(UTC).isoformat()
            ser = serialized or {}
            name = ser.get("name") or (ser.get("id") or ["ChatOpenAI"])[-1]
            prompt_excerpt = prompts[0][:_MAX_PROMPT_CHARS] if prompts else ""
            with self._lock:
                self._run_start_times[str(run_id)] = now
                self._events.append({
                    "event_type": "llm_start",
                    "name": name,
                    "run_id": str(run_id),
                    "parent_run_id": str(parent_run_id) if parent_run_id else None,
                    "timestamp_iso": now_iso,
                    "latency_ms": None,
                    "prompt_excerpt": prompt_excerpt,
                    "response_excerpt": None,
                    "token_count_estimate": None,
                    "tags": tags,
                    "metadata": metadata,
                })
        except Exception as exc:
            logger.warning("LocalTraceCallbackHandler.on_llm_start error: %s", exc)

    def on_llm_end(
        self,
        response: LLMResult,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        **kwargs: Any,  # noqa: ANN401 — BaseCallbackHandler interface requires Any
    ) -> None:
        """Record LLM call end with response excerpt and token estimate."""
        try:
            now = time.monotonic()
            now_iso = datetime.now(UTC).isoformat()
            response_text = ""
            try:
                gen = response.generations[0][0]
                response_text = (
                    getattr(gen, "text", None)
                    or getattr(getattr(gen, "message", None), "content", "")
                    or ""
                )
                if not isinstance(response_text, str):
                    response_text = json.dumps(response_text, default=str)
            except (IndexError, AttributeError):
                pass
            response_excerpt = response_text[:_MAX_RESPONSE_CHARS]
            token_est = count_tokens(response_text) if response_text else None
            with self._lock:
                start = self._run_start_times.pop(str(run_id), None)
                latency_ms = round((now - start) * 1000, 1) if start is not None else None
                self._events.append({
                    "event_type": "llm_end",
                    "name": None,
                    "run_id": str(run_id),
                    "parent_run_id": str(parent_run_id) if parent_run_id else None,
                    "timestamp_iso": now_iso,
                    "latency_ms": latency_ms,
                    "prompt_excerpt": None,
                    "response_excerpt": response_excerpt,
                    "token_count_estimate": token_est,
                    "tags": None,
                    "metadata": None,
                })
        except Exception as exc:
            logger.warning("LocalTraceCallbackHandler.on_llm_end error: %s", exc)

    # ── Tool events (stubs — no tools used currently) ───────────────────────────

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        inputs: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401 — BaseCallbackHandler interface requires Any
    ) -> None:
        """Record tool call start."""
        try:
            now = time.monotonic()
            ser = serialized or {}
            name = ser.get("name") or (ser.get("id") or ["tool"])[-1]
            with self._lock:
                self._run_start_times[str(run_id)] = now
                self._events.append({
                    "event_type": "tool_start",
                    "name": name,
                    "run_id": str(run_id),
                    "parent_run_id": str(parent_run_id) if parent_run_id else None,
                    "timestamp_iso": datetime.now(UTC).isoformat(),
                    "latency_ms": None,
                    "inputs": input_str[:_MAX_INPUT_CHARS] if input_str else None,
                    "outputs": None,
                    "error": None,
                    "tags": tags,
                    "metadata": metadata,
                })
        except Exception as exc:
            logger.warning("LocalTraceCallbackHandler.on_tool_start error: %s", exc)

    def on_tool_end(
        self,
        output: object,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        **kwargs: Any,  # noqa: ANN401 — BaseCallbackHandler interface requires Any
    ) -> None:
        """Record tool call end."""
        try:
            now = time.monotonic()
            with self._lock:
                start = self._run_start_times.pop(str(run_id), None)
                latency_ms = round((now - start) * 1000, 1) if start is not None else None
                self._events.append({
                    "event_type": "tool_end",
                    "name": None,
                    "run_id": str(run_id),
                    "parent_run_id": str(parent_run_id) if parent_run_id else None,
                    "timestamp_iso": datetime.now(UTC).isoformat(),
                    "latency_ms": latency_ms,
                    "inputs": None,
                    "outputs": _safe_truncate(output, _MAX_INPUT_CHARS) if output is not None else None,
                    "error": None,
                    "tags": None,
                    "metadata": None,
                })
        except Exception as exc:
            logger.warning("LocalTraceCallbackHandler.on_tool_end error: %s", exc)

    # ── Flush ───────────────────────────────────────────────────────────────────

    def flush(self) -> None:
        """Write trace files to disk. Safe to call multiple times; only writes once.

        Called automatically when the root chain ends. Also called explicitly from
        the webhook error handler to capture partial traces on workflow failure.
        """
        self._submit_flush(
            end_iso=datetime.now(UTC).isoformat(),
            total_ms=None,
        )

    def _submit_flush(self, end_iso: str, total_ms: float | None) -> None:
        """Snapshot events and submit async file writes. Idempotent."""
        with self._lock:
            if self._flushed:
                return
            self._flushed = True
            events_snapshot = list(self._events)
            start_iso = self._workflow_start_iso
            workflow_start = self._workflow_start

        if total_ms is None and workflow_start is not None:
            total_ms = round((time.monotonic() - workflow_start) * 1000, 1)

        trace_data = self._build_trace_json(events_snapshot, start_iso, end_iso, total_ms)
        summary_text = self._build_summary_txt(events_snapshot, start_iso, end_iso, total_ms)

        out_dir = Path(self.artifacts_dir) / self.incident_id
        _artifact_executor.submit(_write_json_sync, out_dir / "trace.json", trace_data)
        _artifact_executor.submit(_write_text_sync, out_dir / "trace-summary.txt", summary_text)
        logger.debug(
            "LocalTraceCallbackHandler: trace flush submitted for incident %s (%d events)",
            self.incident_id,
            len(events_snapshot),
        )

    # ── Output builders ─────────────────────────────────────────────────────────

    def _build_trace_json(
        self,
        events: list[dict[str, Any]],
        start_iso: str | None,
        end_iso: str,
        total_ms: float | None,
    ) -> dict[str, Any]:
        """Build the trace.json envelope."""
        return {
            "incident_id": self.incident_id,
            "workflow_start_iso": start_iso,
            "workflow_end_iso": end_iso,
            "total_latency_ms": total_ms,
            "event_count": len(events),
            "events": events,
        }

    def _build_summary_txt(
        self,
        events: list[dict[str, Any]],
        start_iso: str | None,
        end_iso: str,
        total_ms: float | None,
    ) -> str:
        """Build the human-readable trace-summary.txt."""
        total_str = f"{round(total_ms)} ms" if total_ms is not None else "unknown"
        lines: list[str] = [
            f"TRACE SUMMARY — incident_id: {self.incident_id}",
            f"Generated:        {end_iso}",
            f"Workflow start:   {start_iso or 'unknown'}",
            f"Total latency:    {total_str}",
            "",
            "=== AGENT TIMINGS ===",
            f"{'Node':<32} {'Latency (ms)':>14}  Status",
            f"{'----':<32} {'------------':>14}  ------",
        ]

        # Map run_id → name from chain_start events
        run_names: dict[str, str] = {
            e["run_id"]: e["name"]
            for e in events
            if e["event_type"] == "chain_start" and e.get("name")
        }
        # Map run_id → latency from chain_end events
        run_latencies: dict[str, float | None] = {
            e["run_id"]: e["latency_ms"]
            for e in events
            if e["event_type"] == "chain_end"
        }
        run_errors: set[str] = {
            e["run_id"] for e in events if e["event_type"] == "chain_error"
        }

        for run_id, name in run_names.items():
            latency = run_latencies.get(run_id)
            status = "ERROR" if run_id in run_errors else "ok"
            lat_str = str(round(latency)) if latency is not None else "—"
            lines.append(f"  {name:<30} {lat_str:>14}  {status}")

        # LLM calls section
        llm_starts = {e["run_id"]: e for e in events if e["event_type"] == "llm_start"}
        llm_ends = {e["run_id"]: e for e in events if e["event_type"] == "llm_end"}
        if llm_starts:
            lines += ["", "=== LLM CALLS ==="]
            for i, (run_id, start_ev) in enumerate(llm_starts.items(), 1):
                end_ev = llm_ends.get(run_id, {})
                latency = end_ev.get("latency_ms")
                tokens = end_ev.get("token_count_estimate")
                lat_str = f"{round(latency)} ms" if latency is not None else "unknown"
                tok_str = f"  tokens_est={tokens}" if tokens is not None else ""
                lines.append(f"[{i}] {start_ev.get('name', 'LLM')}  latency={lat_str}{tok_str}")
                if start_ev.get("prompt_excerpt"):
                    lines.append("    PROMPT (first 500 chars):")
                    for pl in start_ev["prompt_excerpt"].splitlines()[:8]:
                        lines.append(f"      {pl}")
                if end_ev.get("response_excerpt"):
                    lines.append("    RESPONSE (first 1000 chars):")
                    for rl in end_ev["response_excerpt"].splitlines()[:12]:
                        lines.append(f"      {rl}")

        # Errors section
        error_events = [e for e in events if e["event_type"] == "chain_error"]
        lines += ["", "=== ERRORS ==="]
        if error_events:
            for e in error_events:
                name = run_names.get(e["run_id"], e["run_id"])
                lines.append(f"  {name}: {e.get('error', 'unknown error')}")
        else:
            lines.append("  (none)")

        return "\n".join(lines) + "\n"
