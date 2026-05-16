"""Shared utility functions for TriageAgent pipeline."""

import concurrent.futures
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def parse_timestamp(ts: str) -> float:
    """Parse ISO timestamp from alert payload. Returns Unix epoch seconds."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.timestamp()


def extract_log_level(message: str) -> str:
    """Extract log level from message text."""
    message_upper = message.upper()
    for level in ("FATAL", "ERROR", "WARN", "INFO", "DEBUG"):
        if level in message_upper:
            return level
    return "INFO"


def parse_loki_response(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse Loki query_range JSON response into flat log entry list."""
    logs: list[dict[str, Any]] = []
    for stream in data.get("data", {}).get("result", []):
        labels = stream.get("stream", {})
        for value in stream.get("values", []):
            logs.append({
                "timestamp": int(value[0]) // 1_000_000_000,
                "message": value[1],
                "labels": labels,
                "pod": labels.get("k8s_pod_name", labels.get("pod", "")),
                "level": extract_log_level(value[1]),
            })
    return logs


def extract_nf_from_pod_name(pod: str) -> str:
    """Extract NF name prefix from a k8s pod name. Returns lowercase."""
    return pod.split("-")[0].lower()


def count_tokens(text: str) -> int:
    """Approximate token count using 4-chars-per-token heuristic."""
    return max(1, len(text) // 4)


def _write_artifact_sync(
    incident_id: str, name: str, data: Any, artifacts_dir: str
) -> None:
    """Write artifact to disk synchronously. Called from a background thread."""
    try:
        target_dir = Path(artifacts_dir) / incident_id
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / name).write_text(
            json.dumps(data, indent=2, default=str), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("Failed to save artifact %s/%s: %s", incident_id, name, exc)


# Module-level executor: avoids spawning a new thread per artifact write
_artifact_executor: concurrent.futures.ThreadPoolExecutor = (
    concurrent.futures.ThreadPoolExecutor(max_workers=2)
)


def save_artifact(
    incident_id: str, name: str, data: Any, artifacts_dir: str
) -> None:
    """Fire-and-forget artifact write. Non-blocking, non-fatal.

    Submits to a module-level ThreadPoolExecutor so no new thread is created per call.
    Failures are logged as warnings and silently swallowed.
    """
    _artifact_executor.submit(_write_artifact_sync, incident_id, name, data, artifacts_dir)


def compress_dag(
    dags: list[dict[str, Any]] | None,
    token_budget: int,
) -> list[dict[str, Any]]:
    """Compress DAG structures to fit within token_budget.

    Stripping cascade (each step checked against budget):
        1. Return as-is if within budget.
        2. Strip 'keywords' and 'success_log' from all phases.
        3. Keep only phases that have non-empty 'failure_patterns'.
        4. Truncate phases per DAG (always keep first + last phases).
    """
    if not dags:
        return []

    def _fits(d: list[dict[str, Any]]) -> bool:
        return count_tokens(json.dumps(d)) <= token_budget

    if _fits(dags):
        return dags

    # Step 2: strip keywords and success_log
    stripped: list[dict[str, Any]] = []
    for dag in dags:
        phases = [
            {k: v for k, v in phase.items() if k not in ("keywords", "success_log")}
            for phase in dag.get("phases", [])
        ]
        stripped.append({**dag, "phases": phases})

    if _fits(stripped):
        return stripped

    # Step 3: keep only phases with failure_patterns
    fp_only: list[dict[str, Any]] = []
    for dag in stripped:
        phases = [p for p in dag.get("phases", []) if p.get("failure_patterns")]
        fp_only.append({**dag, "phases": phases})

    if _fits(fp_only):
        return fp_only

    # Guard: if all DAGs have zero phases after step-3 filtering, further truncation is impossible
    if not any(d.get("phases") for d in fp_only):
        return fp_only

    # Step 4: truncate phases, always keeping first and last
    result = fp_only
    for max_phases in range(len(max((d.get("phases", []) for d in result), key=len, default=[])), 0, -1):
        truncated = []
        for dag in result:
            phases = dag.get("phases", [])
            if len(phases) > max_phases:
                keep = [phases[0]] if phases else []
                middle = phases[1:-1][:max(0, max_phases - 2)]
                last = [phases[-1]] if len(phases) > 1 else []
                phases = keep + middle + last
            truncated.append({**dag, "phases": phases})
        if _fits(truncated):
            logger.warning("DAG compressed: truncated to %d phases per DAG", max_phases)
            return truncated

    return result


def compress_trace_deviations(
    deviations: dict[str, list[dict[str, Any]]] | None,
    token_budget: int,
) -> dict[str, list[dict[str, Any]]]:
    """Compress trace deviations to fit within token_budget.

    Slices each DAG's deviation list to cfg.rca_max_deviations_per_dag,
    then drops DAGs with empty lists if still over budget.
    """
    if not deviations:
        return {}

    from triage_agent.config import get_config  # deferred to avoid circular import
    cfg = get_config()
    max_per_dag = cfg.rca_max_deviations_per_dag

    sliced = {dag: devs[:max_per_dag] for dag, devs in deviations.items()}

    if count_tokens(json.dumps(sliced)) <= token_budget:
        return sliced

    # Drop empty DAGs first
    non_empty = {dag: devs for dag, devs in sliced.items() if devs}
    if count_tokens(json.dumps(non_empty)) <= token_budget:
        return non_empty

    # Drop DAGs with fewest deviations until within budget
    dag_names = sorted(non_empty.keys(), key=lambda d: len(non_empty[d]))
    while dag_names and count_tokens(json.dumps({d: non_empty[d] for d in dag_names})) > token_budget:
        dag_names.pop(0)

    return {d: non_empty[d] for d in dag_names}
