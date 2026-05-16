"""Shared state object for all agents in the triage pipeline."""

from typing import Annotated, Any, TypedDict


def _last_write(a: dict[str, str] | None, b: dict[str, str] | None) -> dict[str, str] | None:
    """Reducer: keep the most recent non-None write (last-write-wins)."""
    return b if b is not None else a


class TriageState(TypedDict):
    # Input
    alert: dict[str, Any]  # Alertmanager webhook payload

    # InfraAgent outputs (forwarded to RCAAgent)
    infra_checked: bool
    infra_score: float  # 0.0-1.0
    infra_findings: dict[str, Any] | None  # Pod metrics, events, resource usage

    # DAG mapping outputs (alert → one or more 3GPP procedures)
    procedure_names: list[str] | None  # e.g. ["registration_general", "authentication_5g_aka"]
    dag_ids: list[str] | None
    dags: list[dict[str, Any]] | None            # Full DAG structures from Memgraph, one per procedure
    nf_union: list[str] | None         # Deduplicated union of all_nfs across matched DAGs
    mapping_confidence: float          # Overall mapping quality (0.0-1.0)
    mapping_method: str                # "exact_match"|"keyword_match"|"nf_default"|"generic_fallback"

    # NfMetricsAgent/NfLogsAgent/UeTracesAgent outputs
    metrics: dict[str, Any] | None  # {nf_name: [metric_data]}
    logs: dict[str, Any] | None  # {nf_name: [log_entries]}
    discovered_imsis: list[str] | None  # IMSIs active in alarm window
    traces_ready: bool  # True when IMSI traces ingested into Memgraph
    trace_deviations: dict[str, list[dict[str, Any]]] | None  # {dag_name: [deviation_dicts]} from Memgraph comparison
    incident_id: str  # Unique investigation identifier
    evidence_quality_score: float

    # RCAAgent outputs (considers infra + app evidence)
    root_nf: str | None
    failure_mode: str | None
    layer: str  # "infrastructure" or "application"
    confidence: float
    evidence_chain: list[dict[str, Any]]

    # Control flow
    attempt_count: int  # Current attempt (1-based)
    max_attempts: int  # Hard limit (default: 2)
    needs_more_evidence: bool
    evidence_gaps: list[str] | None  # Identified evidence gaps for second attempt
    compressed_evidence: Annotated[dict[str, str] | None, _last_write]  # pre-compressed evidence sections for the LLM prompt
    final_report: dict[str, Any] | None
