"""NfMetricsAgent: Per-NF Prometheus metrics collection via MCP.

No LLM. Queries Prometheus for error rates, latency, CPU, memory per NF
from the candidate list provided by the DAG.
"""

import asyncio
import logging
from typing import Any

from langsmith import traceable

from triage_agent.config import get_config
from triage_agent.mcp.client import MCPClient, MCPQueryError, MCPTimeoutError
from triage_agent.state import TriageState
from triage_agent.utils import count_tokens, parse_timestamp, save_artifact

logger = logging.getLogger(__name__)


def _resolve_nf(
    metric_labels: dict[str, str], nfs_lower: dict[str, str]
) -> str | None:
    """Determine which NF a Prometheus result belongs to.

    Checks 'nf' label first, then extracts prefix from 'pod' label.
    Returns the original-case NF name or None if unresolvable.
    """
    # Direct nf label
    nf_label = metric_labels.get("nf", "").lower()
    if nf_label in nfs_lower:
        return nfs_lower[nf_label]

    # Pod name prefix (e.g. "amf-deployment-abc123" -> "amf")
    pod_label = metric_labels.get("pod", "")
    if pod_label:
        prefix = pod_label.split("-")[0].lower()
        if prefix in nfs_lower:
            return nfs_lower[prefix]

    return None


def organize_metrics_by_nf(
    metrics: list[dict[str, Any]], nfs: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """Group a flat list of Prometheus result entries by NF name.

    Args:
        metrics: List of Prometheus result entries, each with 'metric' and 'value'.
        nfs: List of NF names (e.g. ["AMF", "AUSF"]) from the DAG.

    Returns:
        {NF_NAME: [result_entries...]} grouped by resolved NF.
    """
    # Build lowercase -> original-case lookup
    nfs_lower: dict[str, str] = {nf.lower(): nf for nf in nfs}
    result: dict[str, list[dict[str, Any]]] = {}

    for entry in metrics:
        labels = entry.get("metric", {})
        nf_name = _resolve_nf(labels, nfs_lower)
        if nf_name is None:
            continue
        if nf_name not in result:
            result[nf_name] = []
        result[nf_name].append(entry)

    return result


def build_nf_queries(nfs: list[str]) -> list[str]:
    """Build PromQL queries for each NF: error_rate, p95_latency, cpu, memory.

    Args:
        nfs: List of NF names from the DAG (e.g. ["AMF", "AUSF"]).

    Returns:
        List of PromQL query strings (4 per NF).
    """
    cfg = get_config()
    ew = cfg.promql_error_rate_window
    cw = cfg.promql_cpu_rate_window_nf
    q = cfg.promql_latency_quantile
    queries: list[str] = []
    for nf in nfs:
        nf_lower = nf.lower()
        queries.extend([
            f'rate(http_requests_total{{nf="{nf_lower}",status=~"5.."}}[{ew}])',
            f'histogram_quantile({q}, sum by (le) (rate(http_request_duration_seconds_bucket{{nf="{nf_lower}"}}[{ew}])))',
            f'rate(container_cpu_usage_seconds_total{{pod=~".*{nf_lower}.*"}}[{cw}])',
            f'container_memory_working_set_bytes{{pod=~".*{nf_lower}.*"}}',
        ])
    return queries


async def _fetch_prometheus_metrics(
    queries: list[str],
    alert_time: int,
) -> list[dict[str, Any]]:
    """Execute PromQL queries via MCP client, collecting results per-query.

    Each query is executed individually so that a single failure does not
    discard results from other queries (graceful partial failure).

    Args:
        queries: PromQL query strings to execute.
        alert_time: Unix epoch seconds for the query timestamp.

    Returns:
        Flat list of Prometheus result entries across all successful queries.
    """
    if not queries:
        return []

    results: list[dict[str, Any]] = []
    async with MCPClient() as client:
        for query in queries:
            try:
                data = await client.query_prometheus(query, time=alert_time)
                results.extend(data.get("result", []))
            except MCPTimeoutError:
                logger.warning("Prometheus query timed out: %s", query)
            except MCPQueryError as exc:
                logger.warning("Prometheus query failed: %s — %s", query, exc)

    return results


def compress_nf_metrics(
    metrics: dict[str, Any],
    nf_union: list[str],
    token_budget: int,
) -> dict[str, Any]:
    """Compress NF metrics with DAG-NF protection.

    Compacts Prometheus vector entries (list of {metric, value} dicts) to a
    compact summary dict ({NF: {metric_name: float}}).

    DAG-flow NFs (in nf_union) are ALWAYS included, even when all metrics are
    nominal. Non-DAG NFs are added while the serialised result stays within
    token_budget; they are silently dropped when the budget is exceeded.

    If DAG NFs alone exceed token_budget, a WARNING is logged and they are
    forwarded anyway — correctness takes priority over budget.
    """
    if not metrics:
        return {}

    nf_union_lower: set[str] = {nf.lower() for nf in nf_union}

    def _compact(entries: Any) -> Any:
        """Collapse Prometheus vector entries → {metric_key: float} summary."""
        if not isinstance(entries, list):
            return entries
        summary: dict[str, float] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            labels = entry.get("metric", {})
            value_pair = entry.get("value", [None, None])
            # Prefer explicit 'report' label (set by label_replace in PromQL),
            # then fall back to the metric __name__.
            key = labels.get("report") or labels.get("__name__") or ""
            try:
                val = float(value_pair[1]) if len(value_pair) > 1 else 0.0
            except (TypeError, ValueError):
                val = 0.0
            if key:
                summary[key] = max(summary.get(key, val), val)
        return summary if summary else entries

    compacted = {nf: _compact(data) for nf, data in metrics.items()}

    dag_nfs = {nf: data for nf, data in compacted.items() if nf.lower() in nf_union_lower}
    non_dag_nfs = {
        nf: data for nf, data in compacted.items() if nf.lower() not in nf_union_lower
    }

    # DAG NFs always included
    result: dict[str, Any] = dict(dag_nfs)

    dag_tokens = count_tokens(str(result))
    if dag_tokens > token_budget:
        logger.warning(
            "DAG NF metrics exceed token budget (%d tokens), forwarding anyway for correctness",
            dag_tokens,
        )
        return result

    # Add non-DAG NFs while within budget
    for nf, data in non_dag_nfs.items():
        candidate = {**result, nf: data}
        if count_tokens(str(candidate)) <= token_budget:
            result = candidate

    return result


@traceable(name="NfMetricsAgent")
def metrics_agent(state: TriageState) -> dict[str, Any]:
    """NfMetricsAgent entry point. Pure MCP query, no LLM."""
    nf_union = state.get("nf_union") or []
    if not nf_union:
        return {"metrics": {}}

    cfg = get_config()
    incident_id = state["incident_id"]
    alert_time = parse_timestamp(state["alert"]["startsAt"])

    queries = build_nf_queries(nf_union)

    # Fetch metrics from Prometheus via MCP (graceful degradation on failure)
    raw_results: list[dict[str, Any]] = []
    if queries:
        try:
            raw_results = asyncio.run(
                _fetch_prometheus_metrics(queries, alert_time=int(alert_time))
            )
        except Exception:
            logger.warning(
                "MCP client unavailable, proceeding with empty metrics",
                exc_info=True,
            )

    raw_metrics = organize_metrics_by_nf(raw_results, nf_union)

    # Save pre-filter snapshot (non-blocking, non-fatal)
    save_artifact(incident_id, "pre_filter_metrics.json", raw_metrics, cfg.artifacts_dir)

    compressed = compress_nf_metrics(raw_metrics, nf_union, cfg.rca_token_budget_metrics)

    # Save post-filter snapshot
    save_artifact(incident_id, "post_filter_metrics.json", compressed, cfg.artifacts_dir)

    # Return only the key this agent writes — avoids LangGraph parallel-merge conflict
    # with logs_agent and traces_agent (all three fan out from dag_mapper in parallel).
    return {"metrics": compressed}
