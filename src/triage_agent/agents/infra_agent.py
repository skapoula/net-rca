"""InfraAgent: Infrastructure triage via Prometheus pod metrics.

Rule-based (no LLM). Queries Prometheus via MCP for pod-level health,
computes an infrastructure score, and forwards findings to RCAAgent.
"""

import asyncio
import logging
from typing import Any

import httpx
from langsmith import traceable

from triage_agent.config import get_config
from triage_agent.state import TriageState
from triage_agent.utils import count_tokens, parse_timestamp, save_artifact

logger = logging.getLogger(__name__)


def build_infra_queries(core_namespace: str) -> list[str]:
    """Build PromQL queries scoped to the given K8s namespace.

    The container/pod regex filter is derived from known_nfs plus mongodb
    (an Open5GS dependency that is monitored but is not a 5G NF itself).
    PromQL time windows are read from configuration.
    """
    cfg = get_config()
    ns = core_namespace
    # Build regex from known_nfs plus mongodb (infra dependency).
    nf_names = sorted(set(cfg.known_nfs) | {"mongodb"})
    pattern = "^(" + "|".join(nf_names) + ").*"
    cr = pattern
    pr = pattern
    rw = cfg.promql_restart_window
    ow = cfg.promql_oom_window
    cw = cfg.promql_cpu_rate_window_infra
    return [
        # Pod restarts (configurable window, default 1h)
        f'label_replace(sum by (namespace, pod, container)'
        f'(increase(kube_pod_container_status_restarts_total'
        f'{{namespace="{ns}", container=~"{cr}", '
        f'pod=~"{pr}"}}[{rw}])), '
        f'"report", "pod_restarts", "", "")',
        # OOM kills (configurable window, default 5m)
        f'label_replace(sum by (pod, container) '
        f'(increase(kube_pod_container_status_restarts_total'
        f'{{namespace="{ns}", container=~"{cr}", '
        f'pod=~"{pr}"}}[{ow}]) '
        f'* on(namespace, pod, container) group_left(reason) '
        f'kube_pod_container_status_last_terminated_reason{{reason="OOMKilled"}}), '
        f'"report", "oom_kills_5m", "", "")',
        # CPU usage rate (configurable window, default 2m)
        f'label_replace(sum by (pod, container) '
        f'(rate(container_cpu_usage_seconds_total'
        f'{{namespace="{ns}", container=~"{cr}", '
        f'pod=~"{pr}"}}[{cw}])), '
        f'"report", "cpu_usage_rate_2m", "", "")',
        # Memory usage percent
        f'label_replace((sum by (pod, container) '
        f'(container_memory_working_set_bytes'
        f'{{namespace="{ns}", container=~"{cr}", '
        f'pod=~"{pr}"}}) '
        f'/ sum by (pod, container) '
        f'(kube_pod_container_resource_limits{{resource="memory", namespace="{ns}", '
        f'container=~"{cr}", '
        f'pod=~"{pr}"}})) * 100, '
        f'"report", "memory_usage_percent", "", "")',
        # Pod status
        f'label_replace(sum by (namespace, pod, phase) '
        f'(kube_pod_status_phase{{namespace="{ns}", phase=~"Running|Pending|Unknown|Failed", '
        f'pod=~"{pr}"}}) > 0, '
        f'"report", "pod_status", "", "")',
    ]

async def _fetch_prometheus_metrics(
    affected_nfs: list[str],
    start: int,
    end: int,
) -> dict[str, Any]:
    """Fetch infra metrics from Prometheus via direct HTTP (no MCP).

    Runs every PromQL query from build_infra_queries plus a replica-absence
    query for the affected NFs. Results are collected into a dict keyed by
    the 'report' label injected by label_replace().

    Args:
        affected_nfs: NF names from the triage state.
        start: Unix epoch seconds for window start.
        end: Unix epoch seconds for window end.

    Returns:
        Dict mapping report-label → list of {metric, value} dicts.
        Returns {} on total failure (logged as WARNING).
    """
    cfg = get_config()
    queries = list(build_infra_queries(cfg.core_namespace))

    # Replica-absence query is run separately as a range query (see below).
    # Build the raw PromQL but do NOT add it to the instant-query list.
    replica_query: str | None = None
    if affected_nfs:
        nf_regex = "|".join(nf.lower() for nf in affected_nfs)
        replica_query = (
            f'kube_deployment_status_replicas_available{{deployment=~"{nf_regex}"}}'
        )

    collected: dict[str, list[dict[str, Any]]] = {}

    async with httpx.AsyncClient(timeout=cfg.mcp_timeout) as client:
        # ── Instant queries for all standard infra metrics ───────────────────
        for query in queries:
            for attempt in range(cfg.prometheus_max_retries):
                try:
                    resp = await client.get(
                        f"{cfg.prometheus_url}/api/v1/query",
                        params={"query": query, "time": end},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    for series in data.get("data", {}).get("result", []):
                        report_key = series["metric"].get("report", "unknown")
                        collected.setdefault(report_key, []).append(series)
                    break
                except httpx.TimeoutException:
                    logger.warning(
                        "Prometheus query timed out (attempt %d): %s", attempt + 1, query
                    )
                except httpx.HTTPStatusError as exc:
                    logger.warning("Prometheus HTTP error: %s — %s", query, exc)
                    break
                except Exception:
                    logger.warning(
                        "Prometheus query failed (attempt %d): %s",
                        attempt + 1,
                        query,
                        exc_info=True,
                    )
                    break

        # ── Range query for replica-absence detection ────────────────────────
        # Use a range query over [start, end] so we detect 0-replica states that
        # occurred during the incident window even if Prometheus hasn't yet scraped
        # the current state (instant queries return stale data when queried before
        # kube-state-metrics has scraped the scale-to-0 event).
        if replica_query:
            for attempt in range(cfg.prometheus_max_retries):
                try:
                    resp = await client.get(
                        f"{cfg.prometheus_url}/api/v1/query_range",
                        params={
                            "query": replica_query,
                            "start": start,
                            "end": end,
                            "step": "15s",
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    for series in data.get("data", {}).get("result", []):
                        deployment = series["metric"].get("deployment", "")
                        values = series.get("values", [])
                        # If any sample in the window had 0 replicas, record as absent.
                        if any(float(v[1]) == 0 for v in values):
                            collected.setdefault("replicas_available", []).append(
                                {
                                    "metric": {**series["metric"], "report": "replicas_available"},
                                    "value": [end, "0"],  # sentinel: deployment was absent
                                }
                            )
                            logger.info(
                                "Replica-absence detected via range query: deployment=%s had 0 replicas in window",
                                deployment,
                            )
                        else:
                            # Deployment is healthy — record current (last) value
                            if values:
                                collected.setdefault("replicas_available", []).append(
                                    {
                                        "metric": {**series["metric"], "report": "replicas_available"},
                                        "value": values[-1],
                                    }
                                )
                    break
                except httpx.TimeoutException:
                    logger.warning(
                        "Prometheus replica range query timed out (attempt %d)", attempt + 1
                    )
                except httpx.HTTPStatusError as exc:
                    logger.warning("Prometheus replica range query HTTP error: %s", exc)
                    break
                except Exception:
                    logger.warning(
                        "Prometheus replica range query failed (attempt %d)",
                        attempt + 1,
                        exc_info=True,
                    )
                    break

    return collected


def extract_replica_status(metrics: dict[str, Any]) -> set[str]:
    """Return set of deployment names with 0 available replicas.

    Args:
        metrics: Dict returned by _fetch_prometheus_metrics(), keyed by report label.

    Returns:
        Set of lowercase deployment names that have 0 available replicas.
    """
    absent: set[str] = set()
    for series in metrics.get("replicas_available", []):
        deployment = series["metric"].get("deployment", "")
        try:
            if float(series["value"][1]) == 0:
                absent.add(deployment.lower())
        except (IndexError, ValueError, TypeError):
            pass
    return absent


def _safe_float(value_field: Any) -> float:
    """Extract float from a Prometheus value field, which may be [timestamp, value_str] or a plain number."""
    try:
        if isinstance(value_field, list):
            return float(value_field[1])
        return float(value_field)
    except (IndexError, ValueError, TypeError):
        return 0.0


def _normalize_prometheus_metrics(raw: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Convert raw Prometheus series format to the internal format expected by extract_* functions.

    Prometheus returns each series as {"metric": {label: value, ...}, "value": [timestamp, value_str]}.
    The internal format used by compute_infrastructure_score and extract_* helpers is simpler:
    {"pod": str, "value": float, ...}.

    Args:
        raw: Dict keyed by report label, values are lists of raw Prometheus series dicts.

    Returns:
        Normalized dict with keys: pod_restarts, oom_kills, cpu_usage, memory_percent,
        pod_status, replicas_available.
    """
    normalized: dict[str, Any] = {}

    normalized["pod_restarts"] = [
        {
            "pod": s["metric"].get("pod", "unknown"),
            "container": s["metric"].get("container", ""),
            "value": _safe_float(s.get("value", [0, "0"])),
        }
        for s in raw.get("pod_restarts", [])
    ]

    normalized["oom_kills"] = [
        {
            "pod": s["metric"].get("pod", "unknown"),
            "container": s["metric"].get("container", ""),
            "value": _safe_float(s.get("value", [0, "0"])),
        }
        for s in raw.get("oom_kills_5m", [])
        if _safe_float(s.get("value", [0, "0"])) > 0
    ]

    normalized["cpu_usage"] = [
        {
            "pod": s["metric"].get("pod", "unknown"),
            "container": s["metric"].get("container", ""),
            "value": _safe_float(s.get("value", [0, "0"])),
        }
        for s in raw.get("cpu_usage_rate_2m", [])
    ]

    normalized["memory_percent"] = [
        {
            "pod": s["metric"].get("pod", "unknown"),
            "container": s["metric"].get("container", ""),
            "value": _safe_float(s.get("value", [0, "0"])),
        }
        for s in raw.get("memory_usage_percent", [])
    ]

    normalized["pod_status"] = [
        {
            "pod": s["metric"].get("pod", "unknown"),
            "phase": s["metric"].get("phase", "Unknown"),
        }
        for s in raw.get("pod_status", [])
    ]

    # replicas_available kept in Prometheus format — extract_replica_status reads it directly
    normalized["replicas_available"] = raw.get("replicas_available", [])

    return normalized


# --- Infrastructure score: 4-factor weighted model ---
#
# | Factor                      | Weight | Scoring Logic                                        |
# |-----------------------------|--------|------------------------------------------------------|
# | Pod Reliability (Restarts)  | 0.35   | 0: 0.0, 1-2: 0.4, 3-5: 0.7, >5: 1.0               |
# | Critical Errors (OOM)       | 0.25   | 0: 0.0, >0: 1.0                                     |
# | Pod Health Status            | 0.20   | Running: 0.0, Pending: 0.6, Failed/Unknown: 1.0     |
# | Resource Saturation          | 0.20   | Mem>90%: 1.0, CPU>1.0core: 0.8, Normal: 0.0         |


def compute_infrastructure_score(
    metrics: dict[str, Any],
    affected_nfs: list[str] | None = None,
) -> float:
    """Compute weighted infra score from pod metrics. Returns 0.0-1.0.

    Args:
        metrics: Metrics dict keyed by report label (from _fetch_prometheus_metrics).
        affected_nfs: Optional list of NF names to check for replica-absence.
    """
    cfg = get_config()
    score = 0.0

    # Factor 1: Pod Restarts
    restarts = metrics.get("pod_restarts", [])
    max_restarts = max(
        (entry.get("value", 0) for entry in restarts), default=0
    )
    if max_restarts > cfg.restart_threshold_critical:
        restart_factor = 1.0
    elif max_restarts >= cfg.restart_threshold_high:
        restart_factor = cfg.restart_factor_high
    elif max_restarts >= 1:
        restart_factor = cfg.restart_factor_low
    else:
        restart_factor = 0.0
    score += cfg.infra_weight_restarts * restart_factor

    # Factor 2: OOM kills
    oom_kills = metrics.get("oom_kills", [])
    score += cfg.infra_weight_oom * (1.0 if oom_kills else 0.0)

    # Factor 3: Pod Status
    # Priority: absent deployments (0 replicas) > failed/unknown pods > pending
    dag_nfs_lower = {nf.lower() for nf in (affected_nfs or [])}
    absent_dag_nfs = extract_replica_status(metrics) & dag_nfs_lower if dag_nfs_lower else set()

    if absent_dag_nfs:
        # A DAG-flow NF has 0 available replicas — definitively an infrastructure
        # event (deployment scaled to zero or crashlooping). Individual pod factors
        # are all zero because there are no pod objects, so we short-circuit to 1.0
        # instead of letting the weighted sum produce a misleadingly low score.
        return 1.0

    pod_status = metrics.get("pod_status", [])
    status_factor = 0.0
    for entry in pod_status:
        phase = entry.get("phase", "Running")
        if phase in ("Failed", "Unknown"):
            status_factor = max(status_factor, 1.0)
        elif phase == "Pending":
            status_factor = max(status_factor, cfg.pod_pending_factor)
    score += cfg.infra_weight_pod_status * status_factor

    # Factor 4: Resource Saturation
    memory_entries = metrics.get("memory_percent", [])
    cpu_entries = metrics.get("cpu_usage", [])
    max_mem = max(
        (entry.get("value", 0) for entry in memory_entries), default=0
    )
    max_cpu = max(
        (entry.get("value", 0) for entry in cpu_entries), default=0
    )
    if max_mem > cfg.memory_saturation_pct:
        resource_factor = 1.0
    elif max_cpu > cfg.cpu_saturation_cores:
        resource_factor = cfg.cpu_saturation_factor
    else:
        resource_factor = 0.0
    score += cfg.infra_weight_resources * resource_factor

    return min(score, 1.0)


def extract_restart_counts(metrics: dict[str, Any]) -> dict[str, int]:
    """Return {pod_name: restart_count} for all pods in pod_restarts."""
    result: dict[str, int] = {}
    for entry in metrics.get("pod_restarts", []):
        pod = entry.get("pod", "unknown")
        result[pod] = entry.get("value", 0)
    return result


def extract_oom_events(metrics: dict[str, Any]) -> dict[str, int]:
    """Return {pod_name: oom_count} for pods with OOM kills."""
    result: dict[str, int] = {}
    for entry in metrics.get("oom_kills", []):
        pod = entry.get("pod", "unknown")
        value = entry.get("value", 0)
        if value > 0:
            result[pod] = value
    return result


def extract_resource_metrics(metrics: dict[str, Any]) -> dict[str, dict[str, float]]:
    """Return {pod_name: {"cpu": float, "memory_percent": float}} from resource metrics."""
    result: dict[str, dict[str, float]] = {}
    for entry in metrics.get("cpu_usage", []):
        pod = entry.get("pod", "unknown")
        if pod not in result:
            result[pod] = {"cpu": 0.0, "memory_percent": 0.0}
        result[pod]["cpu"] = entry.get("value", 0.0)
    for entry in metrics.get("memory_percent", []):
        pod = entry.get("pod", "unknown")
        if pod not in result:
            result[pod] = {"cpu": 0.0, "memory_percent": 0.0}
        result[pod]["memory_percent"] = entry.get("value", 0.0)
    return result


def extract_node_status(metrics: dict[str, Any]) -> dict[str, str]:
    """Return {pod_name: phase_string} from pod_status entries."""
    result: dict[str, str] = {}
    for entry in metrics.get("pod_status", []):
        pod = entry.get("pod", "unknown")
        result[pod] = entry.get("phase", "Unknown")
    return result


def count_concurrent_failures(metrics: dict[str, Any]) -> int:
    """Count distinct pods experiencing any failure condition."""
    failing_pods: set[str] = set()

    for entry in metrics.get("pod_restarts", []):
        if entry.get("value", 0) > 0:
            failing_pods.add(entry.get("pod", "unknown"))

    for entry in metrics.get("oom_kills", []):
        if entry.get("value", 0) > 0:
            failing_pods.add(entry.get("pod", "unknown"))

    for entry in metrics.get("pod_status", []):
        phase = entry.get("phase", "Running")
        if phase not in ("Running",):
            failing_pods.add(entry.get("pod", "unknown"))

    return len(failing_pods)


def extract_critical_events(metrics: dict[str, Any]) -> list[dict[str, object]]:
    """Identify critical infrastructure events (OOM, high restarts, failed pods)."""
    cfg = get_config()
    events: list[dict[str, object]] = []

    for entry in metrics.get("oom_kills", []):
        if entry.get("value", 0) > 0:
            events.append({
                "type": "oom_kill",
                "pod": entry.get("pod", "unknown"),
                "container": entry.get("container", ""),
                "value": entry.get("value", 0),
            })

    for entry in metrics.get("pod_restarts", []):
        if entry.get("value", 0) > cfg.restart_threshold_critical:
            events.append({
                "type": "excessive_restarts",
                "pod": entry.get("pod", "unknown"),
                "container": entry.get("container", ""),
                "value": entry.get("value", 0),
            })

    for entry in metrics.get("pod_status", []):
        phase = entry.get("phase", "Running")
        if phase in ("Failed", "Unknown", "CrashLoopBackOff"):
            events.append({
                "type": "pod_failure",
                "pod": entry.get("pod", "unknown"),
                "phase": phase,
            })

    return events


def extract_nfs_from_alert(alert: dict[str, Any]) -> list[str]:
    """Extract affected NF names from alert labels."""
    known_nfs = frozenset(get_config().known_nfs)
    labels = alert.get("labels", {})
    nfs: list[str] = []

    # Primary: explicit 'nf' label (may be comma-separated)
    nf_label = labels.get("nf", "")
    if nf_label:
        for part in nf_label.split(","):
            name = part.strip().lower()
            if name:
                nfs.append(name)

    # Fallback: extract NF prefix from pod name label
    if not nfs:
        pod_label = labels.get("pod", "")
        if pod_label:
            prefix = pod_label.split("-")[0].lower()
            if prefix in known_nfs:
                nfs.append(prefix)

    return nfs


def compress_infra_findings_for_agent(
    infra_findings: dict[str, Any],
    infra_score: float,
    token_budget: int,
) -> dict[str, Any]:
    """Compress infra findings before storing in state.

    If infra_score == 0.0 and no critical events are present, returns a compact
    healthy-status sentinel instead of the full (mostly-empty) findings dict.

    Otherwise returns only problematic data:
      - pod_restarts: non-zero only
      - oom_kills: as-is (already filtered by extract_oom_events)
      - resource_usage: pods above CPU/memory saturation thresholds
      - node_health: non-Running pods only
      - concurrent_failures: only if > 0
      - critical_events: as-is

    If the result still exceeds token_budget, logs a WARNING and forwards anyway
    (correctness > budget).
    """
    cfg = get_config()

    has_issues = (
        infra_score > 0.0
        or bool(infra_findings.get("critical_events"))
        or bool(infra_findings.get("oom_kills"))
    )

    if not has_issues:
        return {"status": "all_pods_healthy"}

    compressed: dict[str, Any] = {}

    if infra_findings.get("critical_events"):
        compressed["critical_events"] = infra_findings["critical_events"]

    if infra_findings.get("oom_kills"):
        compressed["oom_kills"] = infra_findings["oom_kills"]

    nonzero_restarts = {
        pod: count
        for pod, count in infra_findings.get("pod_restarts", {}).items()
        if count > 0
    }
    if nonzero_restarts:
        compressed["pod_restarts"] = nonzero_restarts

    saturated_resources = {
        pod: res
        for pod, res in infra_findings.get("resource_usage", {}).items()
        if (
            res.get("memory_percent", 0.0) > cfg.memory_saturation_pct
            or res.get("cpu", 0.0) > cfg.cpu_saturation_cores
        )
    }
    if saturated_resources:
        compressed["resource_usage"] = saturated_resources

    unhealthy_nodes = {
        pod: phase
        for pod, phase in infra_findings.get("node_health", {}).items()
        if phase != "Running"
    }
    if unhealthy_nodes:
        compressed["node_health"] = unhealthy_nodes

    if infra_findings.get("concurrent_failures", 0) > 0:
        compressed["concurrent_failures"] = infra_findings["concurrent_failures"]

    if count_tokens(str(compressed)) > token_budget:
        logger.warning(
            "Compressed infra findings exceed token budget (%d tokens), forwarding anyway",
            count_tokens(str(compressed)),
        )

    return compressed or {"status": "all_pods_healthy"}


@traceable(name="InfraAgent")
def infra_agent(state: TriageState) -> dict[str, Any]:
    """InfraAgent entry point. Rule-based, no LLM."""
    try:
        cfg = get_config()
        alert = state["alert"]
        incident_id = state["incident_id"]

        alert_time = parse_timestamp(alert["startsAt"])
        start = int(alert_time - cfg.alert_lookback_seconds)
        end = int(alert_time + cfg.alert_lookahead_seconds)

        affected_nfs = extract_nfs_from_alert(alert)

        # Fetch Prometheus metrics via direct HTTP and normalize to internal format
        try:
            metrics_raw = asyncio.run(
                _fetch_prometheus_metrics(affected_nfs, start, end)
            )
            metrics = _normalize_prometheus_metrics(metrics_raw)
        except Exception:
            logger.warning(
                "Prometheus fetch failed, proceeding with empty metrics",
                exc_info=True,
            )
            metrics = {}

        infra_score = compute_infrastructure_score(metrics, affected_nfs)

        raw_findings: dict[str, Any] = {
            "pod_restarts": extract_restart_counts(metrics),
            "oom_kills": extract_oom_events(metrics),
            "resource_usage": extract_resource_metrics(metrics),
            "node_health": extract_node_status(metrics),
            "concurrent_failures": count_concurrent_failures(metrics),
            "critical_events": extract_critical_events(metrics),
        }

        # Save pre-filter snapshot (non-blocking, non-fatal)
        save_artifact(incident_id, "pre_filter_infra.json", raw_findings, cfg.artifacts_dir)

        compressed_findings = compress_infra_findings_for_agent(
            raw_findings, infra_score, cfg.rca_token_budget_infra
        )

        # Save post-filter snapshot
        save_artifact(
            incident_id, "post_filter_infra.json", compressed_findings, cfg.artifacts_dir
        )

        # Return only the keys this agent writes — avoids LangGraph parallel-merge conflict
        # with metrics_agent (both start from START in the same step).
        return {
            "infra_checked": True,
            "infra_score": infra_score,
            "infra_findings": compressed_findings,
        }
    except Exception:
        logger.exception("InfraAgent failed; returning safe defaults")
        return {"infra_checked": True, "infra_score": 0.0, "infra_findings": None}
