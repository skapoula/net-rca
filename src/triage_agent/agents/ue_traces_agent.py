"""UeTracesAgent: IMSI trace discovery, construction, and Memgraph ingestion.

No LLM. Discovers active IMSIs in the alarm window via Loki, constructs
per-IMSI traces, ingests them into Memgraph, and runs deviation detection
against reference DAGs.

Two-path architecture (mirrors NfLogsAgent):
  1. Upfront health check — probe MCP server /ready endpoint.
  2. If MCP reachable → fetch logs via MCP client.
  3. If MCP unreachable → fetch logs via direct Loki HTTP API.

Pipeline:
  1. IMSI discovery pass (Loki query)
  2. Per-IMSI trace construction
  3. Memgraph ingestion + comparison against reference DAG
"""

import asyncio
import logging
import re
from typing import Any

import httpx
from langsmith import traceable

from triage_agent.config import get_config
from triage_agent.mcp.client import MCPClient
from triage_agent.memgraph.connection import get_memgraph
from triage_agent.state import TriageState
from triage_agent.utils import extract_nf_from_pod_name, parse_loki_response, parse_timestamp

logger = logging.getLogger(__name__)



# ---------------------------------------------------------------------------
# Pure functions (no I/O)
# ---------------------------------------------------------------------------


def extract_unique_imsis(logs: list[dict[str, Any]]) -> list[str]:
    """Scan log messages for IMSI pattern 'imsi-<N digits>' (N from config).

    Returns deduplicated list of IMSI digit strings (no 'imsi-' prefix).
    Preserves discovery order.
    """
    digit_length = get_config().imsi_digit_length
    pattern = re.compile(rf"(?i)imsi-(\d{{{digit_length}}})")
    seen: set[str] = set()
    result: list[str] = []
    for entry in logs:
        message = entry.get("message", "")
        for match in pattern.finditer(message):
            imsi = match.group(1)
            if imsi not in seen:
                seen.add(imsi)
                result.append(imsi)
    return result


def per_imsi_logql(imsi: str) -> str:
    """Build LogQL query for a specific IMSI in the configured core namespace."""
    ns = get_config().core_namespace
    return f'{{k8s_namespace_name="{ns}"}} |~ "{imsi}"'


def contract_imsi_trace(
    raw_trace: list[dict[str, Any]], imsi: str
) -> dict[str, Any]:
    """Contract raw log entries into a structured trace dict for Memgraph.

    Returns:
        {"imsi": str, "events": [{timestamp, nf, message}, ...]}
        Events are sorted chronologically by timestamp.
    """
    events: list[dict[str, Any]] = []
    for entry in raw_trace:
        events.append({
            "timestamp": entry.get("timestamp", 0),
            "nf": extract_nf_from_pod_name(entry.get("pod", "unknown")),
            "message": entry.get("message", ""),
        })
    events.sort(key=lambda e: e["timestamp"])
    for i, event in enumerate(events):
        event["order"] = i
    return {"imsi": imsi, "events": events}


# ---------------------------------------------------------------------------
# Loki two-path: MCP + direct HTTP
# ---------------------------------------------------------------------------


async def _check_mcp_available() -> bool:
    """Lightweight MCP health check: probe Loki /ready via MCP server."""
    try:
        async with MCPClient() as client:
            return await client.health_check_loki()
    except Exception:
        return False


async def _fetch_loki_logs_mcp(
    query: str,
    start: int,
    end: int,
) -> list[dict[str, Any]]:
    """Fetch logs from Loki via MCP client."""
    async with MCPClient() as client:
        logs = await client.query_loki(query, start=start, end=end)

    # Normalize pod field: MCPClient reads labels["pod"] which may be empty
    # when Loki uses k8s-style labels (k8s_pod_name).
    for entry in logs:
        if not entry.get("pod"):
            entry["pod"] = entry.get("labels", {}).get("k8s_pod_name", "")

    return logs


async def _fetch_loki_logs_direct(
    query: str,
    start: int,
    end: int,
) -> list[dict[str, Any]]:
    """Fetch logs directly from Loki HTTP API, bypassing MCP server."""
    config = get_config()
    async with httpx.AsyncClient(timeout=config.mcp_timeout) as client:
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
            return parse_loki_response(response.json())
        except httpx.TimeoutException:
            logger.warning("Loki direct query timed out: %s", query)
        except httpx.HTTPStatusError as exc:
            logger.warning("Loki direct query HTTP error: %s — %s", query, exc)
        except Exception:
            logger.warning("Loki direct query failed: %s", query, exc_info=True)
    return []


def loki_query(
    logql: str,
    start: int,
    end: int,
    use_mcp: bool | None = None,
) -> list[dict[str, Any]]:
    """Execute a Loki query with two-path architecture.

    1. Probe MCP server availability (lightweight /ready check) — skipped when
       ``use_mcp`` is provided by the caller (allows batching a single health
       check across many queries).
    2. If reachable → MCP path.
    3. If unreachable → direct Loki HTTP path.
    """
    # Step 1: Health check (only when not provided by caller)
    if use_mcp is None:
        try:
            use_mcp = asyncio.run(_check_mcp_available())
        except Exception:
            logger.warning(
                "MCP health check failed, defaulting to direct Loki",
                exc_info=True,
            )
            use_mcp = False

    # Step 2: Execute query on chosen path
    if use_mcp:
        try:
            return asyncio.run(
                _fetch_loki_logs_mcp(logql, start=start, end=end)
            )
        except Exception:
            logger.warning(
                "MCP query failed despite passing health check,"
                " returning empty results",
                exc_info=True,
            )
            return []
    else:
        logger.info("MCP server unavailable, using direct Loki connection")
        try:
            return asyncio.run(
                _fetch_loki_logs_direct(logql, start=start, end=end)
            )
        except Exception:
            logger.warning(
                "Direct Loki query failed, returning empty results",
                exc_info=True,
            )
            return []


async def _fetch_imsi_trace_async(
    imsi: str,
    start: int,
    end: int,
    use_mcp: bool,
) -> dict[str, Any]:
    """Fetch and contract a single IMSI trace asynchronously."""
    logql = per_imsi_logql(imsi)
    if use_mcp:
        try:
            raw = await _fetch_loki_logs_mcp(logql, start=start, end=end)
        except Exception:
            logger.warning(
                "MCP query failed for IMSI %s, returning empty trace",
                imsi,
                exc_info=True,
            )
            raw = []
    else:
        raw = await _fetch_loki_logs_direct(logql, start=start, end=end)
    return contract_imsi_trace(raw, imsi)


async def _discover_and_build_traces_async(
    discovery_logql: str,
    discovery_start: int,
    discovery_end: int,
    trace_start: int,
    trace_end: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Discover IMSIs and fetch all per-IMSI traces in a single event loop.

    Performs exactly one MCP health check, then runs the discovery query and
    all per-IMSI trace fetches without creating additional event loops.
    """
    use_mcp = await _check_mcp_available()
    if not use_mcp:
        logger.info("MCP server unavailable, using direct Loki for IMSI traces")

    # Discovery query
    if use_mcp:
        discovery_logs = await _fetch_loki_logs_mcp(
            discovery_logql, start=discovery_start, end=discovery_end
        )
    else:
        discovery_logs = await _fetch_loki_logs_direct(
            discovery_logql, start=discovery_start, end=discovery_end
        )
    imsis = extract_unique_imsis(discovery_logs)
    if not imsis:
        return [], []

    # Per-IMSI traces — all concurrent, MCP choice already resolved
    tasks = [_fetch_imsi_trace_async(imsi, trace_start, trace_end, use_mcp) for imsi in imsis]
    traces = list(await asyncio.gather(*tasks))
    return imsis, traces


# ---------------------------------------------------------------------------
# Memgraph interactions
# ---------------------------------------------------------------------------


def ingest_traces_to_memgraph(
    traces: list[dict[str, Any]], incident_id: str
) -> None:
    """Ingest per-IMSI traces into Memgraph using a single batch write."""
    if not traces:
        return
    get_memgraph().ingest_captured_traces_batch(incident_id, traces)


def run_deviation_detection_for_dag(
    incident_id: str, dag_name: str
) -> list[dict[str, Any]]:
    """Compare all ingested traces against a reference DAG in a single query."""
    return get_memgraph().detect_deviations_batch(incident_id, dag_name)


# ---------------------------------------------------------------------------
# Agent entry point
# ---------------------------------------------------------------------------


@traceable(name="UeTracesAgent")
def ue_traces_agent(state: TriageState) -> dict[str, Any]:
    """UeTracesAgent entry point. Pure MCP query + Memgraph, no LLM.

    Pipeline:
      1. IMSI discovery (from state.logs if available, else Loki query)
      2. Per-IMSI trace construction (wider window for full procedure)
      3. Memgraph ingestion + per-procedure deviation detection
    """
    dags = state.get("dags") or []
    if not dags:
        return {"discovered_imsis": [], "traces_ready": False, "trace_deviations": None}

    cfg = get_config()
    alert_time = int(parse_timestamp(state["alert"]["startsAt"]))

    # 1+2. IMSI discovery + per-IMSI trace construction in a single event loop.
    # One MCP health check, one discovery query, then all IMSI fetches concurrent.
    # Note: ue_traces_agent runs in the same parallel superstep as logs_agent,
    # so state["logs"] is not yet populated here. Discovery always uses Loki.
    discovery_logql = (
        f'{{k8s_namespace_name="{cfg.core_namespace}"}} |~ "(?i)imsi-"'
    )
    imsis, traces = asyncio.run(
        _discover_and_build_traces_async(
            discovery_logql=discovery_logql,
            discovery_start=alert_time - cfg.imsi_discovery_window_seconds,
            discovery_end=alert_time + cfg.imsi_discovery_window_seconds,
            trace_start=alert_time - cfg.imsi_trace_lookback_seconds,
            trace_end=alert_time + cfg.alert_lookahead_seconds,
        )
    )

    # 3. Ingest into Memgraph
    ingest_traces_to_memgraph(traces, state["incident_id"])

    # 4. Per-procedure deviation detection
    trace_deviations: dict[str, list[dict[str, Any]]] = {}
    for dag in dags:
        dag_name = dag.get("name", "")
        if dag_name:
            trace_deviations[dag_name] = run_deviation_detection_for_dag(
                state["incident_id"], dag_name
            )

    return {
        "discovered_imsis": imsis,
        "traces_ready": True,
        "trace_deviations": trace_deviations,
    }
