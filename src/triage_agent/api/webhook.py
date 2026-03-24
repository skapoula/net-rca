"""FastAPI webhook endpoint for Alertmanager."""
# ruff: noqa: N815 — camelCase field names match Alertmanager webhook JSON schema

import asyncio
import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from triage_agent.config import get_config
from triage_agent.graph import create_workflow, get_initial_state
from triage_agent.tracing import LocalTraceCallbackHandler

logger = logging.getLogger(__name__)

_cfg = get_config()

app = FastAPI(
    title="5G TriageAgent",
    description="Multi-Agent RCA System for 5G Core Network Failures",
    version=_cfg.app_version,
)

# CORS middleware — restrict allow_origins in production via CORS_ALLOW_ORIGINS env var.
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cfg.cors_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compile the workflow once at startup rather than per request
_workflow = create_workflow()

# Each value: {"ts": float (monotonic), "data": dict | None}
# data=None → pending; data=dict → complete or failed
_incident_store: dict[str, dict[str, Any]] = {}


def _evict_stale() -> None:
    """Remove incident entries older than cfg.incident_ttl_seconds."""
    cutoff = time.monotonic() - _cfg.incident_ttl_seconds
    stale = [k for k, v in _incident_store.items() if v["ts"] < cutoff]
    for k in stale:
        del _incident_store[k]


class IncidentResponse(BaseModel):
    """Response for GET /incidents/{incident_id}."""

    incident_id: str
    status: str  # "pending" | "complete" | "failed"
    final_report: dict[str, Any] | None = None


async def _run_triage(alert_dict: dict[str, Any], incident_id: str) -> None:
    """Run the LangGraph triage workflow in a background thread.

    Uses asyncio.to_thread to avoid nested event loop conflicts since the
    agent functions call asyncio.run() internally.
    """
    handler = LocalTraceCallbackHandler(
        incident_id=incident_id,
        artifacts_dir=_cfg.artifacts_dir,
    )
    try:
        initial_state = get_initial_state(alert=alert_dict, incident_id=incident_id)
        result = await asyncio.to_thread(
            _workflow.invoke,
            initial_state,
            {"callbacks": [handler]},
        )
        _incident_store[incident_id] = {"ts": time.monotonic(), "data": result.get("final_report") or {}}
        logger.info(
            f"Triage complete: incident_id={incident_id}, "
            f"report={result.get('final_report')}"
        )
    except Exception:
        logger.exception(f"Triage failed: incident_id={incident_id}")
        handler.flush()  # write partial trace even on workflow failure
        _incident_store[incident_id] = {"ts": time.monotonic(), "data": {"error": "triage_failed"}}


class AlertLabel(BaseModel):
    """Alertmanager alert labels."""

    alertname: str
    severity: str = "warning"
    namespace: str = "5g-core"
    nf: str | None = None


class AlertAnnotation(BaseModel):
    """Alertmanager alert annotations."""

    summary: str = ""
    description: str = ""


class Alert(BaseModel):
    """Single alert from Alertmanager."""

    status: str
    labels: AlertLabel
    annotations: AlertAnnotation = AlertAnnotation()
    startsAt: str
    endsAt: str = "0001-01-01T00:00:00Z"
    generatorURL: str = ""
    fingerprint: str = ""


class AlertmanagerPayload(BaseModel):
    """Alertmanager webhook payload."""

    receiver: str = "triage-agent"
    status: str
    alerts: list[Alert]
    groupLabels: dict[str, str] = {}
    commonLabels: dict[str, str] = {}
    commonAnnotations: dict[str, str] = {}
    externalURL: str = ""
    version: str = "4"
    groupKey: str = ""


class TriageResponse(BaseModel):
    """Response from triage endpoint."""

    incident_id: str
    status: str
    message: str
    alerts_received: int


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    timestamp: str
    memgraph: bool
    prometheus: bool
    loki: bool


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint for Kubernetes probes."""
    from triage_agent.mcp.client import MCPClient
    from triage_agent.memgraph.connection import get_memgraph

    # Check Memgraph
    try:
        memgraph = get_memgraph()
        memgraph_ok = memgraph.health_check()
    except Exception:
        memgraph_ok = False

    # Check MCP servers
    async with MCPClient() as mcp:
        prometheus_ok = await mcp.health_check_prometheus()
        loki_ok = await mcp.health_check_loki()

    overall_status = "healthy" if (memgraph_ok and prometheus_ok and loki_ok) else "degraded"

    return HealthResponse(
        status=overall_status,
        timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        memgraph=memgraph_ok,
        prometheus=prometheus_ok,
        loki=loki_ok,
    )


@app.post("/webhook", response_model=TriageResponse)
async def receive_alert(
    payload: AlertmanagerPayload, background_tasks: BackgroundTasks
) -> TriageResponse:
    """Receive alerts from Alertmanager and trigger triage workflow."""
    incident_id = str(uuid.uuid4())

    logger.info(
        f"Received {len(payload.alerts)} alerts, incident_id={incident_id}, "
        f"status={payload.status}"
    )

    if not payload.alerts:
        raise HTTPException(status_code=400, detail="No alerts in payload")

    # Only process firing alerts
    firing_alerts = [a for a in payload.alerts if a.status == "firing"]
    if not firing_alerts:
        return TriageResponse(
            incident_id=incident_id,
            status="skipped",
            message="No firing alerts to process",
            alerts_received=len(payload.alerts),
        )

    _evict_stale()
    _incident_store[incident_id] = {"ts": time.monotonic(), "data": None}  # pending
    background_tasks.add_task(_run_triage, firing_alerts[0].model_dump(), incident_id)

    return TriageResponse(
        incident_id=incident_id,
        status="accepted",
        message=f"Processing {len(firing_alerts)} firing alerts",
        alerts_received=len(payload.alerts),
    )


@app.get("/incidents/{incident_id}", response_model=IncidentResponse)
async def get_incident(incident_id: str) -> IncidentResponse:
    """Poll triage status for a given incident_id.

    Returns 404 if the incident_id is unknown (never submitted).
    Status is "pending" while the background task is running, "complete"
    when a final_report was produced, and "failed" if the workflow raised.
    """
    if incident_id not in _incident_store:
        raise HTTPException(status_code=404, detail=f"Unknown incident_id: {incident_id}")
    data = _incident_store[incident_id]["data"]
    if data is None:
        return IncidentResponse(incident_id=incident_id, status="pending")
    if "error" in data:
        return IncidentResponse(incident_id=incident_id, status="failed", final_report=data)
    return IncidentResponse(incident_id=incident_id, status="complete", final_report=data)


@app.get("/")
async def root() -> dict[str, Any]:
    """Root endpoint with API info."""
    return {
        "name": "5G TriageAgent",
        "version": get_config().app_version,
        "docs": "/docs",
        "health": "/health",
        "webhook": "/webhook",
    }
