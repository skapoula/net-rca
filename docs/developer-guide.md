# Developer Guide

Technical architecture reference for contributors and integrators. For agent authoring patterns
see [Agent Development](agent-development.md); for all environment variables see
[Configuration Reference](configuration-reference.md); for REST endpoints see the FastAPI
auto-docs at `http://localhost:8000/docs` when running locally.

---

## System Architecture

5G TriageAgent is a four-layer system: an **entry layer** (FastAPI webhook), an **agentic
pipeline** (LangGraph directed graph), a **data layer** (Prometheus + Loki via MCP, Memgraph
graph DB), and a **support layer** (config, tracing, utils).

All agents except `RCAAgent` are deterministic — they query external data sources and apply
rule-based scoring. Only `RCAAgent` calls an LLM.

### C4 Context

```{mermaid}
C4Context
    title 5G TriageAgent — System Context

    Person(noc, "NOC Engineer", "Receives and reviews RCA reports")
    System(triage, "5G TriageAgent", "Orchestrates multi-agent RCA pipeline for 5G core failures")
    System_Ext(alertmanager, "Prometheus Alertmanager", "Fires alert webhooks on threshold breaches")
    System_Ext(prometheus, "Prometheus", "Time-series metrics for pods and NFs")
    System_Ext(loki, "Loki", "Log aggregation for 5G core NFs")
    System_Ext(llm, "LLM API", "OpenAI / Anthropic / local vLLM — used by RCAAgent only")
    System_Ext(langsmith, "LangSmith", "Distributed tracing and evaluation")

    Rel(alertmanager, triage, "POST /webhook", "HTTP")
    Rel(triage, prometheus, "PromQL queries", "HTTP/MCP")
    Rel(triage, loki, "LogQL queries", "HTTP/MCP")
    Rel(triage, llm, "Structured prompt", "HTTPS")
    Rel(triage, langsmith, "Spans + traces", "HTTPS")
    Rel(triage, noc, "RCA report", "HTTP JSON")
```

### C4 Container

```{mermaid}
C4Container
    title 5G TriageAgent — Containers

    Container(webhook, "FastAPI Webhook", "Python / uvicorn", "Receives Alertmanager webhooks; exposes GET /incidents/{id}")
    Container(pipeline, "LangGraph Pipeline", "Python / LangGraph", "Directed graph of 7 agent nodes; manages TriageState")
    Container(memgraph, "Memgraph", "In-memory graph DB", "Stores 3GPP reference DAGs and captured IMSI traces (Bolt :7687)")
    Container(artifacts, "Artifact Store", "Local filesystem", "Per-incident JSON snapshots at artifacts_dir/<incident_id>/")

    Rel(webhook, pipeline, "invoke(initial_state)", "in-process")
    Rel(pipeline, memgraph, "Cypher queries", "Bolt / neo4j driver")
    Rel(pipeline, artifacts, "save_artifact()", "filesystem")
```

### C4 Component

```{mermaid}
C4Component
    title LangGraph Pipeline — Components

    Component(infra, "InfraAgent", "infra_agent.py", "Scores infrastructure health from pod metrics")
    Component(dagmapper, "DagMapper", "dag_mapper.py", "Maps alert labels to 3GPP procedure DAGs from Memgraph")
    Component(metrics, "NfMetricsAgent", "metrics_agent.py", "Collects per-NF Prometheus metrics")
    Component(logs, "NfLogsAgent", "logs_agent.py", "Collects per-NF Loki logs")
    Component(traces, "UeTracesAgent", "ue_traces_agent.py", "Discovers IMSIs; ingests traces into Memgraph; detects DAG deviations")
    Component(eq, "EvidenceQuality", "evidence_quality.py", "Grades evidence completeness; sets retry gate")
    Component(join, "join_for_rca", "rca_agent.py", "Barrier node — compresses all evidence into LLM-sized sections")
    Component(rca, "RCAAgent", "rca_agent.py", "Only LLM-calling agent; produces structured RCA report")
```

---

## Pipeline Workflow

The pipeline is a compiled LangGraph `StateGraph`. Every node has the signature
`node(state: TriageState) -> dict[str, Any]` and **must never raise** — errors are written
into state fields (e.g. empty dicts or `None`) so downstream agents degrade gracefully.

### Workflow

```{mermaid}
flowchart TD
    START([START]) --> infra_agent & dag_mapper

    dag_mapper --> metrics_agent & logs_agent & ue_traces_agent

    metrics_agent --> evidence_quality
    logs_agent --> evidence_quality
    ue_traces_agent --> evidence_quality

    infra_agent --> join_for_rca
    evidence_quality --> join_for_rca

    join_for_rca --> rca_agent

    rca_agent --> retry{"should_retry?\nneeds_more_evidence\nAND attempt < max"}
    retry -->|retry| increment_attempt --> rca_agent
    retry -->|finalize| finalize_report --> END([END])
```

### Node Reference

::::{grid} 1 1 2 2
:gutter: 2

:::{grid-item-card} InfraAgent
**LLM calls:** 0 (rule-based)

**Reads:** `alert`

**Writes:** `infra_score`, `infra_findings`, `infra_checked`

Queries Prometheus for pod restarts, OOM kills, CPU/memory usage, and pod status across the
`core_namespace`. Produces a weighted `infra_score` (0.0–1.0). Score ≥ 0.80 → infrastructure
root cause; ≥ 0.60 → possible infra-triggered app failure; < 0.30 → pure application failure.
:::

:::{grid-item-card} DagMapper
**LLM calls:** 0 (Memgraph query + keyword match)

**Reads:** `alert`

**Writes:** `procedure_names`, `dag_ids`, `dags`, `nf_union`, `mapping_confidence`, `mapping_method`

Maps alert labels to one or more 3GPP procedure DAGs stored in Memgraph. Tries, in order:
exact match on `alertname`, keyword match on alert labels, NF-specific fallback, then generic
fallback. Fans out the three collection agents with the resolved `nf_union`.
:::

:::{grid-item-card} NfMetricsAgent
**LLM calls:** 0 (MCP query)

**Reads:** `nf_union`, `alert`

**Writes:** `metrics`

Queries Prometheus via MCP for error rate, p95 latency, and CPU per NF in `nf_union`.
Returns `metrics` as `{nf_name: {error_rate, latency_p95, cpu_cores}}`.
:::

:::{grid-item-card} NfLogsAgent
**LLM calls:** 0 (MCP query)

**Reads:** `nf_union`, `alert`

**Writes:** `logs`

Queries Loki via MCP for error-level log lines per NF within the alert time window.
Falls back to direct HTTP if MCP times out. Returns `logs` as `{nf_name: [log_entries]}`.
:::

:::{grid-item-card} UeTracesAgent
**LLM calls:** 0 (MCP query + Memgraph)

**Reads:** `nf_union`, `alert`, `dags`

**Writes:** `discovered_imsis`, `traces_ready`, `trace_deviations`

Discovers active IMSIs from Loki during the alert window; ingests their signalling
sequences into Memgraph as `CapturedTrace` nodes; compares against reference `ReferenceTrace`
DAGs to identify step deviations.
:::

:::{grid-item-card} EvidenceQuality
**LLM calls:** 0 (rule-based)

**Reads:** `metrics`, `logs`, `trace_deviations`

**Writes:** `evidence_quality_score`, `evidence_gaps`, `needs_more_evidence`

Assigns a fixed quality score based on which evidence sources are populated (see
[Configuration Reference](configuration-reference.md#evidence-quality-and-compression)).
Sets `needs_more_evidence = True` when `confidence < min_confidence_default` and
`attempt_count < max_attempts`.
:::

:::{grid-item-card} join_for_rca
**LLM calls:** 0 (deterministic compression)

**Reads:** all state fields

**Writes:** `compressed_evidence`

Barrier node — waits for both `infra_agent` and `evidence_quality` to complete.
Calls `compress_evidence()` to truncate each evidence section to its token budget before
passing to the LLM. Uses `Annotated[..., _last_write]` reducer so retry attempts
overwrite the previous compression.
:::

:::{grid-item-card} RCAAgent
**LLM calls:** 1 per attempt

**Reads:** `compressed_evidence`

**Writes:** `root_nf`, `failure_mode`, `layer`, `confidence`, `evidence_chain`

The only LLM-calling agent. Sends compressed evidence to the configured LLM provider
via a structured prompt. Parses the response into `RCAResult` (Pydantic model).
Sets `needs_more_evidence` if `confidence < min_confidence_default`. Supports up to
`max_attempts` retries with exponential backoff.
:::

::::

---

## Shared State Object

All agents communicate through `TriageState` — a `TypedDict` defined in
`src/triage_agent/state.py`. Every node reads what it needs and returns only the fields
it owns. LangGraph merges deltas automatically.

### State Schema

| Field                    | Type                                             | Written by                      | Read by                                          |
| ------------------------ | ------------------------------------------------ | ------------------------------- | ------------------------------------------------ |
| `alert`                  | `dict[str, Any]`                                 | webhook (input)                 | all agents                                       |
| `incident_id`            | `str`                                            | webhook (input)                 | all agents                                       |
| `infra_checked`          | `bool`                                           | `infra_agent`                   | —                                                |
| `infra_score`            | `float`                                          | `infra_agent`                   | `join_for_rca`, `rca_agent`                      |
| `infra_findings`         | `dict \| None`                                   | `infra_agent`                   | `join_for_rca`                                   |
| `procedure_names`        | `list[str] \| None`                              | `dag_mapper`                    | `join_for_rca`, `finalize_report`                |
| `dag_ids`                | `list[str] \| None`                              | `dag_mapper`                    | `ue_traces_agent`                                |
| `dags`                   | `list[dict] \| None`                             | `dag_mapper`                    | `metrics_agent`, `logs_agent`, `ue_traces_agent` |
| `nf_union`               | `list[str] \| None`                              | `dag_mapper`                    | `metrics_agent`, `logs_agent`, `ue_traces_agent` |
| `mapping_confidence`     | `float`                                          | `dag_mapper`                    | `finalize_report`                                |
| `mapping_method`         | `str`                                            | `dag_mapper`                    | `finalize_report`                                |
| `metrics`                | `dict \| None`                                   | `metrics_agent`                 | `evidence_quality`, `join_for_rca`               |
| `logs`                   | `dict \| None`                                   | `logs_agent`                    | `evidence_quality`, `join_for_rca`               |
| `discovered_imsis`       | `list[str] \| None`                              | `ue_traces_agent`               | —                                                |
| `traces_ready`           | `bool`                                           | `ue_traces_agent`               | `evidence_quality`                               |
| `trace_deviations`       | `dict \| None`                                   | `ue_traces_agent`               | `evidence_quality`, `join_for_rca`               |
| `evidence_quality_score` | `float`                                          | `evidence_quality`              | `join_for_rca`, `rca_agent`, `should_retry`      |
| `evidence_gaps`          | `list[str] \| None`                              | `evidence_quality`              | `rca_agent`                                      |
| `needs_more_evidence`    | `bool`                                           | `evidence_quality`, `rca_agent` | `should_retry`                                   |
| `compressed_evidence`    | `Annotated[dict[str, str] \| None, _last_write]` | `join_for_rca`                  | `rca_agent`                                      |
| `root_nf`                | `str \| None`                                    | `rca_agent`                     | `finalize_report`                                |
| `failure_mode`           | `str \| None`                                    | `rca_agent`                     | `finalize_report`                                |
| `layer`                  | `str`                                            | `rca_agent`                     | `finalize_report`                                |
| `confidence`             | `float`                                          | `rca_agent`                     | `should_retry`, `finalize_report`                |
| `evidence_chain`         | `list[dict]`                                     | `rca_agent`                     | `finalize_report`                                |
| `attempt_count`          | `int`                                            | `increment_attempt`             | `should_retry`, `finalize_report`                |
| `max_attempts`           | `int`                                            | webhook (input)                 | `should_retry`                                   |
| `final_report`           | `dict \| None`                                   | `finalize_report`               | webhook response                                 |

### State Flow Diagram

```{mermaid}
stateDiagram-v2
    [*] --> alert_received: POST /webhook
    alert_received --> parallel_start: create initial TriageState

    state parallel_start <<fork>>
    parallel_start --> InfraAgent
    parallel_start --> DagMapper

    state DagMapper_fan <<fork>>
    DagMapper --> DagMapper_fan: writes nf_union
    DagMapper_fan --> NfMetricsAgent
    DagMapper_fan --> NfLogsAgent
    DagMapper_fan --> UeTracesAgent

    state evidence_join <<join>>
    NfMetricsAgent --> evidence_join
    NfLogsAgent --> evidence_join
    UeTracesAgent --> evidence_join
    evidence_join --> EvidenceQuality

    state rca_barrier <<join>>
    InfraAgent --> rca_barrier
    EvidenceQuality --> rca_barrier
    rca_barrier --> join_for_rca: compress evidence

    join_for_rca --> RCAAgent

    RCAAgent --> retry_check
    retry_check --> increment_attempt: needs_more_evidence AND attempt < max
    increment_attempt --> RCAAgent
    retry_check --> finalize_report: confidence OK or max reached
    finalize_report --> [*]: final_report written
```

---

## Data Layer

### MCP Client (Prometheus + Loki)

`src/triage_agent/mcp/client.py` — `MCPClient` wraps all Prometheus and Loki HTTP calls.
Agents never call these APIs directly.

**Key methods:**

| Method                                            | Returns      | Notes                                                                          |
| ------------------------------------------------- | ------------ | ------------------------------------------------------------------------------ |
| `query_prometheus(query)`                         | `dict`       | Instant query; retries on HTTP 429                                             |
| `query_prometheus_range(query, start, end, step)` | `dict`       | Range query; `start`/`end` are Unix int seconds                                |
| `query_loki(logql, start, end, limit)`            | `list[dict]` | `start`/`end` are Unix int seconds — client converts to nanoseconds internally |

All methods are `async`. Call with `asyncio.run()` from sync agents or `await` from async ones.

### Memgraph (Graph DB)

`src/triage_agent/memgraph/connection.py` — `get_memgraph()` returns a singleton
`MemgraphConnection`. Uses the `neo4j` Python driver (Bolt-compatible).

**Key methods:**

| Method                                | Returns                | Notes                                                            |
| ------------------------------------- | ---------------------- | ---------------------------------------------------------------- |
| `execute_cypher(query, params)`       | `list[dict[str, Any]]` | Read queries; retries on `ServiceUnavailable` / `TransientError` |
| `execute_cypher_write(query, params)` | `None`                 | Write queries; same retry policy                                 |

Retry policy: exponential backoff `2^attempt` seconds, up to `memgraph_max_retries` attempts.

### Reference DAG Schema

3GPP procedure DAGs are pre-loaded from Cypher files in `dags/` via an init container.

```
(:Procedure {name: "registration_general"})
    -[:HAS_DAG]->
(:ReferenceTrace {name: "Registration_General"})
    -[:STEP {order: 1}]->
(:RefEvent {name: "UE_sends_Registration_Request", nf: "AMF"})
```

Captured IMSI traces are written by `UeTracesAgent` during a live incident:

```
(:CapturedTrace {incident_id: "...", imsi: "..."})
    -[:STEP {order: 1, timestamp: "..."}]->
(:CapturedEvent {name: "...", nf: "..."})
```

Deviation detection compares `CapturedTrace` step sequences against `ReferenceTrace` steps
using Cypher subgraph matching.

---

## API

The FastAPI webhook is defined in `src/triage_agent/api/webhook.py`.

### `POST /webhook`

Alertmanager fires this endpoint for each alert group.

**Request body** (Alertmanager v2 format):

```json
{
  "version": "4",
  "groupKey": "...",
  "status": "firing",
  "receiver": "triage-agent",
  "alerts": [
    {
      "status": "firing",
      "labels": {
        "alertname": "RegistrationFailures",
        "namespace": "5g-core",
        "severity": "critical"
      },
      "startsAt": "2025-01-15T10:30:00Z"
    }
  ]
}
```

**Response** `202 Accepted`:

```json
{
  "incident_id": "abc123",
  "status": "accepted",
  "message": "Triage started"
}
```

The pipeline runs asynchronously. Poll `GET /incidents/{incident_id}` for results.

### `GET /incidents/{incident_id}`

Returns the current status and final report for a completed incident.

**Response** `200 OK` (completed):

```json
{
  "incident_id": "abc123",
  "status": "completed",
  "report": {
    "layer": "application",
    "root_nf": "AMF",
    "failure_mode": "Registration timeout — AUSF unreachable",
    "confidence": 0.88,
    "evidence_chain": [{ "step": "AMF pod restarted 3 times", "nf": "AMF" }],
    "infra_score": 0.12,
    "evidence_quality_score": 0.95,
    "attempt_count": 1,
    "procedure_names": ["registration_general"],
    "mapping_confidence": 0.9,
    "mapping_method": "exact_match",
    "nf_union": ["AMF", "AUSF", "UDM"]
  }
}
```

### `GET /health`

Liveness probe. Returns `{"status": "ok"}`.

### `GET /metrics`

Returns basic pipeline metrics (incident count, average confidence, average duration).

---

## Configuration

All configuration is loaded from environment variables (or a `.env` file) via Pydantic Settings.
Call `get_config()` once at module load time inside your agent — the result is cached via
`@lru_cache`.

```python
from triage_agent.config import get_config
cfg = get_config()
```

Key config categories:

| Category             | Key variables                                                      |
| -------------------- | ------------------------------------------------------------------ |
| Infrastructure       | `MEMGRAPH_HOST`, `MEMGRAPH_PORT`, `CORE_NAMESPACE`, `KNOWN_NFS`    |
| Service connectivity | `PROMETHEUS_URL`, `LOKI_URL`, `MCP_TIMEOUT`                        |
| LLM                  | `LLM_PROVIDER`, `LLM_MODEL`, `LLM_API_KEY`, `LLM_BASE_URL`         |
| Pipeline flow        | `MAX_ATTEMPTS`, `MIN_CONFIDENCE_DEFAULT`, `ALERT_LOOKBACK_SECONDS` |
| Scoring              | `INFRA_WEIGHT_*`, `RESTART_THRESHOLD_*`, `EQ_SCORE_*`              |
| Observability        | `LANGCHAIN_TRACING_V2`, `LANGSMITH_API_KEY`, `ARTIFACTS_DIR`       |

See [Configuration Reference](configuration-reference.md) for the complete table with defaults.

---

## LangSmith Observability

Every agent function must carry `@traceable(name="AgentName")` from `langsmith`. Without it,
the function is invisible in LangSmith traces and production debugging becomes very hard.

```python
from langsmith import traceable

@traceable(name="MyAgent")
def my_agent(state: TriageState) -> dict[str, Any]:
    ...
```

Enable tracing by setting `LANGCHAIN_TRACING_V2=true` and `LANGSMITH_API_KEY=<key>`.
The `setup_langsmith()` function in `src/triage_agent/tracing.py` is called at startup and
wires the necessary environment variables automatically when tracing is enabled.

---

## Deployment

### Kubernetes (Production)

The recommended deployment uses three containers in a single pod:

| Container           | Image               | Role                                      |
| ------------------- | ------------------- | ----------------------------------------- |
| `triage-agent`      | Application image   | Main FastAPI + LangGraph process          |
| `memgraph`          | `memgraph/memgraph` | Graph DB sidecar (localhost Bolt)         |
| `dag-loader` (init) | `mgconsole`         | Loads Cypher DAG files before main starts |

Manifests are in `k8s/`. Apply in order:

```bash
kubectl apply -f k8s/deployment-with-init.yaml
kubectl apply -f k8s/alertmanager-webhook.yaml
```

See [Memgraph Sidecar Guide](memgraph-sidecar-guide.md) for sidecar configuration details.

### Local Development

```bash
# 1. Install dependencies
uv sync                   # preferred — uses uv.lock
# or: pip install -e ".[dev]"

# 2. Copy and edit environment
cp triage-agent.env.example .env
# Set GROQ_API_KEY (or LLM_API_KEY), PROMETHEUS_URL, LOKI_URL

# 3. Start Memgraph locally
docker run -d -p 7687:7687 memgraph/memgraph:latest

# 4. Load reference DAGs
mgconsole < dags/registration_general.cypher
mgconsole < dags/authentication_5g_aka.cypher
mgconsole < dags/pdu_session_establishment.cypher

# 5. Start the webhook server
uv run uvicorn triage_agent.api.webhook:app --reload --port 8000

# 6. Verify
curl http://localhost:8000/health
```

---

## Testing

### Running Tests

```bash
# Unit tests (no external deps)
uv run pytest tests/unit/ -v

# With coverage
uv run pytest tests/unit/ --cov=triage_agent --cov-report=term-missing

# Integration tests (requires live Memgraph)
uv run pytest tests/integration/ --memgraph-url bolt://localhost:7687

# E2E tests (requires running service)
uv run pytest tests/e2e/ --alert-webhook http://localhost:8000/webhook
```

### Test Structure

```
tests/
├── conftest.py            # sample_initial_state fixture (fully populated TriageState)
├── unit/
│   ├── test_infra_agent.py
│   ├── test_dag_mapper.py
│   ├── test_metrics_agent.py
│   ├── test_logs_agent.py
│   ├── test_ue_traces_agent.py
│   ├── test_evidence_quality.py
│   ├── test_rca_agent.py
│   └── test_graph.py
├── integration/
│   └── test_memgraph_connection.py
└── e2e/
    └── test_webhook_flow.py
```

Unit tests mock all external connections (MCP, Memgraph, LLM). Use the `sample_initial_state`
fixture from `conftest.py` as the base state for every agent test.

See [Agent Development](agent-development.md#testing-patterns) for mock patterns.

---

## Project Structure

```
net-rca/
├── .readthedocs.yaml          # ReadTheDocs build configuration
├── pyproject.toml             # Project metadata and dependencies
├── triage-agent.env.example   # Environment variable template
├── src/triage_agent/
│   ├── config.py              # TriageAgentConfig (Pydantic Settings, @lru_cache)
│   ├── state.py               # TriageState TypedDict — shared across all agents
│   ├── graph.py               # create_workflow(), LangGraph workflow definition
│   ├── tracing.py             # LangSmith @traceable setup
│   ├── utils.py               # save_artifact() and shared helpers
│   ├── agents/
│   │   ├── infra_agent.py     # InfraAgent — rule-based pod metrics scoring
│   │   ├── dag_mapper.py      # DagMapper — alert → 3GPP DAG resolution
│   │   ├── metrics_agent.py   # NfMetricsAgent — Prometheus per-NF metrics
│   │   ├── logs_agent.py      # NfLogsAgent — Loki per-NF logs
│   │   ├── ue_traces_agent.py # UeTracesAgent — IMSI discovery + trace deviation
│   │   ├── evidence_quality.py# EvidenceQuality gate — scores evidence completeness
│   │   └── rca_agent.py       # RCAAgent + join_for_rca — LLM analysis
│   ├── api/
│   │   └── webhook.py         # FastAPI routes: POST /webhook, GET /incidents/{id}
│   ├── mcp/
│   │   └── client.py          # MCPClient — Prometheus + Loki HTTP wrappers
│   └── memgraph/
│       └── connection.py      # MemgraphConnection singleton, Cypher helpers
├── dags/                      # 3GPP procedure DAGs as Cypher scripts
│   ├── registration_general.cypher
│   ├── authentication_5g_aka.cypher
│   └── pdu_session_establishment.cypher
├── k8s/                       # Kubernetes manifests
│   ├── deployment-with-init.yaml
│   └── alertmanager-webhook.yaml
├── tests/                     # pytest test suite
└── docs/                      # Sphinx / ReadTheDocs source
```

---

## Conventions and Hard Rules

| Rule                                                         | Reason                                                                                                                                                                                                   |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| No LLM calls outside `rca_agent.py`                          | All other agents are deterministic by design; adding LLM calls breaks reproducibility                                                                                                                    |
| No Redis — use Memgraph                                      | Memgraph stores 3GPP procedure DAGs as a graph; Redis is a key-value cache                                                                                                                               |
| No hardcoded PromQL in agent functions                       | Queries live in config constants so they can be tested and updated centrally                                                                                                                             |
| `@traceable` required on every agent                         | LangSmith invisibility makes production debugging impossible                                                                                                                                             |
| No blocking I/O in async functions                           | Use `httpx`, not `requests`; use `asyncio.to_thread()` for sync libraries                                                                                                                                |
| No state mutation outside agent return dicts                 | LangGraph state is passed through the graph; in-place side effects corrupt the workflow                                                                                                                  |
| Always use `state.get("field") or default`                   | State fields may be `None` if an upstream agent failed or was skipped                                                                                                                                    |
| `eq_score_metrics_logs` must equal `high_evidence_threshold` | If they diverge, the relaxed confidence gate never (or always) activates — see [Configuration Reference](configuration-reference.md#constraint-eq_score_metrics_logs-must-equal-high_evidence_threshold) |
