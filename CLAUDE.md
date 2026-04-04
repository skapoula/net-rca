# CLAUDE.md — net-rca

> **Scope:** PROJECT-LEVEL — inherits org-wide policy from `/workspace/CLAUDE.md`.
> Rules here extend or override global where they conflict.
> Personal overrides go in `CLAUDE.local.md` (auto-gitignored).

<!-- Global context loaded automatically via directory traversal — no import needed. -->

## Project: 5G TriageAgent

### What This Is
A multi-agent LangGraph orchestration system for real-time root cause analysis of 5G core network failures. When Prometheus Alertmanager fires an alert, the system coordinates specialized agents to localize failures across infrastructure, NF, and 3GPP procedure layers.

### Architecture
See `docs/triageagent_architecture_design2.md` for the full design. The pipeline is:
```
InfraAgent (parallel) → NfMetricsAgent + NfLogsAgent + UeTracesAgent (parallel) → EvidenceQuality → RCAAgent
```

### Project Structure

```
src/triage_agent/
├── state.py              # TriageState TypedDict — shared across all agents
├── graph.py              # build_graph(), LangGraph workflow definition
├── config.py             # Pydantic Settings, @lru_cache singleton
├── tracing.py            # LangSmith @traceable setup
├── utils.py              # Shared helpers
├── agents/
│   ├── infra_agent.py    # InfraAgent — Prometheus metrics, deterministic
│   ├── metrics_agent.py  # NfMetricsAgent — per-NF metric analysis
│   ├── logs_agent.py     # NfLogsAgent — Loki log queries
│   ├── ue_traces_agent.py # UeTracesAgent — IMSI trace correlation
│   ├── evidence_quality.py # EvidenceQuality gate — grades evidence before RCA
│   ├── rca_agent.py      # RCAAgent — ONLY agent that calls LLM
│   └── dag_mapper.py     # Maps 3GPP procedure DAGs from Memgraph
├── api/
│   └── webhook.py        # FastAPI webhook (POST /webhook) — Alertmanager entry point
├── mcp/
│   └── client.py         # MCP client for Prometheus + Loki connections
└── memgraph/
    └── connection.py     # Memgraph Bolt connection (Neo4j driver)
```

### Tech Stack
- **Orchestration**: LangGraph (directed graph workflow)
- **Observability**: LangSmith (tracing, feedback)
- **Data Sources**: Prometheus (metrics), Loki (logs) via MCP protocol
- **Graph DB**: Memgraph (Bolt protocol on port 7687, Cypher queries) — stores 3GPP reference DAGs and IMSI traces
- **LLM**: Used only by RCAAgent for analysis. All other agents are deterministic.
- **API**: FastAPI webhook endpoint on port 8000

### Key Conventions

#### MANDATORY: Test-First Development
```bash
# 1. Write tests first
claude "Write pytest tests for xyz class... Don't implement yet."
# 2. Review tests, ensure they match your requirements
# 3. Generate implementation
claude "Implement xyz to pass these tests: [paste tests]"
# 4. Verify
pytest tests/unit/test_xyz_agent.py -v
mypy src/triage_agent/agents/xyz_agent.py --strict
ruff check src/triage_agent/agents/xyz_agent.py
# 5. Commit
```

#### State Management
- All agents read/write to a shared `TriageState` TypedDict (see `src/triage_agent/state.py`)
- Never modify state outside of agent functions
- Use LangGraph's `Send` for parallel execution

#### Database
- Memgraph, NOT Redis. Bolt protocol, port 7687, `mgconsole` CLI, Cypher queries.
- DAG definitions are Cypher scripts in `dags/` — loaded via init container
- Neo4j Python driver is used for Memgraph (compatible Bolt protocol)

#### Deployment
- Container configs are in `k8s/` — do not embed YAML in Python code
- Memgraph runs as sidecar container, not separate service
- Init container loads DAGs before main app starts

#### 5G Protocol
- The auth procedure is **5G AKA** (TS 33.501 Fig 6.1.3.2), NOT EAP-AKA'
- Reference DAGs from: TS 23.502 (procedures), TS 33.501 (security)
- NF names: AMF, SMF, UPF, NRF, AUSF, UDM, UDR, PCF, NSSF

#### Code Style
- Type hints required on all functions
- **Every agent function must have `@traceable` from langsmith** — without it the function is invisible in LangSmith traces and debugging becomes very hard
- Async functions preferred for MCP calls
- No LLM calls except in rca_agent.py

### Environment Variables

See `triage-agent.env.example` for the full template. Required before running locally:

```bash
LANGCHAIN_API_KEY=...          # LangSmith tracing
LANGCHAIN_TRACING_V2=true
GROQ_API_KEY=...               # LLM for RCAAgent (or equivalent)
PROMETHEUS_URL=http://prometheus:9090
LOKI_URL=http://loki:3100
MEMGRAPH_BOLT_URL=bolt://localhost:7687
```

### Running Tests
```bash
pytest tests/unit/ -v
pytest tests/integration/ --memgraph-url bolt://localhost:7687
pytest tests/e2e/ --alert-webhook http://localhost:8000/webhook
```

### Building
```bash
pip install -e ".[dev]"

# Lint + format
ruff check src/ tests/ && ruff format src/ tests/

# Type-check
mypy src/ --strict

# Run locally
uvicorn triage_agent.api.webhook:app --reload --port 8000

# Load DAGs into Memgraph
mgconsole < dags/registration_general.cypher
mgconsole < dags/authentication_5g_aka.cypher
mgconsole < dags/pdu_session_establishment.cypher
```

### Task Verification Commands
```bash
# Check Memgraph connectivity
mgconsole -host localhost -port 7687 <<< "MATCH (n) RETURN count(n);"

# Test Prometheus MCP
curl -s http://prometheus:9090/api/v1/query?query=up | jq '.data.result'

# Test Loki MCP
curl -s 'http://loki:3100/loki/api/v1/labels' | jq '.data'

# Run LangGraph workflow locally
python -c "from triage_agent.graph import create_workflow; print(create_workflow().get_graph().draw_ascii())"
```

### Custom Agents

Three domain-specific subagents in `.claude/agents/` — use these instead of general-purpose Claude for their specialisms:

| Agent | When to use |
|---|---|
| `5g-protocol-reviewer` | Reviewing any code that implements or references 3GPP procedures (AKA, PDU session, registration, handover) |
| `promql-builder` | Writing or debugging PromQL queries for Prometheus metrics |
| `memgraph-expert` | Writing Cypher queries, designing DAG schemas, or debugging Memgraph connectivity |

### Common Mistakes to Avoid
1. **Don't use Redis** — this project uses Memgraph for graph storage
2. **Don't add LLM calls to non-RCA agents** — only RCAAgent uses LLM
3. **Don't hardcode PromQL in agent functions** — use INFRA_PROMETHEUS_QUERIES constants
4. **Don't forget @traceable decorator** — required for LangSmith observability
5. **Don't use blocking I/O in async functions** — use httpx, not requests
6. **Don't skip tests** — always write tests first, then implement

---

## Do Not

Extends global Do Not. Project-specific hard rules:

| Rule | Reason |
|---|---|
| **No LLM calls outside `rca_agent.py`** | All other agents are deterministic by design; adding LLM calls breaks the separation and makes traces non-reproducible |
| **No Redis** — use Memgraph | Redis is for key-value caching; Memgraph stores 3GPP procedure DAGs as a graph — not interchangeable |
| **No hardcoded PromQL** in agent functions | Queries must live in constants so they can be tested and updated centrally |
| **No missing `@traceable` on agent functions** | LangSmith invisibility makes production debugging impossible |
| **No blocking I/O in async functions** | Use `httpx`, not `requests`; use `asyncio.to_thread()` for sync libraries |
| **No state mutation outside agent functions** | LangGraph state is passed through the graph; side-effects outside agent boundaries corrupt the workflow |
| **No K8s YAML embedded in Python** | Container configs belong in `k8s/` — not interpolated into application code |
