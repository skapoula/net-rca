# 5G TriageAgent

**5G TriageAgent** is a multi-agent LangGraph orchestration system for real-time root cause
analysis of 5G core network failures. When Prometheus Alertmanager fires an alert
(e.g. `RegistrationFailures`), the system coordinates five specialised agents through a
directed pipeline to localise failures across infrastructure, network function, and 3GPP
procedure layers — producing a structured root cause report in seconds.

```{toctree}
:maxdepth: 2
:caption: Guides

developer-guide
agent-development
configuration-reference
memgraph-sidecar-guide
```

```{toctree}
:maxdepth: 1
:caption: Reference

triageagent_architecture_design2
autoapi/index
```

---

## At a Glance

| Property      | Value                           |
| ------------- | ------------------------------- |
| Version       | 3.2.0                           |
| Python        | 3.11+                           |
| Orchestration | LangGraph                       |
| Graph DB      | Memgraph (Bolt, port 7687)      |
| Observability | LangSmith                       |
| LLM agents    | 1 of 7 (RCAAgent only)          |
| Entry point   | FastAPI webhook `POST /webhook` |

## Pipeline

```{mermaid}
flowchart TD
    A["Alertmanager"] -->|webhook| B["LangGraph Orchestrator"]
    B --> C(["START"])

    C --> D["InfraAgent\nCheck pod metrics via MCP"]
    C --> DM["DagMapper\nAlert → 3GPP procedure DAGs"]

    DM --> E["NfMetricsAgent\nMCP: Prometheus"]
    DM --> E2["NfLogsAgent\nMCP: Loki"]
    DM --> E3["UeTracesAgent\nMCP: Loki + Memgraph"]

    E --> F["EvidenceQuality"]
    E2 --> F
    E3 --> F

    D --> JR
    F --> JR["join_for_rca\n(compress evidence)"]
    JR --> G["RCAAgent\nLLM analysis"]

    G --> RETRY{"should_retry?"}
    RETRY -->|retry| INC["increment_attempt"]
    INC --> G
    RETRY -->|finalize| H["finalize_report"]
    H --> I(["END"])
```
