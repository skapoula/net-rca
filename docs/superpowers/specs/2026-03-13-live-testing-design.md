# Live Testing Design — 5G TriageAgent v3.2 against Free5GC on k3s

**Date:** 2026-03-13
**Scope:** End-to-end live verification of TriageAgent against the production Free5GC core network
**Approach:** Sequential Layer-by-Layer — each phase gates the next

---

## 1. Objective

Verify that the 5G TriageAgent v3.2 works as documented across three dimensions:

- **(a) Component validation** — each agent performs as designed against live 5G core data
- **(b) Integration validation** — each cross-component interface operates correctly
- **(c) End-to-end validation** — the full RCA pipeline produces accurate results for known failure scenarios

---

## 2. Cluster Environment

| Resource | Value |
|---|---|
| 5G Core software | Free5GC |
| 5G Core namespace | `5g-core` |
| Prometheus service | `kube-prom-kube-prometheus-prometheus.monitoring:9090` |
| Loki service | `loki.monitoring:3100` |
| LLM inference | `http://qwen3-4b.ml-serving.svc.cluster.local/v1` |
| LLM model | `qwen3-4b-instruct-2507.Q4_K_M.gguf` |
| TriageAgent deploy namespace | `monitoring` |
| Artifacts path (in-pod) | `/app/artifacts/<incident_id>/` (assumes default `ARTIFACTS_DIR=artifacts`, container workdir `/app`) |

### Free5GC NF Pods (`5g-core` namespace)

| NF | Pod | Deployment |
|---|---|---|
| AMF | `amf-8f76f76b9-dqqhx` | `amf` |
| AUSF | `ausf-557b995b7-xt64m` | `ausf` |
| SMF | `smf-854f858746-m6jgq` | `smf` |
| UDM | `udm-58cc5c7d7c-q64rq` | `udm` |
| UDR | `udr-7456945f49-hnsnw` | `udr` |
| UPF | `upf-9f5hz` | DaemonSet `upf` |
| NRF | `nrf-599b56f964-7spm5` | `nrf` |
| NSSF | `nssf-5bb644c4d7-92xfr` | `nssf` |
| PCF | `pcf-f9775c644-c8vff` | `pcf` |

### UERANSIM (`5g-core` namespace)

| Resource | Value |
|---|---|
| Deployment | `ueransim` |
| Pod | `ueransim-7f8cd9c6c4-ljztf` |
| Containers | `gnb` + `ue-1` through `ue-10` (11 total) |
| UE ConfigMap | `ue-config` |
| gNB ConfigMap | `gnb-config` |
| IMSIs | `imsi-208930000000001` – `imsi-208930000000010` |
| Current operator key (`op`) | `8e27b6af0e692e750f32667a3b14605d` (`opType: OPC`) |
| Current APN | `internet` |

### Reference DAG names (as stored in Memgraph)

| File | Memgraph `ReferenceTrace.name` |
|---|---|
| `dags/registration_general.cypher` | `Registration_General` |
| `dags/authentication_5g_aka.cypher` | `Authentication_5G_AKA` |
| `dags/pdu_session_establishment.cypher` | `PDU_Session_Establishment` |

---

## 3. Phase Overview

```
Phase 0    → Pre-flight checks (cluster readiness gate)
Phase 0.5  → Build, deploy, and configure TriageAgent in monitoring namespace
Phase 1    → Health verification (TriageAgent + Memgraph + DAGs)
Phase 2    → Component validation (a): each agent performs as designed
Phase 3    → Integration validation (b): cross-component interfaces
Phase 4    → End-to-end validation (c): 4 scenarios, accurate RCA
```

Each phase has explicit pass criteria. Do not proceed to the next phase until all criteria are met.

---

## 4. Phase 0 — Pre-flight Checks

Verify the cluster is fully ready before any deployment or testing.

```bash
# Free5GC pods all Running
kubectl get pods -n 5g-core

# UERANSIM Running with 11/11 containers
kubectl get pods -n 5g-core | grep ueransim

# Prometheus is scraping the 5G core
curl -s 'http://kube-prom-kube-prometheus-prometheus.monitoring:9090/api/v1/query?query=up{namespace="5g-core"}' \
  | jq '.data.result | length'
# → must be > 0

# Loki has live Free5GC logs
curl -s --data-urlencode 'query={namespace="5g-core"}' \
  'http://loki.monitoring:3100/loki/api/v1/query_range?limit=5' \
  | jq '.data.result | length'
# → must be > 0

# LLM inference service is responding
curl -s http://qwen3-4b.ml-serving.svc.cluster.local/v1/models | jq '.data[0].id'
# → must return model name
```

**Pass criteria:**
- All Free5GC NF pods `Running 2/2`
- UERANSIM pod `Running 11/11`
- Prometheus returns ≥ 1 scrape result for `up{namespace="5g-core"}`
- Loki returns ≥ 1 log stream for `{namespace="5g-core"}`
- LLM inference service responds with model ID

---

## 5. Phase 0.5 — Build, Deploy & Configure

### 5.1 Build and import container image

```bash
cd /workspace/net-rca

docker build -t triage-agent:v3.2 .
docker save triage-agent:v3.2 -o /tmp/triage-agent-v3.2.tar
sudo k3s ctr images import /tmp/triage-agent-v3.2.tar

# Verify image available in k3s
sudo k3s ctr images ls | grep triage-agent
```

### 5.2 Configure and deploy to `monitoring` namespace

Edit `k8s/triage-agent-configmap.yaml` — only non-default overrides needed
(Prometheus and Loki URLs are already correct defaults in `config.py`):

```yaml
# Set in triage-agent-configmap.yaml:
LLM_PROVIDER: local
LLM_BASE_URL: http://qwen3-4b.ml-serving.svc.cluster.local/v1
LLM_MODEL: qwen3-4b-instruct-2507.Q4_K_M.gguf
CORE_NAMESPACE: 5g-core
```

Apply manifests. Note: `dag-configmap.yaml` and `memgraph-pvc.yaml` have `namespace: 5g-monitoring`
hardcoded — patch to `monitoring` before applying:

```bash
# Patch namespace in two manifests that hardcode 5g-monitoring
sed 's/namespace: 5g-monitoring/namespace: monitoring/' k8s/dag-configmap.yaml \
  | kubectl apply -f -
sed 's/namespace: 5g-monitoring/namespace: monitoring/' k8s/memgraph-pvc.yaml \
  | kubectl apply -f -

# Remaining manifests respect -n flag
kubectl apply -n monitoring -f k8s/triage-agent-configmap.yaml
kubectl apply -n monitoring -f k8s/deployment-with-init.yaml
kubectl apply -n monitoring -f k8s/triage-agent-to-qwen3-4b-netpol.yaml

# Patch imagePullPolicy to Never (local image, no registry)
kubectl patch deployment triage-agent -n monitoring \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"triage-agent","imagePullPolicy":"Never"}]}}}}'
```

### 5.3 Port-forward and export variables

```bash
kubectl port-forward -n monitoring svc/triage-agent 8000:8000 &
export WEBHOOK_URL=http://localhost:8000
export TRIAGE_POD=$(kubectl get pod -n monitoring -l app=triage-agent \
  -o jsonpath='{.items[0].metadata.name}')
```

### 5.4 LangSmith tracing (optional but recommended for step 3.8)

```bash
# Add to triage-agent-configmap.yaml to enable LangSmith execution traces:
#   LANGCHAIN_TRACING_V2: "true"
#   LANGSMITH_API_KEY: "<your-key>"
# Without this, step 3.8 falls back to artifact timestamps for execution order verification.
```

### 5.5 Operational access — browse and pull artifacts

```bash
# Open bash shell in triage-agent container
kubectl exec -it -n monitoring $TRIAGE_POD -c triage-agent -- bash

# Inside: browse artifacts
ls /app/artifacts/
ls /app/artifacts/<incident_id>/
cat /app/artifacts/<incident_id>/post_filter_metrics.json

# Pull all artifacts to local machine
# (assumes default ARTIFACTS_DIR=artifacts, container workdir /app)
kubectl cp monitoring/$TRIAGE_POD:/app/artifacts ./artifacts-local -c triage-agent
```

### 5.6 Operational access — inject new DAGs (memgraph sidecar)

```bash
# Open bash in memgraph sidecar
kubectl exec -it -n monitoring $TRIAGE_POD -c memgraph -- bash

# Option 1: pipe local .cypher file directly
kubectl exec -i -n monitoring $TRIAGE_POD -c memgraph -- mgconsole \
  < /workspace/net-rca/dags/new_dag.cypher

# Option 2: copy file then execute
kubectl cp /workspace/net-rca/dags/new_dag.cypher \
  monitoring/$TRIAGE_POD:/tmp/new_dag.cypher -c memgraph
kubectl exec -n monitoring $TRIAGE_POD -c memgraph -- \
  bash -c "mgconsole < /tmp/new_dag.cypher"

# Verify all DAGs after injection
kubectl exec -n monitoring $TRIAGE_POD -c memgraph -- \
  bash -c 'echo "MATCH (t:ReferenceTrace) RETURN t.name;" | mgconsole'
```

---

## 6. Phase 1 — Health Verification

```bash
# All 3 containers in correct state
kubectl get pods -n monitoring | grep triage-agent
# → dag-loader: Completed, memgraph: Running, triage-agent: Running

# DAGs loaded — verify all 3 by exact name (PascalCase)
kubectl exec -n monitoring $TRIAGE_POD -c memgraph -- \
  bash -c 'echo "MATCH (t:ReferenceTrace) RETURN t.name;" | mgconsole'
# → Registration_General
# → Authentication_5G_AKA
# → PDU_Session_Establishment

# App health
curl http://localhost:8000/health
# → {"status": "healthy", "memgraph": true, "prometheus": true, "loki": true, "timestamp": "..."}

curl -o /dev/null -w "%{http_code}" http://localhost:8000/health/ready
# → 200
```

**Pass criteria:**
- `dag-loader` container status: `Completed`
- `memgraph` container status: `Running`
- `triage-agent` container status: `Running`
- All 3 DAGs present with exact names: `Registration_General`, `Authentication_5G_AKA`, `PDU_Session_Establishment`
- `GET /health` → `{"status": "healthy", "memgraph": true, "prometheus": true, "loki": true}`
- `GET /health/ready` → `200 OK`

---

## 7. Phase 2 — Component Validation

### Step 2.0 — Generate live 5G traffic (prerequisite for all component tests)

Restart UERANSIM to trigger fresh registration, authentication, and PDU session procedures for all 10 UEs:

```bash
kubectl rollout restart deployment ueransim -n 5g-core
kubectl rollout status deployment ueransim -n 5g-core

# Authoritative gate: confirm all 10 IMSIs appear in AMF log lines
# Wait ~30s for procedures to complete, then run:
sleep 30
for i in $(seq 1 10); do
  IMSI=$(printf "imsi-20893000000000%d" $i)
  COUNT=$(curl -s \
    --data-urlencode "query={namespace=\"5g-core\", pod=~\".*amf.*\"} |= \"$IMSI\"" \
    --data-urlencode "start=$(date -d '5 minutes ago' +%s)000000000" \
    --data-urlencode "end=$(date +%s)000000000" \
    --data-urlencode "limit=1" \
    'http://loki.monitoring:3100/loki/api/v1/query_range' \
    | jq '.data.result | length')
  echo "$IMSI: $COUNT log streams"
done
# → each IMSI should show ≥ 1 log stream
```

**Gate:** Do not proceed until all 10 IMSIs (`imsi-208930000000001` – `imsi-208930000000010`) appear in AMF Loki logs.

### Steps 2.1–2.7 — Component verification

All component tests use a single webhook POST. Trigger it once and inspect the resulting incident:

```bash
INCIDENT=$(curl -s -X POST $WEBHOOK_URL/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "receiver": "triage-agent",
    "status": "firing",
    "alerts": [{
      "status": "firing",
      "labels": {
        "alertname": "RegistrationFailures",
        "nf": "amf",
        "namespace": "5g-core",
        "severity": "warning"
      },
      "startsAt": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
    }]
  }' | jq -r '.incident_id')
echo "Incident: $INCIDENT"

# Poll until complete
watch -n 10 "curl -s $WEBHOOK_URL/incidents/$INCIDENT | jq '{status,root_nf,confidence,layer,evidence_quality_score}'"
```

**Component pass criteria:**

| Agent | Field to inspect | Pass criterion |
|---|---|---|
| **DagMapper** | `mapping_confidence`, `procedure_names` | `mapping_confidence = 1.0`; `procedure_names` includes `Registration_General` |
| **InfraAgent** | `infra_score` | Float 0.0–1.0; no exception |
| **NfMetricsAgent** | `post_filter_metrics.json` | Non-empty; data present for **all NFs in `nf_union`** |
| **NfLogsAgent** | `post_filter_logs.json` | Non-empty; entries for **all NFs in `nf_union`** |
| **UeTracesAgent** | `ue_traces` | Non-empty; ≥ 1 of the 10 IMSIs discovered and ingested into Memgraph |
| **EvidenceQualityAgent** | `evidence_quality_score` | ≥ 0.50 (metrics + logs both present) |
| **RCAAgent** | `final_report` | Contains all 5 fields (`root_nf`, `failure_mode`, `layer`, `confidence`, `evidence_chain`); no `llm_timeout` sentinel |

Artifacts auto-saved per incident at `/app/artifacts/<incident_id>/`:
- `pre_filter_metrics.json` — raw PromQL results
- `post_filter_metrics.json` — compressed metrics fed to RCA
- `pre_filter_logs.json` — raw Loki log entries
- `post_filter_logs.json` — compressed + annotated logs

---

## 8. Phase 3 — Integration Validation

For every step, inputs and outputs are recorded via the agents' built-in `save_artifact()` calls.
Token counts are recorded against the configured budget ceilings.

### 3.1 — NfMetricsAgent ↔ Prometheus (direct HTTP)

- Verify 4 PromQL queries per NF (error rate, p95 latency, CPU, memory) return results for **all NFs in `nf_union`**
- **Pass:** `post_filter_metrics.json` contains entries for every NF in `nf_union`; no NF missing
- **Token counter:** record token count of `post_filter_metrics.json`; compare against budget ceiling of **500 tokens**
- **Artifacts:** `pre_filter_metrics.json` (raw PromQL results), `post_filter_metrics.json` (compressed)

### 3.2 — NfLogsAgent ↔ Loki (two-path: MCPClient wrapper or direct HTTP fallback)

- Verify LogQL queries return log lines for **all NFs in `nf_union`**
- Confirm which path was used: check agent logs for `"MCP server unavailable, using direct Loki connection"` or its absence
- **Pass:** `post_filter_logs.json` contains entries for every NF in `nf_union`; correct path logged
- **Token counter:** record token count of `post_filter_logs.json`; compare against budget ceiling of **1,300 tokens**
- **Artifacts:** `pre_filter_logs.json` (raw log entries), `post_filter_logs.json` (compressed + annotated)

### 3.3 — UeTracesAgent ↔ Memgraph (write path)

- Verify captured traces ingested for all discovered IMSIs from UERANSIM traffic
- After incident completes:
  ```bash
  kubectl exec -n monitoring $TRIAGE_POD -c memgraph -- \
    bash -c 'echo "MATCH (t:CapturedTrace) RETURN count(t);" | mgconsole'
  # → must be > 0
  ```
- **Pass:** CapturedTrace count > 0; at least one trace per discovered IMSI
- **Token counter:** record token count of `trace_deviations` output; compare against budget ceiling of **500 tokens**
- **Artifacts:** IMSI discovery list (input), Memgraph `CapturedTrace` count confirmation (output)

### 3.4 — UeTracesAgent ↔ Memgraph (deviation detection)

- Verify deviation detection Cypher runs against all 3 loaded reference DAGs
- **Pass:** `trace_deviations` field present and structured; no Cypher query errors in logs
- **Artifacts:** deviation result per DAG (`Registration_General`, `Authentication_5G_AKA`, `PDU_Session_Establishment`)

### 3.5 — InfraAgent contribution to RCAAgent input

- **Token counter:** record token count of infra evidence block in compressed evidence; compare against budget ceiling of **400 tokens**
- **Artifacts:** infra evidence block as it appears in the `join_for_rca` compressed evidence output

### 3.6 — DagMapper contribution to RCAAgent input

- **Token counter:** record token count of DAG evidence block in compressed evidence; compare against budget ceiling of **800 tokens**
- **Artifacts:** DAG evidence block as it appears in the `join_for_rca` compressed evidence output

### 3.7 — RCAAgent ↔ LLM (qwen3-4b)

- **Pass:** All 5 fields present; `confidence` is float; no `llm_timeout` sentinel; response within 300s
- **Token counters:**
  - Total prompt tokens sent to LLM (sum of all agent contributions + system prompt ~400 tokens)
  - LLM response tokens
  - Total tokens (prompt + response)
  - If retry triggered: token cost of retry call recorded separately
- **Artifacts:** full compressed evidence prompt (input), raw LLM JSON response (output)

### 3.8 — LangGraph graph execution order

- **Pass:** All 8 main-path nodes appear in correct dependency order (exact names from `graph.py`):
  `infra_agent` → `dag_mapper` → (`metrics_agent` + `logs_agent` + `traces_agent`) → `evidence_quality` → `join_for_rca` → `rca_agent`; no skipped nodes
  (`increment_attempt` and `finalize` nodes execute only on retry/finalize paths — excluded from this count)
- **Verification:** via LangSmith trace (if `LANGCHAIN_TRACING_V2=true`) or by comparing `mtime` of per-agent artifact files under `/app/artifacts/<incident_id>/`

### 3.9 — Webhook → incident lifecycle

- `POST /webhook` → `200` with `incident_id`
- Immediate poll → `{"status": "pending"}`
- Final poll → `{"status": "complete", "final_report": {...}}`
- **Pass:** Full lifecycle completes within 360s; no 500 errors or hung incidents

---

## 9. Phase 4 — End-to-End Validation

### Pre-scenario setup

```bash
# Confirm variables are set
echo $TRIAGE_POD
echo $WEBHOOK_URL

# Helper: trigger webhook with configurable severity (default: critical)
trigger_webhook() {
  local alertname=$1
  local nf=$2
  local severity=${3:-critical}
  curl -s -X POST $WEBHOOK_URL/webhook \
    -H "Content-Type: application/json" \
    -d '{
      "receiver": "triage-agent",
      "status": "firing",
      "alerts": [{
        "status": "firing",
        "labels": {
          "alertname": "'"$alertname"'",
          "nf": "'"$nf"'",
          "namespace": "5g-core",
          "severity": "'"$severity"'"
        },
        "startsAt": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
      }]
    }' | jq -r '.incident_id'
}

# Helper: poll until complete
poll_incident() {
  local id=$1
  watch -n 10 "curl -s $WEBHOOK_URL/incidents/$id | \
    jq '{status,root_nf,failure_mode,layer,confidence,evidence_quality_score}'"
}
```

---

### Scenario 4.1 — Sunny Day (no failure, healthy baseline)

**Purpose:** Establish a healthy baseline with no injected failures. Verify the full pipeline completes without false positives. Record baseline token counts for comparison with failure scenarios.

**Setup:** UERANSIM already running with 10 registered UEs from Phase 2 Step 2.0. No changes.

**Trigger:**
```bash
# Use "warning" severity — realistic for a healthy network with low-level alert
INCIDENT=$(trigger_webhook "RegistrationFailures" "amf" "warning")
echo "Incident: $INCIDENT"
poll_incident $INCIDENT
```

**Pass criteria:**
- Pipeline completes without errors or exceptions
- All agents return data: `nf_metrics`, `nf_logs`, `ue_traces` all non-empty
- `infra_score < 0.3` (all NF pods healthy, no restarts)
- No significant trace deviations against `Registration_General` DAG
- LLM returns valid JSON with all 5 fields; no `llm_timeout`
- All 10 IMSIs (`imsi-208930000000001`–`imsi-208930000000010`) discoverable in trace data
- **Baseline token counts recorded** per agent for comparison with failure scenarios

**Restore:** Nothing to restore.

---

### Scenario 4.2 — Registration Failure (AMF scaled to 0)

**Failure type:** Infrastructure — sustained AMF outage

**Inject failure:**
```bash
# Record current replica count
kubectl get deployment amf -n 5g-core -o jsonpath='{.spec.replicas}'
# → expected: 1

# Scale AMF to 0
kubectl scale deployment amf -n 5g-core --replicas=0

# Confirm pod is terminating
kubectl get pods -n 5g-core | grep amf

# Wait for Prometheus to scrape the outage (~30s)
sleep 30

# Restart UERANSIM to trigger registration attempts against unavailable AMF
kubectl rollout restart deployment ueransim -n 5g-core
kubectl rollout status deployment ueransim -n 5g-core
sleep 15
```

**Trigger:**
```bash
INCIDENT=$(trigger_webhook "RegistrationFailures" "amf")
echo "Incident: $INCIDENT"
poll_incident $INCIDENT
```

**Pass criteria:**
- `root_nf = AMF`
- `layer = infrastructure`
- `failure_mode` references pod unavailability or service outage
- `confidence ≥ 0.70`
- `infra_score > 0.5`
- `evidence_chain` references `Registration_General` DAG deviation
- Token counts higher than 4.1 baseline (failure generates more log/metric signal)
- Note: IMSI discovery expected to be zero or partial (AMF unavailable, UEs cannot register)

**Restore:**
```bash
kubectl scale deployment amf -n 5g-core --replicas=1
kubectl rollout status deployment amf -n 5g-core

# Wait for AMF to complete NRF registration before restarting UERANSIM
# (rollout status confirms pod Ready but NRF registration takes additional time)
sleep 15

kubectl rollout restart deployment ueransim -n 5g-core
kubectl rollout status deployment ueransim -n 5g-core

# Confirm all 10 UEs re-registered before next scenario
sleep 30
for i in $(seq 1 10); do
  IMSI=$(printf "imsi-20893000000000%d" $i)
  COUNT=$(curl -s \
    --data-urlencode "query={namespace=\"5g-core\", pod=~\".*amf.*\"} |= \"$IMSI\"" \
    --data-urlencode "start=$(date -d '2 minutes ago' +%s)000000000" \
    --data-urlencode "end=$(date +%s)000000000" \
    --data-urlencode "limit=1" \
    'http://loki.monitoring:3100/loki/api/v1/query_range' \
    | jq '.data.result | length')
  echo "$IMSI: $COUNT"
done
```

---

### Scenario 4.3 — Authentication Failure (wrong operator key)

**Failure type:** Application — 5G AKA authentication vector mismatch at AUSF/UDM

**Inject failure:**
```bash
# Backup current ue-config
kubectl get configmap ue-config -n 5g-core -o yaml > ue-config-backup.yaml

# Patch op key to zeroed-out invalid value
# AUSF will compute HXRES* that does not match UE's RES* → auth rejection for all 10 UEs
# Note: field name is 'op' (operator key) with opType: OPC — confirmed from deployed configmap
kubectl get configmap ue-config -n 5g-core -o yaml \
  | sed "s/op: '8e27b6af0e692e750f32667a3b14605d'/op: '00000000000000000000000000000000'/" \
  | kubectl apply -f -

# Verify patch applied
kubectl get configmap ue-config -n 5g-core -o jsonpath='{.data.ue-base\.yaml}' | grep "^op:"
# → op: '00000000000000000000000000000000'

# Restart UERANSIM — all 10 UEs will fail 5G AKA authentication
kubectl rollout restart deployment ueransim -n 5g-core
kubectl rollout status deployment ueransim -n 5g-core
sleep 30
```

**Trigger:**
```bash
INCIDENT=$(trigger_webhook "AuthenticationFailures" "ausf")
echo "Incident: $INCIDENT"
poll_incident $INCIDENT
```

**Pass criteria:**
- `root_nf` ∈ `{AUSF, UDM}`
- `layer = application` (no pod restarts — `infra_score < 0.3`)
- `failure_mode` references authentication vector mismatch or RES* failure
- `confidence ≥ 0.70`
- `evidence_chain` references `Authentication_5G_AKA` DAG deviation (RES*/HXRES* mismatch phase)
- All 10 IMSI traces show authentication failure deviation in Memgraph

**Restore:**
```bash
kubectl apply -f ue-config-backup.yaml

# Verify op key restored
kubectl get configmap ue-config -n 5g-core -o jsonpath='{.data.ue-base\.yaml}' | grep "^op:"
# → op: '8e27b6af0e692e750f32667a3b14605d'

kubectl rollout restart deployment ueransim -n 5g-core
kubectl rollout status deployment ueransim -n 5g-core

# Confirm all 10 UEs re-registered with correct OPC before proceeding to 4.4
sleep 30
for i in $(seq 1 10); do
  IMSI=$(printf "imsi-20893000000000%d" $i)
  COUNT=$(curl -s \
    --data-urlencode "query={namespace=\"5g-core\", pod=~\".*amf.*\"} |= \"$IMSI\"" \
    --data-urlencode "start=$(date -d '2 minutes ago' +%s)000000000" \
    --data-urlencode "end=$(date +%s)000000000" \
    --data-urlencode "limit=1" \
    'http://loki.monitoring:3100/loki/api/v1/query_range' \
    | jq '.data.result | length')
  echo "$IMSI: $COUNT"
done
```

---

### Scenario 4.4 — PDU Session Failure (unsupported APN)

**Failure type:** Application — DNN not provisioned in SMF; registration and authentication succeed

**Inject failure:**
```bash
# Backup current ue-config (reuse ue-config-backup.yaml if already saved from 4.3)
kubectl get configmap ue-config -n 5g-core -o yaml > ue-config-backup.yaml

# Patch APN to an unprovisioned value
# Note: field name is 'apn' — confirmed from deployed configmap (UERANSIM v4.2.0)
kubectl get configmap ue-config -n 5g-core -o yaml \
  | sed "s/apn: 'internet'/apn: 'invalid-internet'/" \
  | kubectl apply -f -

# Verify patch applied
kubectl get configmap ue-config -n 5g-core -o jsonpath='{.data.ue-base\.yaml}' | grep "apn:"
# → apn: 'invalid-internet'

# Restart UERANSIM — UEs register and authenticate successfully but PDU session setup fails
kubectl rollout restart deployment ueransim -n 5g-core
kubectl rollout status deployment ueransim -n 5g-core
sleep 30
```

**Trigger:**
```bash
INCIDENT=$(trigger_webhook "PDUSessionFailures" "smf")
echo "Incident: $INCIDENT"
poll_incident $INCIDENT
```

**Pass criteria:**
- `root_nf = SMF`
- `layer = application` (`infra_score < 0.3` — registration and auth succeed)
- `failure_mode` references DNN not supported or APN rejection
- `confidence ≥ 0.70`
- `evidence_chain` references `PDU_Session_Establishment` DAG deviation

**Restore:**
```bash
kubectl apply -f ue-config-backup.yaml

# Verify APN restored
kubectl get configmap ue-config -n 5g-core -o jsonpath='{.data.ue-base\.yaml}' | grep "apn:"
# → apn: 'internet'

kubectl rollout restart deployment ueransim -n 5g-core
kubectl rollout status deployment ueransim -n 5g-core
```

---

## 10. Overall Pass Criteria

| Metric | Target |
|---|---|
| **Accuracy** | `root_nf` correct in ≥ 2 of 3 failure scenarios |
| **Layer detection** | `infrastructure` vs `application` correct in all 3 failure scenarios |
| **Confidence** | ≥ 0.70 in all failure scenarios |
| **Sunny day** | No false positive; `infra_score < 0.3`; pipeline completes cleanly |
| **IMSI coverage** | All 10 IMSIs discoverable in Scenarios 4.1, 4.3, 4.4; partial/zero expected in 4.2 (AMF unavailable) |
| **Latency** | Full pipeline completes in < 360s per scenario |
| **Evidence quality** | `evidence_quality_score ≥ 0.50` in all 4 scenarios |
| **Token budgets** | All agent contributions within ceilings; delta vs 4.1 baseline noted |
| **Artifacts** | All per-agent `pre_filter_*` and `post_filter_*` files saved for all 4 scenarios |
| **Restore** | Cluster returned to healthy state after each scenario; all 10 UEs re-registered before next scenario |

---

## 11. Token Budget Reference

| Section | Budget ceiling | Notes |
|---|---|---|
| InfraAgent evidence | 400 tokens | infra evidence block |
| DagMapper evidence | 800 tokens | DAG evidence block |
| NfMetricsAgent | 500 tokens | `post_filter_metrics.json` |
| NfLogsAgent | 1,300 tokens | `post_filter_logs.json` |
| UeTracesAgent | 500 tokens | `trace_deviations` |
| System prompt / RCA template | ~400 tokens | fixed overhead |
| **Total LLM context** | **~3,900 tokens** | evidence + prompt template |

---

## 12. Evidence Quality Score Reference

| Score | Sources present |
|---|---|
| 0.95 | metrics + logs + traces |
| 0.85 | traces + one other source |
| 0.80 | metrics + logs (no traces) |
| 0.50 | traces only |
| 0.40 | metrics only |
| 0.35 | logs only |
| 0.10 | no evidence (sentinel) |
