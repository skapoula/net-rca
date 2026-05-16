# Live Testing from Pod — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the live test plan for 5G TriageAgent v3.2 against Free5GC on k3s, verifying component behaviour, integration correctness, and end-to-end RCA accuracy across 4 scenarios — running TriageAgent **locally inside this devcontainer** (no new pod spin-up).

**Difference from `2026-03-13-live-testing.md`:** Phase 0.5 no longer builds/imports/deploys a Docker image to k3s. Instead, TriageAgent is started with `uvicorn` directly in this pod. All artifact access uses local filesystem paths instead of `kubectl exec` / `kubectl cp`. Memgraph queries use a local `mgconsole` connection. The k3s cluster is still used for Free5GC, UERANSIM, Prometheus, and Loki — only the TriageAgent process moves local.

**Architecture:** Test scripts are created first and used to drive each phase. Each script is self-contained, idempotent where possible, and captures results to a timestamped `test-results/` directory. All phases gate on explicit pass criteria before proceeding.

**Tech Stack:** bash, kubectl, curl, jq, uvicorn, mgconsole, k3s, Free5GC, UERANSIM, TriageAgent v3.2 (local), Memgraph (local bolt://localhost:7687), Prometheus, Loki, qwen3-4b LLM

---

## File Map

### Created
| File | Responsibility |
|---|---|
| `scripts/testing/helpers.sh` | Shared functions: `trigger_webhook`, `poll_incident`, `check_imsi_loki`, `collect_token_count`, `check_local_agent` |
| `scripts/testing/phase0-preflight.sh` | Phase 0: cluster + local-process readiness checks |
| `scripts/testing/phase05-start-local.sh` | Phase 0.5: load DAGs into local Memgraph + start uvicorn |
| `scripts/testing/phase1-health.sh` | Phase 1: TriageAgent process health and DAG verification (local) |
| `scripts/testing/phase2-components.sh` | Phase 2: UERANSIM traffic generation + per-agent component checks |
| `scripts/testing/phase3-integration.sh` | Phase 3: integration boundary checks + token counting |
| `scripts/testing/phase4-e2e.sh` | Phase 4: all 4 E2E scenarios with inject/trigger/verify/restore |
| `scripts/testing/run-all.sh` | Orchestrator: run all phases in order, gate on pass |

### Modified
| File | Change |
|---|---|
| `k8s/dag-configmap.yaml` | Fix hardcoded `namespace: 5g-monitoring` → `monitoring` |
| `k8s/memgraph-pvc.yaml` | Fix hardcoded `namespace: 5g-monitoring` → `monitoring` |

---

## Chunk 1: Test Infrastructure

### Task 1: Fix manifest namespace in dag-configmap.yaml and memgraph-pvc.yaml

**Files:**
- Modify: `k8s/dag-configmap.yaml`
- Modify: `k8s/memgraph-pvc.yaml`

- [ ] **Step 1.1: Verify current namespace values**

```bash
grep "namespace:" /workspace/net-rca/k8s/dag-configmap.yaml
grep "namespace:" /workspace/net-rca/k8s/memgraph-pvc.yaml
```
Expected output: `namespace: 5g-monitoring` in both files.

- [ ] **Step 1.2: Fix dag-configmap.yaml**

```bash
sed -i 's/namespace: 5g-monitoring/namespace: monitoring/' \
  /workspace/net-rca/k8s/dag-configmap.yaml
```

- [ ] **Step 1.3: Fix memgraph-pvc.yaml**

```bash
sed -i 's/namespace: 5g-monitoring/namespace: monitoring/' \
  /workspace/net-rca/k8s/memgraph-pvc.yaml
```

- [ ] **Step 1.4: Verify both files now say monitoring**

```bash
grep "namespace:" /workspace/net-rca/k8s/dag-configmap.yaml
grep "namespace:" /workspace/net-rca/k8s/memgraph-pvc.yaml
```
Expected: `namespace: monitoring` in both.

- [ ] **Step 1.5: Commit**

```bash
cd /workspace/net-rca
git add k8s/dag-configmap.yaml k8s/memgraph-pvc.yaml
git commit -m "fix(k8s): correct hardcoded namespace 5g-monitoring -> monitoring"
```

---

### Task 2: Write shared test helpers

**Files:**
- Create: `scripts/testing/helpers.sh`

**Key changes from original plan:**
- `resolve_triage_pod()` removed — no k8s pod to resolve.
- `check_local_agent()` added — verifies uvicorn is running on port 8000.
- `ARTIFACTS_DIR` added — local path where TriageAgent writes artifacts.
- `collect_token_count()` reads from local `ARTIFACTS_DIR` instead of `kubectl exec`.
- `pull_artifacts()` is a no-op copy from local source to results dir.
- Memgraph accessed via `mgconsole -host localhost -port 7687`.
- `TRIAGE_POD` / `TRIAGE_NS` variables removed.

- [ ] **Step 2.1: Create helpers.sh**

```bash
cat > /workspace/net-rca/scripts/testing/helpers.sh << 'HELPERS'
#!/usr/bin/env bash
# helpers.sh — shared functions for all live test scripts (local-pod variant)
set -euo pipefail

# ── Environment ──────────────────────────────────────────────────────────────
export WEBHOOK_URL="${WEBHOOK_URL:-http://localhost:8000}"
export ARTIFACTS_DIR="${ARTIFACTS_DIR:-/workspace/net-rca/artifacts}"
export RESULTS_DIR="${RESULTS_DIR:-/workspace/net-rca/test-results/$(date +%Y%m%d-%H%M%S)}"
export PROMETHEUS_URL="${PROMETHEUS_URL:-http://kube-prom-kube-prometheus-prometheus.monitoring:9090}"
export LOKI_URL="${LOKI_URL:-http://loki.monitoring:3100}"
export MEMGRAPH_HOST="${MEMGRAPH_HOST:-localhost}"
export MEMGRAPH_PORT="${MEMGRAPH_PORT:-7687}"
export CORE_NS="5g-core"
export TRIAGE_LOG="${TRIAGE_LOG:-/tmp/triage-agent.log}"

mkdir -p "$RESULTS_DIR"

# ── Logging ───────────────────────────────────────────────────────────────────
log()  { echo "[$(date +%H:%M:%S)] $*"; }
pass() { echo "[PASS] $*" | tee -a "$RESULTS_DIR/summary.txt"; }
fail() { echo "[FAIL] $*" | tee -a "$RESULTS_DIR/summary.txt"; }
info() { echo "[INFO] $*"; }

# ── Verify local TriageAgent is running ───────────────────────────────────────
check_local_agent() {
  if ! curl -s --max-time 3 "$WEBHOOK_URL/health" > /dev/null 2>&1; then
    fail "TriageAgent not reachable at $WEBHOOK_URL — run phase05-start-local.sh first"
    exit 1
  fi
  log "TriageAgent reachable at $WEBHOOK_URL"
}

# ── Webhook trigger ───────────────────────────────────────────────────────────
# Usage: trigger_webhook <alertname> <nf> [severity=critical]
# Returns: incident_id
trigger_webhook() {
  local alertname=$1
  local nf=$2
  local severity=${3:-critical}
  local starts_at
  starts_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  curl -s -X POST "$WEBHOOK_URL/webhook" \
    -H "Content-Type: application/json" \
    -d "{
      \"receiver\": \"triage-agent\",
      \"status\": \"firing\",
      \"alerts\": [{
        \"status\": \"firing\",
        \"labels\": {
          \"alertname\": \"$alertname\",
          \"nf\": \"$nf\",
          \"namespace\": \"$CORE_NS\",
          \"severity\": \"$severity\"
        },
        \"startsAt\": \"$starts_at\"
      }]
    }" | jq -r '.incident_id'
}

# ── Poll incident until complete ──────────────────────────────────────────────
# Usage: poll_incident <incident_id> [timeout_seconds=360]
# Returns: final_report JSON (also saved to RESULTS_DIR)
poll_incident() {
  local incident_id=$1
  local timeout=${2:-360}
  local elapsed=0
  local result

  log "Polling incident $incident_id (timeout ${timeout}s)..."
  while [[ $elapsed -lt $timeout ]]; do
    result=$(curl -s "$WEBHOOK_URL/incidents/$incident_id")
    local status
    status=$(echo "$result" | jq -r '.status // "unknown"')

    if [[ "$status" == "complete" ]]; then
      echo "$result" | jq . | tee "$RESULTS_DIR/${incident_id}.json"
      log "Incident $incident_id complete"
      echo "$result"
      return 0
    fi

    log "Status: $status (${elapsed}s elapsed)"
    sleep 10
    elapsed=$((elapsed + 10))
  done

  fail "Incident $incident_id did not complete within ${timeout}s"
  return 1
}

# ── Per-IMSI Loki check ───────────────────────────────────────────────────────
# Usage: check_imsi_loki [lookback_minutes=5]
check_imsi_loki() {
  local lookback_min=${1:-5}
  local start end all_found=true

  start=$(date -d "${lookback_min} minutes ago" +%s)000000000
  end=$(date +%s)000000000

  for i in $(seq 1 10); do
    local imsi
    imsi=$(printf "imsi-20893000000000%d" "$i")
    local count
    count=$(curl -s \
      --data-urlencode "query={namespace=\"$CORE_NS\", pod=~\".*amf.*\"} |= \"$imsi\"" \
      --data-urlencode "start=$start" \
      --data-urlencode "end=$end" \
      --data-urlencode "limit=1" \
      "$LOKI_URL/loki/api/v1/query_range" \
      | jq '.data.result | length')
    echo "  $imsi: $count stream(s)"
    [[ "$count" -gt 0 ]] || all_found=false
  done

  $all_found && return 0 || return 1
}

# ── Token counter (local filesystem) ─────────────────────────────────────────
# Usage: collect_token_count <incident_id> <artifact_filename>
collect_token_count() {
  local incident_id=$1
  local artifact=$2
  local artifact_path="$ARTIFACTS_DIR/$incident_id/$artifact"

  if [[ ! -f "$artifact_path" ]]; then
    echo "  $artifact: NOT FOUND (checked $artifact_path)"
    return 1
  fi

  local chars token_est
  chars=$(wc -c < "$artifact_path")
  token_est=$((chars / 4))
  echo "  $artifact: ~${token_est} tokens (${chars} chars)"
  echo "${incident_id}|${artifact}|${token_est}" >> "$RESULTS_DIR/token_counts.txt"
}

# ── Copy local artifacts to results dir ──────────────────────────────────────
pull_artifacts() {
  local incident_id=$1
  local src="$ARTIFACTS_DIR/$incident_id"
  local dest="$RESULTS_DIR/artifacts/$incident_id"

  mkdir -p "$dest"
  if [[ -d "$src" ]]; then
    cp -r "$src/." "$dest/"
    log "Artifacts copied: $src → $dest"
  else
    log "Warning: no artifact directory found at $src"
  fi
}

# ── Memgraph query helper ─────────────────────────────────────────────────────
# Usage: mgquery <cypher_query>
# Returns: mgconsole output
mgquery() {
  local query="$1"
  echo "$query" | mgconsole -host "$MEMGRAPH_HOST" -port "$MEMGRAPH_PORT" 2>/dev/null
}

HELPERS
chmod +x /workspace/net-rca/scripts/testing/helpers.sh
```

- [ ] **Step 2.2: Verify helpers.sh has no syntax errors**

```bash
bash -n /workspace/net-rca/scripts/testing/helpers.sh && echo "Syntax OK"
```
Expected: `Syntax OK`

- [ ] **Step 2.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/helpers.sh
git commit -m "test(scripts): add shared live test helper functions (local-pod variant)"
```

---

### Task 3: Write Phase 0 pre-flight script

**Files:**
- Create: `scripts/testing/phase0-preflight.sh`

**Key changes from original:** Removes triage-agent pod checks (not in cluster). Keeps Free5GC, UERANSIM, Prometheus, Loki, and LLM checks. Adds a check that Memgraph is reachable locally on bolt port 7687.

- [ ] **Step 3.1: Create phase0-preflight.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase0-preflight.sh << 'PHASE0'
#!/usr/bin/env bash
# phase0-preflight.sh — verify cluster + local Memgraph are ready before testing
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

log "=== Phase 0: Pre-flight Checks ==="
ERRORS=0

# 1. Free5GC NF pods all Running
log "Checking Free5GC NF pods..."
kubectl get pods -n "$CORE_NS" | tee "$RESULTS_DIR/phase0-pods.txt"
NOT_RUNNING=$(kubectl get pods -n "$CORE_NS" \
  --field-selector=status.phase!=Running \
  --no-headers 2>/dev/null | grep -v "Completed" | wc -l)
if [[ "$NOT_RUNNING" -eq 0 ]]; then
  pass "All Free5GC pods Running"
else
  fail "Some Free5GC pods not Running ($NOT_RUNNING)"
  ERRORS=$((ERRORS + 1))
fi

# 2. UERANSIM Running 11/11
log "Checking UERANSIM pod..."
UERANSIM_READY=$(kubectl get pod -n "$CORE_NS" -l app=ueransim \
  -o jsonpath='{.items[0].status.containerStatuses[*].ready}' 2>/dev/null \
  | tr ' ' '\n' | grep -c "true" || echo 0)
if [[ "$UERANSIM_READY" -eq 11 ]]; then
  pass "UERANSIM 11/11 containers ready"
else
  fail "UERANSIM: only $UERANSIM_READY/11 containers ready"
  ERRORS=$((ERRORS + 1))
fi

# 3. Prometheus scraping 5G core
log "Checking Prometheus has 5G core metrics..."
PROM_COUNT=$(curl -s \
  "$PROMETHEUS_URL/api/v1/query?query=up%7Bnamespace%3D%225g-core%22%7D" \
  | jq '.data.result | length')
if [[ "$PROM_COUNT" -gt 0 ]]; then
  pass "Prometheus: $PROM_COUNT scrape targets in 5g-core"
else
  fail "Prometheus: no scrape targets found for 5g-core"
  ERRORS=$((ERRORS + 1))
fi

# 4. Loki has Free5GC logs
log "Checking Loki has 5G core logs..."
LOKI_COUNT=$(curl -s \
  --data-urlencode 'query={namespace="5g-core"}' \
  --data-urlencode "start=$(date -d '10 minutes ago' +%s)000000000" \
  --data-urlencode "end=$(date +%s)000000000" \
  --data-urlencode "limit=5" \
  "$LOKI_URL/loki/api/v1/query_range" \
  | jq '.data.result | length')
if [[ "$LOKI_COUNT" -gt 0 ]]; then
  pass "Loki: $LOKI_COUNT log streams from 5g-core"
else
  fail "Loki: no log streams from 5g-core"
  ERRORS=$((ERRORS + 1))
fi

# 5. Local Memgraph reachable on bolt port 7687
log "Checking local Memgraph (bolt://localhost:7687)..."
MG_COUNT=$(echo "MATCH (n) RETURN count(n);" \
  | mgconsole -host localhost -port 7687 2>/dev/null \
  | grep -oP '\d+' | tail -1 || echo "UNREACHABLE")
if [[ "$MG_COUNT" != "UNREACHABLE" ]]; then
  pass "Local Memgraph reachable — $MG_COUNT node(s) in graph"
else
  fail "Local Memgraph not reachable on port 7687 — start Memgraph before proceeding"
  ERRORS=$((ERRORS + 1))
fi

# 6. LLM inference service responding
log "Checking LLM inference service..."
LLM_URL="http://qwen3-4b.ml-serving.svc.cluster.local/v1/models"
LLM_MODEL=$(curl -s --max-time 10 "$LLM_URL" | jq -r '.data[0].id // "NOT FOUND"')
if [[ "$LLM_MODEL" != "NOT FOUND" ]]; then
  pass "LLM inference: model $LLM_MODEL available"
else
  fail "LLM inference: service not responding or model not found"
  ERRORS=$((ERRORS + 1))
fi

# Summary
echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 0 PASSED — cluster + local Memgraph are ready"
  exit 0
else
  fail "Phase 0 FAILED — $ERRORS check(s) failed — do not proceed"
  exit 1
fi
PHASE0
chmod +x /workspace/net-rca/scripts/testing/phase0-preflight.sh
```

- [ ] **Step 3.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/phase0-preflight.sh && echo "Syntax OK"
```

- [ ] **Step 3.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/phase0-preflight.sh
git commit -m "test(scripts): add Phase 0 pre-flight check script (local-pod variant)"
```

---

## Chunk 2: Start Local Agent and Verify

### Task 4: Write Phase 0.5 local-start script

**Files:**
- Create: `scripts/testing/phase05-start-local.sh`

**What this replaces:** The original `phase05-deploy.sh` built a Docker image, imported it into k3s, and deployed via kubectl. This script instead:
1. Loads the 3 DAG Cypher scripts into the local Memgraph instance.
2. Starts `uvicorn` in the background, logging to `$TRIAGE_LOG`.
3. Waits for `/health` to return healthy.
4. Exports `WEBHOOK_URL` and `ARTIFACTS_DIR` to `$RESULTS_DIR/env.sh`.

- [ ] **Step 4.1: Create phase05-start-local.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase05-start-local.sh << 'PHASE05'
#!/usr/bin/env bash
# phase05-start-local.sh — load DAGs into local Memgraph + start TriageAgent via uvicorn
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

log "=== Phase 0.5: Load DAGs + Start TriageAgent Locally ==="
cd /workspace/net-rca

# 1. Load DAGs into local Memgraph
log "Loading DAGs into local Memgraph (bolt://localhost:7687)..."
for dag in dags/registration_general.cypher \
           dags/authentication_5g_aka.cypher \
           dags/pdu_session_establishment.cypher; do
  if [[ -f "$dag" ]]; then
    mgconsole -host localhost -port 7687 < "$dag"
    pass "DAG loaded: $dag"
  else
    fail "DAG file not found: $dag"
    exit 1
  fi
done

# 2. Verify DAGs are in Memgraph
log "Verifying DAG nodes in Memgraph..."
DAG_COUNT=$(echo "MATCH (t:ReferenceTrace) RETURN count(t);" \
  | mgconsole -host localhost -port 7687 2>/dev/null \
  | grep -oP '\d+' | tail -1 || echo 0)
if [[ "$DAG_COUNT" -ge 3 ]]; then
  pass "Memgraph: $DAG_COUNT ReferenceTrace node(s) loaded"
else
  fail "Memgraph: expected ≥3 ReferenceTrace nodes, got $DAG_COUNT"
  exit 1
fi

# 3. Stop any existing uvicorn on port 8000
if lsof -ti:8000 > /dev/null 2>&1; then
  log "Stopping existing process on port 8000..."
  kill "$(lsof -ti:8000)" 2>/dev/null || true
  sleep 2
fi

# 4. Start uvicorn in background
log "Starting TriageAgent with uvicorn (log: $TRIAGE_LOG)..."
mkdir -p "$ARTIFACTS_DIR"
nohup uvicorn triage_agent.api.webhook:app --port 8000 \
  > "$TRIAGE_LOG" 2>&1 &
UVICORN_PID=$!
echo "$UVICORN_PID" > /tmp/triage-agent.pid
log "uvicorn PID: $UVICORN_PID"

# 5. Wait for /health to return 200 (up to 30s)
log "Waiting for TriageAgent to be healthy..."
ELAPSED=0
until curl -s --max-time 3 "$WEBHOOK_URL/health" | jq -e '.status == "healthy"' > /dev/null 2>&1; do
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  if [[ $ELAPSED -ge 30 ]]; then
    fail "TriageAgent did not become healthy within 30s. Check $TRIAGE_LOG"
    tail -20 "$TRIAGE_LOG"
    exit 1
  fi
done
pass "TriageAgent healthy at $WEBHOOK_URL"

# 6. Save env for subsequent scripts
cat > "$RESULTS_DIR/env.sh" << ENV
export WEBHOOK_URL=$WEBHOOK_URL
export ARTIFACTS_DIR=$ARTIFACTS_DIR
export TRIAGE_LOG=$TRIAGE_LOG
export UVICORN_PID=$UVICORN_PID
ENV
log "Environment saved to $RESULTS_DIR/env.sh"
pass "Phase 0.5 COMPLETE — TriageAgent running locally (PID $UVICORN_PID)"
PHASE05
chmod +x /workspace/net-rca/scripts/testing/phase05-start-local.sh
```

- [ ] **Step 4.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/phase05-start-local.sh && echo "Syntax OK"
```

- [ ] **Step 4.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/phase05-start-local.sh
git commit -m "test(scripts): add Phase 0.5 local DAG-load and uvicorn start script"
```

---

### Task 5: Write Phase 1 health verification script

**Files:**
- Create: `scripts/testing/phase1-health.sh`

**Key changes from original:** Container state checks (`dag-loader`, `memgraph`, `triage-agent`) replaced with: local process check (uvicorn PID alive, port 8000 responding). Memgraph DAG check uses local `mgconsole` instead of `kubectl exec -c memgraph`. `/health` and `/health/ready` endpoint checks are unchanged.

- [ ] **Step 5.1: Create phase1-health.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase1-health.sh << 'PHASE1'
#!/usr/bin/env bash
# phase1-health.sh — verify local TriageAgent health, DAGs, and endpoints
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
check_local_agent

log "=== Phase 1: Health Verification ==="
ERRORS=0

# 1. uvicorn process alive
log "Checking uvicorn process..."
if [[ -f /tmp/triage-agent.pid ]]; then
  UVICORN_PID=$(cat /tmp/triage-agent.pid)
  if kill -0 "$UVICORN_PID" 2>/dev/null; then
    pass "uvicorn process alive (PID $UVICORN_PID)"
  else
    fail "uvicorn PID $UVICORN_PID is not running"
    ERRORS=$((ERRORS+1))
  fi
else
  fail "No PID file at /tmp/triage-agent.pid — was phase05-start-local.sh run?"
  ERRORS=$((ERRORS+1))
fi

# 2. Port 8000 listening
log "Checking port 8000 is open..."
if lsof -ti:8000 > /dev/null 2>&1; then
  pass "Port 8000 is open"
else
  fail "Port 8000 is not open — TriageAgent may not be listening"
  ERRORS=$((ERRORS+1))
fi

# 3. DAG names loaded in local Memgraph (exact PascalCase required)
log "Verifying DAGs in local Memgraph..."
DAG_OUTPUT=$(echo "MATCH (t:ReferenceTrace) RETURN t.name;" \
  | mgconsole -host localhost -port 7687 2>/dev/null \
  | tee "$RESULTS_DIR/phase1-dags.txt")

for DAG_NAME in "Registration_General" "Authentication_5G_AKA" "PDU_Session_Establishment"; do
  if echo "$DAG_OUTPUT" | grep -q "$DAG_NAME"; then
    pass "DAG loaded: $DAG_NAME"
  else
    fail "DAG missing: $DAG_NAME"
    ERRORS=$((ERRORS+1))
  fi
done

# 4. /health endpoint — all dependencies green
log "Checking /health endpoint..."
HEALTH=$(curl -s "$WEBHOOK_URL/health" | tee "$RESULTS_DIR/phase1-health.json")
echo "$HEALTH" | jq .
HEALTH_STATUS=$(echo "$HEALTH" | jq -r '.status')
MEMGRAPH_OK=$(echo "$HEALTH" | jq -r '.memgraph')
PROMETHEUS_OK=$(echo "$HEALTH" | jq -r '.prometheus')
LOKI_OK=$(echo "$HEALTH" | jq -r '.loki')

[[ "$HEALTH_STATUS" == "healthy" && "$MEMGRAPH_OK" == "true" \
  && "$PROMETHEUS_OK" == "true" && "$LOKI_OK" == "true" ]] && \
  pass "/health: healthy, memgraph=true, prometheus=true, loki=true" || \
  { fail "/health check failed: $HEALTH"; ERRORS=$((ERRORS+1)); }

# 5. /health/ready
log "Checking /health/ready..."
READY_CODE=$(curl -o /dev/null -s -w "%{http_code}" "$WEBHOOK_URL/health/ready")
[[ "$READY_CODE" == "200" ]] && \
  pass "/health/ready: 200 OK" || { fail "/health/ready: $READY_CODE"; ERRORS=$((ERRORS+1)); }

# Summary
echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 1 PASSED — TriageAgent healthy and ready"
  exit 0
else
  fail "Phase 1 FAILED — $ERRORS check(s) failed"
  exit 1
fi
PHASE1
chmod +x /workspace/net-rca/scripts/testing/phase1-health.sh
```

- [ ] **Step 5.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/phase1-health.sh && echo "Syntax OK"
```

- [ ] **Step 5.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/phase1-health.sh
git commit -m "test(scripts): add Phase 1 health verification script (local-pod variant)"
```

---

## Chunk 3: Component and Integration Validation

### Task 6: Write Phase 2 component validation script

**Files:**
- Create: `scripts/testing/phase2-components.sh`

**Key changes from original:**
- `kubectl exec -c triage-agent -- cat /app/artifacts/...` → `cat "$ARTIFACTS_DIR/..."`
- `kubectl exec -c memgraph -- bash -c '... | mgconsole'` → `echo "..." | mgconsole -host localhost -port 7687`
- `kubectl logs -c triage-agent` removed (use `cat "$TRIAGE_LOG"` for triage-agent output)

- [ ] **Step 6.1: Create phase2-components.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase2-components.sh << 'PHASE2'
#!/usr/bin/env bash
# phase2-components.sh — generate traffic + verify each agent performs as designed (local-pod)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
check_local_agent

log "=== Phase 2: Component Validation ==="
ERRORS=0

# ── Step 2.0: Generate live 5G traffic ────────────────────────────────────────
log "Step 2.0 — Restarting UERANSIM to generate fresh registration traffic..."
kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"

log "Waiting 30s for UE registration procedures to complete..."
sleep 30

log "Checking all 10 IMSIs appear in AMF logs..."
if check_imsi_loki 5; then
  pass "Step 2.0: All 10 IMSIs registered and visible in Loki"
else
  fail "Step 2.0: Not all IMSIs visible in Loki — check UERANSIM logs"
  kubectl logs -n "$CORE_NS" \
    "$(kubectl get pod -n "$CORE_NS" -l app=ueransim -o jsonpath='{.items[0].metadata.name}')" \
    -c ue-1 --tail=20
  ERRORS=$((ERRORS+1))
fi

# ── Trigger component test incident ──────────────────────────────────────────
log "Triggering component test incident (alertname=RegistrationFailures)..."
INCIDENT=$(trigger_webhook "RegistrationFailures" "amf" "warning")
log "Incident ID: $INCIDENT"
echo "$INCIDENT" > "$RESULTS_DIR/phase2-incident-id.txt"

log "Polling for completion (up to 360s)..."
REPORT=$(poll_incident "$INCIDENT" 360) || { fail "Incident did not complete"; exit 1; }

# ── Step 2.1: DagMapper ───────────────────────────────────────────────────────
log "Step 2.1 — Checking DagMapper output..."
MAPPING_CONF=$(echo "$REPORT" | jq -r '.final_report.mapping_confidence // empty')
PROC_NAMES=$(echo "$REPORT" | jq -r '.final_report.procedure_names // [] | @json')
if [[ "$MAPPING_CONF" == "1.0" ]] && echo "$PROC_NAMES" | grep -q "Registration_General"; then
  pass "DagMapper: mapping_confidence=1.0, Registration_General mapped"
else
  fail "DagMapper: mapping_confidence=$MAPPING_CONF, procedures=$PROC_NAMES"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.2: InfraAgent ─────────────────────────────────────────────────────
log "Step 2.2 — Checking InfraAgent output..."
INFRA_SCORE=$(echo "$REPORT" | jq -r '.final_report.infra_score // empty')
if [[ -n "$INFRA_SCORE" ]]; then
  pass "InfraAgent: infra_score=$INFRA_SCORE"
else
  fail "InfraAgent: infra_score missing from report"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.3: NfMetricsAgent ──────────────────────────────────────────────────
log "Step 2.3 — Checking NfMetricsAgent artifacts..."
METRICS_FILE="$ARTIFACTS_DIR/$INCIDENT/post_filter_metrics.json"
if [[ -f "$METRICS_FILE" ]]; then
  NF_COUNT=$(jq 'keys | length' "$METRICS_FILE" 2>/dev/null || echo 0)
  if [[ "$NF_COUNT" -gt 0 ]]; then
    pass "NfMetricsAgent: post_filter_metrics.json has $NF_COUNT NFs"
  else
    fail "NfMetricsAgent: post_filter_metrics.json is empty"
    ERRORS=$((ERRORS+1))
  fi
else
  fail "NfMetricsAgent: post_filter_metrics.json not found at $METRICS_FILE"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.4: NfLogsAgent ────────────────────────────────────────────────────
log "Step 2.4 — Checking NfLogsAgent artifacts..."
LOGS_FILE="$ARTIFACTS_DIR/$INCIDENT/post_filter_logs.json"
LOG_NF_COUNT=0
if [[ -f "$LOGS_FILE" ]]; then
  LOG_NF_COUNT=$(jq 'keys | length' "$LOGS_FILE" 2>/dev/null || echo 0)
fi
if [[ "$LOG_NF_COUNT" -gt 0 ]]; then
  pass "NfLogsAgent: post_filter_logs.json has $LOG_NF_COUNT NFs"
else
  fail "NfLogsAgent: post_filter_logs.json empty or missing"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.5: UeTracesAgent ──────────────────────────────────────────────────
log "Step 2.5 — Checking UeTracesAgent Memgraph write..."
TRACE_COUNT=$(echo "MATCH (t:CapturedTrace) RETURN count(t);" \
  | mgconsole -host localhost -port 7687 2>/dev/null \
  | grep -oP '\d+' | tail -1 || echo 0)
if [[ "$TRACE_COUNT" -gt 0 ]]; then
  pass "UeTracesAgent: $TRACE_COUNT CapturedTrace(s) in Memgraph"
else
  fail "UeTracesAgent: no CapturedTrace nodes found"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.6: EvidenceQualityAgent ───────────────────────────────────────────
log "Step 2.6 — Checking EvidenceQualityAgent score..."
EQ_SCORE=$(echo "$REPORT" | jq -r '.final_report.evidence_quality_score // 0')
EQ_OK=$(echo "$EQ_SCORE >= 0.50" | bc -l 2>/dev/null || echo 0)
if [[ "$EQ_OK" -eq 1 ]]; then
  pass "EvidenceQualityAgent: evidence_quality_score=$EQ_SCORE (≥0.50)"
else
  fail "EvidenceQualityAgent: evidence_quality_score=$EQ_SCORE (<0.50)"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.7: RCAAgent ───────────────────────────────────────────────────────
log "Step 2.7 — Checking RCAAgent output..."
ROOT_NF=$(echo "$REPORT" | jq -r '.final_report.root_nf // empty')
FAIL_MODE=$(echo "$REPORT" | jq -r '.final_report.failure_mode // empty')
LAYER=$(echo "$REPORT" | jq -r '.final_report.layer // empty')
CONF=$(echo "$REPORT" | jq -r '.final_report.confidence // 0')
EVIDENCE=$(echo "$REPORT" | jq -r '.final_report.evidence_chain // empty')

if [[ -n "$ROOT_NF" && -n "$FAIL_MODE" && -n "$LAYER" \
  && -n "$CONF" && -n "$EVIDENCE" && "$FAIL_MODE" != "llm_timeout" ]]; then
  pass "RCAAgent: all 5 fields present, no llm_timeout"
  log "  root_nf=$ROOT_NF  failure_mode=$FAIL_MODE  layer=$LAYER  confidence=$CONF"
else
  fail "RCAAgent: missing fields or llm_timeout. root_nf=$ROOT_NF failure_mode=$FAIL_MODE"
  ERRORS=$((ERRORS+1))
fi

# ── Copy artifacts ────────────────────────────────────────────────────────────
pull_artifacts "$INCIDENT"

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 2 PASSED — all 7 components perform as designed"
  exit 0
else
  fail "Phase 2 FAILED — $ERRORS component check(s) failed"
  exit 1
fi
PHASE2
chmod +x /workspace/net-rca/scripts/testing/phase2-components.sh
```

- [ ] **Step 6.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/phase2-components.sh && echo "Syntax OK"
```

- [ ] **Step 6.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/phase2-components.sh
git commit -m "test(scripts): add Phase 2 component validation script (local-pod variant)"
```

---

### Task 7: Write Phase 3 integration validation script

**Files:**
- Create: `scripts/testing/phase3-integration.sh`

**Key changes from original:**
- All `kubectl exec -c triage-agent -- cat` replaced with local `cat "$ARTIFACTS_DIR/..."`
- All `kubectl exec -c memgraph -- bash -c '... | mgconsole'` replaced with `echo "..." | mgconsole -host localhost -port 7687`
- `kubectl logs -c triage-agent` replaced with `grep` on `$TRIAGE_LOG`
- `kubectl exec -n ... ls /app/artifacts/...` replaced with `ls "$ARTIFACTS_DIR/..."`

- [ ] **Step 7.1: Create phase3-integration.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase3-integration.sh << 'PHASE3'
#!/usr/bin/env bash
# phase3-integration.sh — verify cross-component wiring + token budgets (local-pod)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
check_local_agent

log "=== Phase 3: Integration Validation ==="
ERRORS=0

# Reuse Phase 2 incident or trigger new one
if [[ -f "$RESULTS_DIR/phase2-incident-id.txt" ]]; then
  INCIDENT=$(cat "$RESULTS_DIR/phase2-incident-id.txt")
  log "Reusing Phase 2 incident: $INCIDENT"
else
  log "No Phase 2 incident found — triggering new one..."
  INCIDENT=$(trigger_webhook "RegistrationFailures" "amf" "warning")
  poll_incident "$INCIDENT" 360 > /dev/null
fi

# ── 3.1: NfMetricsAgent ↔ Prometheus ────────────────────────────────────────
log "3.1 — NfMetricsAgent ↔ Prometheus (all NFs in nf_union)..."
METRICS_FILE="$ARTIFACTS_DIR/$INCIDENT/post_filter_metrics.json"
METRIC_NFS=0
[[ -f "$METRICS_FILE" ]] && METRIC_NFS=$(jq 'keys | length' "$METRICS_FILE" 2>/dev/null || echo 0)
if [[ "$METRIC_NFS" -gt 0 ]]; then
  pass "3.1: Prometheus data for $METRIC_NFS NFs"
  jq 'keys' "$METRICS_FILE" | tee "$RESULTS_DIR/phase3-metric-nfs.txt"
else
  fail "3.1: No NF metrics returned from Prometheus"
  ERRORS=$((ERRORS+1))
fi
log "Token count for 3.1:"
collect_token_count "$INCIDENT" "post_filter_metrics.json"

# ── 3.2: NfLogsAgent ↔ Loki ─────────────────────────────────────────────────
log "3.2 — NfLogsAgent ↔ Loki (path selection + all NFs in nf_union)..."
LOGS_FILE="$ARTIFACTS_DIR/$INCIDENT/post_filter_logs.json"
LOG_NFS=0
[[ -f "$LOGS_FILE" ]] && LOG_NFS=$(jq 'keys | length' "$LOGS_FILE" 2>/dev/null || echo 0)
if [[ "$LOG_NFS" -gt 0 ]]; then
  pass "3.2: Loki data for $LOG_NFS NFs"
  jq 'keys' "$LOGS_FILE" | tee "$RESULTS_DIR/phase3-log-nfs.txt"
else
  fail "3.2: No NF logs returned from Loki"
  ERRORS=$((ERRORS+1))
fi
# Check which Loki path was used (MCP or direct)
MCP_PATH=$(grep -E "MCP server unavailable|using direct Loki" "$TRIAGE_LOG" 2>/dev/null | tail -1 \
  || echo "MCP path used (no fallback message in log)")
log "  Loki path: $MCP_PATH"
echo "$MCP_PATH" > "$RESULTS_DIR/phase3-loki-path.txt"
log "Token count for 3.2:"
collect_token_count "$INCIDENT" "post_filter_logs.json"

# ── 3.3: UeTracesAgent ↔ Memgraph (write) ───────────────────────────────────
log "3.3 — UeTracesAgent ↔ Memgraph (CapturedTrace write)..."
TRACE_COUNT=$(echo "MATCH (t:CapturedTrace) RETURN count(t);" \
  | mgconsole -host localhost -port 7687 2>/dev/null \
  | grep -oP '\d+' | tail -1 || echo 0)
if [[ "$TRACE_COUNT" -gt 0 ]]; then
  pass "3.3: $TRACE_COUNT CapturedTrace node(s) in Memgraph"
else
  fail "3.3: No CapturedTrace nodes found"
  ERRORS=$((ERRORS+1))
fi
echo "$TRACE_COUNT CapturedTrace nodes" > "$RESULTS_DIR/phase3-captured-traces.txt"

# ── 3.4: UeTracesAgent ↔ Memgraph (deviation detection) ────────────────────
log "3.4 — UeTracesAgent deviation detection (all 3 DAGs)..."
DEV_OUTPUT=$(echo "MATCH (t:CapturedTrace)-[:DEVIATES_FROM]->(r:ReferenceTrace) RETURN r.name, count(t);" \
  | mgconsole -host localhost -port 7687 2>/dev/null \
  | tee "$RESULTS_DIR/phase3-deviations.txt" || echo "")
if grep -qE "Cypher.*error|CypherError" "$TRIAGE_LOG" 2>/dev/null; then
  fail "3.4: Cypher query errors in triage-agent log"
  ERRORS=$((ERRORS+1))
else
  pass "3.4: Deviation detection ran without Cypher errors"
fi
echo "Deviation output:" && echo "$DEV_OUTPUT"

# ── 3.5: InfraAgent token contribution ───────────────────────────────────────
log "3.5 — InfraAgent token contribution to RCA..."
COMPRESSED_FILE="$ARTIFACTS_DIR/$INCIDENT/compressed_evidence.json"
INFRA_EVIDENCE=""
[[ -f "$COMPRESSED_FILE" ]] && INFRA_EVIDENCE=$(jq '.infra // empty' "$COMPRESSED_FILE" 2>/dev/null || echo "")
INFRA_TOKENS=$(( ${#INFRA_EVIDENCE} / 4 ))
echo "  infra evidence block: ~${INFRA_TOKENS} tokens (budget: 400)"
echo "infra|${INFRA_TOKENS}" >> "$RESULTS_DIR/token_counts.txt"
if [[ "$INFRA_TOKENS" -le 400 ]]; then
  pass "3.5: InfraAgent tokens within budget (~${INFRA_TOKENS} ≤ 400)"
else
  log "  WARNING: InfraAgent tokens (~${INFRA_TOKENS}) exceed 400 ceiling"
fi

# ── 3.6: DagMapper token contribution ────────────────────────────────────────
log "3.6 — DagMapper token contribution to RCA..."
DAG_EVIDENCE=""
[[ -f "$COMPRESSED_FILE" ]] && DAG_EVIDENCE=$(jq '.dags // empty' "$COMPRESSED_FILE" 2>/dev/null || echo "")
DAG_TOKENS=$(( ${#DAG_EVIDENCE} / 4 ))
echo "  dag evidence block: ~${DAG_TOKENS} tokens (budget: 800)"
echo "dags|${DAG_TOKENS}" >> "$RESULTS_DIR/token_counts.txt"
if [[ "$DAG_TOKENS" -le 800 ]]; then
  pass "3.6: DagMapper tokens within budget (~${DAG_TOKENS} ≤ 800)"
else
  log "  WARNING: DagMapper tokens (~${DAG_TOKENS}) exceed 800 ceiling"
fi

# ── 3.7: RCAAgent ↔ LLM token counts ────────────────────────────────────────
log "3.7 — RCAAgent ↔ LLM token counts..."
collect_token_count "$INCIDENT" "llm_prompt.txt"
collect_token_count "$INCIDENT" "llm_response.json"
REPORT=$(curl -s "$WEBHOOK_URL/incidents/$INCIDENT")
CONF=$(echo "$REPORT" | jq -r '.final_report.confidence // 0')
FAIL_MODE=$(echo "$REPORT" | jq -r '.final_report.failure_mode // ""')
if [[ "$FAIL_MODE" != "llm_timeout" ]]; then
  pass "3.7: LLM responded without timeout (confidence=$CONF)"
else
  fail "3.7: LLM returned llm_timeout sentinel"
  ERRORS=$((ERRORS+1))
fi

# ── 3.8: LangGraph execution order ───────────────────────────────────────────
log "3.8 — LangGraph execution order via artifact mtimes..."
ls -lt "$ARTIFACTS_DIR/$INCIDENT/" 2>/dev/null \
  | tee "$RESULTS_DIR/phase3-artifact-order.txt" || \
  log "  Warning: no artifacts directory at $ARTIFACTS_DIR/$INCIDENT/"
pass "3.8: Artifact timestamps saved to phase3-artifact-order.txt for manual verification"

# ── 3.9: Webhook incident lifecycle ──────────────────────────────────────────
log "3.9 — Webhook incident lifecycle..."
NEW_INCIDENT=$(trigger_webhook "RegistrationFailures" "amf" "warning")
PENDING=$(curl -s "$WEBHOOK_URL/incidents/$NEW_INCIDENT" | jq -r '.status')
if [[ "$PENDING" == "pending" ]]; then
  pass "3.9: Immediate poll returns status=pending"
else
  fail "3.9: Expected pending, got $PENDING"
  ERRORS=$((ERRORS+1))
fi

log "Waiting for second incident to complete..."
FINAL=$(poll_incident "$NEW_INCIDENT" 360)
FINAL_STATUS=$(echo "$FINAL" | jq -r '.status')
[[ "$FINAL_STATUS" == "complete" ]] && \
  pass "3.9: Lifecycle complete end-to-end" || \
  { fail "3.9: Final status=$FINAL_STATUS"; ERRORS=$((ERRORS+1)); }

pull_artifacts "$NEW_INCIDENT"

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 3 PASSED — all integration boundaries verified"
  exit 0
else
  fail "Phase 3 FAILED — $ERRORS integration check(s) failed"
  exit 1
fi
PHASE3
chmod +x /workspace/net-rca/scripts/testing/phase3-integration.sh
```

- [ ] **Step 7.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/phase3-integration.sh && echo "Syntax OK"
```

- [ ] **Step 7.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/phase3-integration.sh
git commit -m "test(scripts): add Phase 3 integration validation script (local-pod variant)"
```

---

## Chunk 4: End-to-End Scenarios

### Task 8: Write Phase 4 E2E scenarios script

**Files:**
- Create: `scripts/testing/phase4-e2e.sh`

**Key changes from original:** The `verify_rca` helper is unchanged (uses curl). Scenario inject/restore logic is unchanged (uses kubectl against Free5GC in cluster). All artifact-reading uses local `cat "$ARTIFACTS_DIR/..."`. No `kubectl exec` or `kubectl cp` needed.

- [ ] **Step 8.1: Create phase4-e2e.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase4-e2e.sh << 'PHASE4'
#!/usr/bin/env bash
# phase4-e2e.sh — 4 E2E scenarios with inject/trigger/verify/restore (local-pod)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
check_local_agent

log "=== Phase 4: End-to-End Validation ==="
ERRORS=0
SCENARIO_RESULTS=()

# ── Helper: verify RCA fields ─────────────────────────────────────────────────
verify_rca() {
  local incident_id=$1 expected_nf_re=$2 expected_layer=$3 label=$4
  local report root_nf layer confidence fail_mode eq_score

  report=$(curl -s "$WEBHOOK_URL/incidents/$incident_id")
  root_nf=$(echo "$report"    | jq -r '.final_report.root_nf // ""')
  layer=$(echo "$report"      | jq -r '.final_report.layer // ""')
  confidence=$(echo "$report" | jq -r '.final_report.confidence // 0')
  fail_mode=$(echo "$report"  | jq -r '.final_report.failure_mode // ""')
  eq_score=$(echo "$report"   | jq -r '.final_report.evidence_quality_score // 0')

  log "  root_nf=$root_nf  layer=$layer  confidence=$confidence"
  log "  failure_mode=$fail_mode  evidence_quality=$eq_score"
  echo "$report" | jq . >> "$RESULTS_DIR/${label}-report.json"

  local ok=true
  [[ "$root_nf" =~ $expected_nf_re ]] || { fail "$label: root_nf=$root_nf not in [$expected_nf_re]"; ok=false; }
  [[ "$layer" == "$expected_layer" ]]  || { fail "$label: layer=$layer expected $expected_layer"; ok=false; }
  [[ "$fail_mode" != "llm_timeout" ]]  || { fail "$label: llm_timeout sentinel"; ok=false; }
  local conf_ok; conf_ok=$(echo "$confidence >= 0.70" | bc -l)
  [[ "$conf_ok" -eq 1 ]] || { fail "$label: confidence=$confidence < 0.70"; ok=false; }
  local eq_ok; eq_ok=$(echo "$eq_score >= 0.50" | bc -l)
  [[ "$eq_ok" -eq 1 ]] || { fail "$label: evidence_quality=$eq_score < 0.50"; ok=false; }

  $ok && { pass "$label PASSED (root_nf=$root_nf layer=$layer confidence=$confidence)"; return 0; }
  ERRORS=$((ERRORS+1)); return 1
}

# ── 4.1: Sunny Day ────────────────────────────────────────────────────────────
log ""
log "=== Scenario 4.1: Sunny Day ==="
INCIDENT_41=$(trigger_webhook "RegistrationFailures" "amf" "warning")
log "Incident: $INCIDENT_41"
REPORT_41=$(poll_incident "$INCIDENT_41" 360)

INFRA_41=$(echo "$REPORT_41" | jq -r '.final_report.infra_score // 1')
INFRA_OK=$(echo "$INFRA_41 < 0.3" | bc -l)
[[ "$INFRA_OK" -eq 1 ]] && pass "4.1: infra_score=$INFRA_41 < 0.3 (no false positive)" \
  || fail "4.1: infra_score=$INFRA_41 ≥ 0.3 — possible false positive"

FAIL_41=$(echo "$REPORT_41" | jq -r '.final_report.failure_mode // ""')
[[ "$FAIL_41" != "llm_timeout" ]] && pass "4.1: LLM responded without timeout" \
  || { fail "4.1: llm_timeout"; ERRORS=$((ERRORS+1)); }

log "Recording baseline token counts..."
for artifact in pre_filter_metrics.json post_filter_metrics.json \
                pre_filter_logs.json post_filter_logs.json; do
  collect_token_count "$INCIDENT_41" "$artifact"
done
pull_artifacts "$INCIDENT_41"
SCENARIO_RESULTS+=("4.1:SUNNY_DAY:infra_score=$INFRA_41")

# ── 4.2: Registration Failure (AMF scaled to 0) ───────────────────────────────
log ""
log "=== Scenario 4.2: Registration Failure (AMF → 0 replicas) ==="

log "Injecting failure: scaling amf to 0..."
kubectl scale deployment amf -n "$CORE_NS" --replicas=0
kubectl get pods -n "$CORE_NS" | grep amf | tee "$RESULTS_DIR/scenario42-inject.txt"
sleep 30

log "Restarting UERANSIM to trigger registration attempts against unavailable AMF..."
kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 15

INCIDENT_42=$(trigger_webhook "RegistrationFailures" "amf")
log "Incident: $INCIDENT_42"
REPORT_42=$(poll_incident "$INCIDENT_42" 360)

verify_rca "$INCIDENT_42" "^AMF$" "infrastructure" "4.2" \
  && SCENARIO_RESULTS+=("4.2:REG_FAIL:PASS") || SCENARIO_RESULTS+=("4.2:REG_FAIL:FAIL")
pull_artifacts "$INCIDENT_42"

log "Restoring: scaling amf back to 1..."
kubectl scale deployment amf -n "$CORE_NS" --replicas=1
kubectl rollout status deployment amf -n "$CORE_NS"
sleep 15

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 30
log "Confirming UEs re-registered after restore..."
check_imsi_loki 3 || log "WARNING: not all IMSIs visible after restore — proceeding with caution"

# ── 4.3: Authentication Failure (wrong OPC key) ───────────────────────────────
log ""
log "=== Scenario 4.3: Authentication Failure (wrong op key) ==="

log "Backing up ue-config..."
kubectl get configmap ue-config -n "$CORE_NS" -o yaml \
  > "$RESULTS_DIR/ue-config-backup.yaml"

log "Injecting failure: patching op key to zeroed value..."
kubectl get configmap ue-config -n "$CORE_NS" -o yaml \
  | sed "s/op: '8e27b6af0e692e750f32667a3b14605d'/op: '00000000000000000000000000000000'/" \
  | kubectl apply -f -
OP_PATCHED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "^op:" | head -1)
log "  op field after patch: $OP_PATCHED"

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 30

INCIDENT_43=$(trigger_webhook "AuthenticationFailures" "ausf")
log "Incident: $INCIDENT_43"
REPORT_43=$(poll_incident "$INCIDENT_43" 360)

verify_rca "$INCIDENT_43" "^(AUSF|UDM)$" "application" "4.3" \
  && SCENARIO_RESULTS+=("4.3:AUTH_FAIL:PASS") || SCENARIO_RESULTS+=("4.3:AUTH_FAIL:FAIL")
pull_artifacts "$INCIDENT_43"

log "Restoring: applying ue-config backup..."
kubectl apply -f "$RESULTS_DIR/ue-config-backup.yaml"
OP_RESTORED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "^op:" | head -1)
log "  op field after restore: $OP_RESTORED"

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 30
log "Confirming UEs re-registered after restore..."
check_imsi_loki 3 || log "WARNING: not all IMSIs visible after restore"

# ── 4.4: PDU Session Failure (wrong APN) ──────────────────────────────────────
log ""
log "=== Scenario 4.4: PDU Session Failure (wrong APN) ==="

log "Injecting failure: patching APN to invalid-internet..."
kubectl get configmap ue-config -n "$CORE_NS" -o yaml \
  | sed "s/apn: 'internet'/apn: 'invalid-internet'/" \
  | kubectl apply -f -
APN_PATCHED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "apn:" | head -1)
log "  APN after patch: $APN_PATCHED"

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 30

INCIDENT_44=$(trigger_webhook "PDUSessionFailures" "smf")
log "Incident: $INCIDENT_44"
REPORT_44=$(poll_incident "$INCIDENT_44" 360)

verify_rca "$INCIDENT_44" "^SMF$" "application" "4.4" \
  && SCENARIO_RESULTS+=("4.4:PDU_FAIL:PASS") || SCENARIO_RESULTS+=("4.4:PDU_FAIL:FAIL")
pull_artifacts "$INCIDENT_44"

log "Restoring: applying ue-config backup (reuse from 4.3)..."
kubectl apply -f "$RESULTS_DIR/ue-config-backup.yaml"
APN_RESTORED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "apn:" | head -1)
log "  APN after restore: $APN_RESTORED"

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"

# ── Overall summary ───────────────────────────────────────────────────────────
log ""
log "=== Phase 4 Summary ==="
CORRECT=0
for result in "${SCENARIO_RESULTS[@]}"; do
  echo "  $result" | tee -a "$RESULTS_DIR/phase4-summary.txt"
done

for result in "${SCENARIO_RESULTS[@]:1}"; do
  [[ "$result" == *":PASS" ]] && CORRECT=$((CORRECT+1)) || true
done
log "Failure scenario accuracy: $CORRECT / 3 correct (need ≥ 2)"

if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 4 PASSED"
  exit 0
else
  fail "Phase 4: $ERRORS check(s) failed"
  cat "$RESULTS_DIR/summary.txt"
  exit 1
fi
PHASE4
chmod +x /workspace/net-rca/scripts/testing/phase4-e2e.sh
```

- [ ] **Step 8.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/phase4-e2e.sh && echo "Syntax OK"
```

- [ ] **Step 8.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/phase4-e2e.sh
git commit -m "test(scripts): add Phase 4 E2E scenario script (local-pod variant)"
```

---

### Task 9: Write results collection script and orchestrator

**Files:**
- Create: `scripts/testing/run-all.sh`

**Key changes from original:**
- No port-forward check (TriageAgent is local, no kubectl port-forward needed).
- Calls `phase05-start-local.sh` instead of `phase05-deploy.sh`.
- On exit, offers to stop uvicorn via saved PID file.

- [ ] **Step 9.1: Create run-all.sh**

```bash
cat > /workspace/net-rca/scripts/testing/run-all.sh << 'RUNALL'
#!/usr/bin/env bash
# run-all.sh — execute all test phases in order; gate each phase on pass (local-pod)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Create a single timestamped results directory shared across all phases
export RESULTS_DIR="/workspace/net-rca/test-results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
echo "Results directory: $RESULTS_DIR"

source "$SCRIPT_DIR/helpers.sh"

log "Starting full test run (local-pod variant). Results: $RESULTS_DIR"

"$SCRIPT_DIR/phase0-preflight.sh"     || { echo "GATE: Phase 0 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase05-start-local.sh"  || { echo "GATE: Phase 0.5 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase1-health.sh"        || { echo "GATE: Phase 1 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase2-components.sh"    || { echo "GATE: Phase 2 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase3-integration.sh"   || { echo "GATE: Phase 3 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase4-e2e.sh"

echo ""
echo "=== FINAL RESULTS ==="
cat "$RESULTS_DIR/summary.txt"
echo ""
echo "Full results in: $RESULTS_DIR"

# Offer to stop uvicorn
if [[ -f /tmp/triage-agent.pid ]]; then
  echo ""
  echo "TriageAgent (uvicorn) is still running (PID $(cat /tmp/triage-agent.pid))."
  echo "To stop: kill \$(cat /tmp/triage-agent.pid)"
fi
RUNALL
chmod +x /workspace/net-rca/scripts/testing/run-all.sh
```

- [ ] **Step 9.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/run-all.sh && echo "Syntax OK"
```

- [ ] **Step 9.3: Verify all scripts have correct syntax**

```bash
for f in /workspace/net-rca/scripts/testing/*.sh; do
  bash -n "$f" && echo "OK: $f" || echo "FAIL: $f"
done
```
Expected: all files print `OK`.

- [ ] **Step 9.4: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/run-all.sh
git commit -m "test(scripts): add run-all.sh orchestrator for full test suite (local-pod variant)"
```

---

### Task 10: Execute the full test plan

- [ ] **Step 10.1: Ensure Memgraph is running locally**

```bash
# Verify Memgraph bolt is reachable before starting
mgconsole -host localhost -port 7687 <<< "MATCH (n) RETURN count(n);"
```
Expected: returns a count (may be 0 if fresh).

- [ ] **Step 10.2: Run the full test suite**

```bash
cd /workspace/net-rca
export WEBHOOK_URL=http://localhost:8000
export ARTIFACTS_DIR=/workspace/net-rca/artifacts

bash scripts/testing/run-all.sh 2>&1 | tee /tmp/test-run.log
```

Expected final output (all phases pass):
```
[PASS] Phase 0 PASSED — cluster + local Memgraph are ready
[PASS] Phase 0.5 COMPLETE — TriageAgent running locally
[PASS] Phase 1 PASSED — TriageAgent healthy and ready
[PASS] Phase 2 PASSED — all 7 components perform as designed
[PASS] Phase 3 PASSED — all integration boundaries verified
[PASS] 4.1 PASSED ...
[PASS] 4.2 PASSED (root_nf=AMF layer=infrastructure ...)
[PASS] 4.3 PASSED (root_nf=AUSF|UDM layer=application ...)
[PASS] 4.4 PASSED (root_nf=SMF layer=application ...)
```

- [ ] **Step 10.3: Commit test results**

```bash
cd /workspace/net-rca
RESULTS_DIR=$(ls -td test-results/*/ | head -1)
git add test-results/
git commit -m "test(results): live test run (local-pod) against Free5GC on k3s $(date +%Y-%m-%d)"
```

- [ ] **Step 10.4: If any phase fails — triage**

```bash
# Check triage-agent logs
tail -100 /tmp/triage-agent.log

# Browse local artifacts
ls /workspace/net-rca/artifacts/
cat /workspace/net-rca/artifacts/<incident-id>/post_filter_logs.json

# Check local Memgraph state
mgconsole -host localhost -port 7687 <<< "MATCH (t:ReferenceTrace) RETURN t.name;"
mgconsole -host localhost -port 7687 <<< "MATCH (t:CapturedTrace) RETURN count(t);"

# Restart TriageAgent if needed
kill $(cat /tmp/triage-agent.pid) 2>/dev/null || true
cd /workspace/net-rca
nohup uvicorn triage_agent.api.webhook:app --port 8000 > /tmp/triage-agent.log 2>&1 &
echo $! > /tmp/triage-agent.pid
```
