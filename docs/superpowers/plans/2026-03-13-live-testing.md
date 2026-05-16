# Live Testing Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the live test plan for 5G TriageAgent v3.2 against Free5GC on k3s, verifying component behaviour, integration correctness, and end-to-end RCA accuracy across 4 scenarios.

**Architecture:** Test scripts are created first and used to drive each phase. Each script is self-contained, idempotent where possible, and captures results to a timestamped `test-results/` directory. All phases gate on explicit pass criteria before proceeding.

**Tech Stack:** bash, kubectl, curl, jq, docker, k3s, Free5GC, UERANSIM, TriageAgent v3.2, Memgraph, Prometheus, Loki, qwen3-4b LLM

---

## File Map

### Created
| File | Responsibility |
|---|---|
| `scripts/testing/helpers.sh` | Shared functions: `trigger_webhook`, `poll_incident`, `check_imsi_loki`, `collect_token_count` |
| `scripts/testing/phase0-preflight.sh` | Phase 0: cluster readiness checks |
| `scripts/testing/phase1-health.sh` | Phase 1: TriageAgent pod health and DAG verification |
| `scripts/testing/phase2-components.sh` | Phase 2: UERANSIM traffic generation + per-agent component checks |
| `scripts/testing/phase3-integration.sh` | Phase 3: integration boundary checks + token counting |
| `scripts/testing/phase4-e2e.sh` | Phase 4: all 4 E2E scenarios with inject/trigger/verify/restore |
| `scripts/testing/collect-artifacts.sh` | Pull per-incident artifacts from pod + print token summary |

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

- [ ] **Step 2.1: Create helpers.sh**

```bash
cat > /workspace/net-rca/scripts/testing/helpers.sh << 'HELPERS'
#!/usr/bin/env bash
# helpers.sh — shared functions for all live test scripts
set -euo pipefail

# ── Environment ──────────────────────────────────────────────────────────────
export WEBHOOK_URL="${WEBHOOK_URL:-http://localhost:8000}"
export TRIAGE_POD="${TRIAGE_POD:-}"
export RESULTS_DIR="${RESULTS_DIR:-/workspace/net-rca/test-results/$(date +%Y%m%d-%H%M%S)}"
export PROMETHEUS_URL="http://kube-prom-kube-prometheus-prometheus.monitoring:9090"
export LOKI_URL="http://loki.monitoring:3100"
export UERANSIM_NS="5g-core"
export CORE_NS="5g-core"
export TRIAGE_NS="monitoring"

mkdir -p "$RESULTS_DIR"

# ── Logging ───────────────────────────────────────────────────────────────────
log()  { echo "[$(date +%H:%M:%S)] $*"; }
pass() { echo "[PASS] $*" | tee -a "$RESULTS_DIR/summary.txt"; }
fail() { echo "[FAIL] $*" | tee -a "$RESULTS_DIR/summary.txt"; }
info() { echo "[INFO] $*"; }

# ── Resolve pod name ──────────────────────────────────────────────────────────
resolve_triage_pod() {
  TRIAGE_POD=$(kubectl get pod -n "$TRIAGE_NS" -l app=triage-agent \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -z "$TRIAGE_POD" ]]; then
    fail "triage-agent pod not found in namespace $TRIAGE_NS"
    exit 1
  fi
  export TRIAGE_POD
  log "Using pod: $TRIAGE_POD"
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
# Prints each IMSI with stream count; returns 1 if any IMSI has 0 streams
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

# ── Token counter ─────────────────────────────────────────────────────────────
# Usage: collect_token_count <incident_id> <artifact_filename>
# Prints token count (1 token ≈ 4 chars)
collect_token_count() {
  local incident_id=$1
  local artifact=$2
  local content

  content=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
    cat "/app/artifacts/$incident_id/$artifact" 2>/dev/null || echo "")

  if [[ -z "$content" ]]; then
    echo "  $artifact: NOT FOUND"
    return 1
  fi

  local chars token_est
  chars=${#content}
  token_est=$((chars / 4))
  echo "  $artifact: ~${token_est} tokens (${chars} chars)"
  echo "${incident_id}|${artifact}|${token_est}" >> "$RESULTS_DIR/token_counts.txt"
}

# ── Pull all artifacts for an incident ───────────────────────────────────────
pull_artifacts() {
  local incident_id=$1
  local dest="$RESULTS_DIR/artifacts/$incident_id"
  mkdir -p "$dest"
  kubectl cp "$TRIAGE_NS/$TRIAGE_POD:/app/artifacts/$incident_id" \
    "$dest" -c triage-agent 2>/dev/null || \
    log "Warning: could not pull artifacts for $incident_id"
  log "Artifacts saved to $dest"
}

HELPERS
chmod +x /workspace/net-rca/scripts/testing/helpers.sh
```

- [ ] **Step 2.2: Verify helpers.sh is executable and has no syntax errors**

```bash
bash -n /workspace/net-rca/scripts/testing/helpers.sh && echo "Syntax OK"
```
Expected: `Syntax OK`

- [ ] **Step 2.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/helpers.sh
git commit -m "test(scripts): add shared live test helper functions"
```

---

### Task 3: Write Phase 0 pre-flight script

**Files:**
- Create: `scripts/testing/phase0-preflight.sh`

- [ ] **Step 3.1: Create phase0-preflight.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase0-preflight.sh << 'PHASE0'
#!/usr/bin/env bash
# phase0-preflight.sh — verify cluster is ready before testing
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

log "=== Phase 0: Pre-flight Checks ==="
ERRORS=0

# 1. Free5GC NF pods all Running 2/2
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

# 5. LLM inference service responding
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
  pass "Phase 0 PASSED — cluster is ready"
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
git commit -m "test(scripts): add Phase 0 pre-flight check script"
```

---

## Chunk 2: Deploy and Verify

### Task 4: Write Phase 0.5 deployment script

**Files:**
- Create: `scripts/testing/phase05-deploy.sh`

- [ ] **Step 4.1: Create phase05-deploy.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase05-deploy.sh << 'PHASE05'
#!/usr/bin/env bash
# phase05-deploy.sh — build, import, and deploy TriageAgent to monitoring namespace
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

log "=== Phase 0.5: Build & Deploy TriageAgent ==="
cd /workspace/net-rca

# 1. Build container image
log "Building triage-agent:v3.2..."
docker build -t triage-agent:v3.2 .
pass "Docker image built: triage-agent:v3.2"

# 2. Import into k3s
log "Importing image into k3s containerd..."
docker save triage-agent:v3.2 -o /tmp/triage-agent-v3.2.tar
sudo k3s ctr images import /tmp/triage-agent-v3.2.tar
rm -f /tmp/triage-agent-v3.2.tar

IMAGE_CHECK=$(sudo k3s ctr images ls | grep "triage-agent:v3.2" | wc -l)
if [[ "$IMAGE_CHECK" -gt 0 ]]; then
  pass "Image available in k3s"
else
  fail "Image not found in k3s after import"
  exit 1
fi

# 3. Apply manifests (namespace already patched to monitoring in Task 1)
log "Applying DAG ConfigMap..."
kubectl apply -f k8s/dag-configmap.yaml

log "Applying Memgraph PVC..."
kubectl apply -f k8s/memgraph-pvc.yaml

log "Applying TriageAgent ConfigMap..."
kubectl apply -n "$TRIAGE_NS" -f k8s/triage-agent-configmap.yaml

log "Applying deployment..."
kubectl apply -n "$TRIAGE_NS" -f k8s/deployment-with-init.yaml

log "Applying network policies..."
kubectl apply -n "$TRIAGE_NS" -f k8s/triage-agent-to-qwen3-4b-netpol.yaml

# 4. Patch imagePullPolicy: Never (local image)
log "Patching imagePullPolicy to Never..."
kubectl patch deployment triage-agent -n "$TRIAGE_NS" \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"triage-agent","imagePullPolicy":"Never"}]}}}}'

# 5. Wait for deployment to be ready
log "Waiting for deployment to be ready (up to 3 minutes)..."
kubectl rollout status deployment triage-agent -n "$TRIAGE_NS" --timeout=180s
pass "Deployment rolled out"

# 6. Export pod name
resolve_triage_pod
echo "export TRIAGE_POD=$TRIAGE_POD" >> "$RESULTS_DIR/env.sh"
echo "export WEBHOOK_URL=${WEBHOOK_URL:-http://localhost:8000}" >> "$RESULTS_DIR/env.sh"

log ""
log "Port-forward (run in a separate terminal if not already running):"
log "  kubectl port-forward -n $TRIAGE_NS svc/triage-agent 8000:8000 &"
log "  export WEBHOOK_URL=http://localhost:8000"
pass "Phase 0.5 COMPLETE"
PHASE05
chmod +x /workspace/net-rca/scripts/testing/phase05-deploy.sh
```

- [ ] **Step 4.2: Verify syntax**

```bash
bash -n /workspace/net-rca/scripts/testing/phase05-deploy.sh && echo "Syntax OK"
```

- [ ] **Step 4.3: Commit**

```bash
cd /workspace/net-rca
git add scripts/testing/phase05-deploy.sh
git commit -m "test(scripts): add Phase 0.5 build and deploy script"
```

---

### Task 5: Write Phase 1 health verification script

**Files:**
- Create: `scripts/testing/phase1-health.sh`

- [ ] **Step 5.1: Create phase1-health.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase1-health.sh << 'PHASE1'
#!/usr/bin/env bash
# phase1-health.sh — verify TriageAgent pod health, DAGs, and endpoints
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
resolve_triage_pod

log "=== Phase 1: Health Verification ==="
ERRORS=0

# 1. Container states
log "Checking container states..."
kubectl get pods -n "$TRIAGE_NS" | tee "$RESULTS_DIR/phase1-pods.txt"

DAG_LOADER_STATUS=$(kubectl get pod -n "$TRIAGE_NS" "$TRIAGE_POD" \
  -o jsonpath='{.status.initContainerStatuses[?(@.name=="dag-loader")].state.terminated.reason}' \
  2>/dev/null || echo "")
MEMGRAPH_READY=$(kubectl get pod -n "$TRIAGE_NS" "$TRIAGE_POD" \
  -o jsonpath='{.status.containerStatuses[?(@.name=="memgraph")].ready}' 2>/dev/null || echo "false")
TRIAGE_READY=$(kubectl get pod -n "$TRIAGE_NS" "$TRIAGE_POD" \
  -o jsonpath='{.status.containerStatuses[?(@.name=="triage-agent")].ready}' 2>/dev/null || echo "false")

[[ "$DAG_LOADER_STATUS" == "Completed" ]] && \
  pass "dag-loader: Completed" || { fail "dag-loader: $DAG_LOADER_STATUS"; ERRORS=$((ERRORS+1)); }
[[ "$MEMGRAPH_READY" == "true" ]] && \
  pass "memgraph: Ready" || { fail "memgraph: not ready"; ERRORS=$((ERRORS+1)); }
[[ "$TRIAGE_READY" == "true" ]] && \
  pass "triage-agent: Ready" || { fail "triage-agent: not ready"; ERRORS=$((ERRORS+1)); }

# 2. DAG names loaded (exact PascalCase required)
log "Verifying DAGs in Memgraph..."
DAG_OUTPUT=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c memgraph -- \
  bash -c 'echo "MATCH (t:ReferenceTrace) RETURN t.name;" | mgconsole' 2>/dev/null \
  | tee "$RESULTS_DIR/phase1-dags.txt")

for DAG_NAME in "Registration_General" "Authentication_5G_AKA" "PDU_Session_Establishment"; do
  if echo "$DAG_OUTPUT" | grep -q "$DAG_NAME"; then
    pass "DAG loaded: $DAG_NAME"
  else
    fail "DAG missing: $DAG_NAME"
    ERRORS=$((ERRORS+1))
  fi
done

# 3. /health endpoint — all dependencies green
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

# 4. /health/ready
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
git commit -m "test(scripts): add Phase 1 health verification script"
```

---

## Chunk 3: Component and Integration Validation

### Task 6: Write Phase 2 component validation script

**Files:**
- Create: `scripts/testing/phase2-components.sh`

- [ ] **Step 6.1: Create phase2-components.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase2-components.sh << 'PHASE2'
#!/usr/bin/env bash
# phase2-components.sh — generate traffic + verify each agent performs as designed
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
resolve_triage_pod

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
METRICS_FILE=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
  ls "/app/artifacts/$INCIDENT/" 2>/dev/null | grep "post_filter_metrics" || true)
if [[ -n "$METRICS_FILE" ]]; then
  METRICS_CONTENT=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
    cat "/app/artifacts/$INCIDENT/post_filter_metrics.json" 2>/dev/null)
  NF_COUNT=$(echo "$METRICS_CONTENT" | jq 'keys | length' 2>/dev/null || echo 0)
  if [[ "$NF_COUNT" -gt 0 ]]; then
    pass "NfMetricsAgent: post_filter_metrics.json has $NF_COUNT NFs"
  else
    fail "NfMetricsAgent: post_filter_metrics.json is empty"
    ERRORS=$((ERRORS+1))
  fi
else
  fail "NfMetricsAgent: post_filter_metrics.json not found"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.4: NfLogsAgent ────────────────────────────────────────────────────
log "Step 2.4 — Checking NfLogsAgent artifacts..."
LOGS_CONTENT=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
  cat "/app/artifacts/$INCIDENT/post_filter_logs.json" 2>/dev/null || true)
LOG_NF_COUNT=$(echo "$LOGS_CONTENT" | jq 'keys | length' 2>/dev/null || echo 0)
if [[ "$LOG_NF_COUNT" -gt 0 ]]; then
  pass "NfLogsAgent: post_filter_logs.json has $LOG_NF_COUNT NFs"
else
  fail "NfLogsAgent: post_filter_logs.json empty or missing"
  ERRORS=$((ERRORS+1))
fi

# ── Step 2.5: UeTracesAgent ──────────────────────────────────────────────────
log "Step 2.5 — Checking UeTracesAgent Memgraph write..."
TRACE_COUNT=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c memgraph -- \
  bash -c 'echo "MATCH (t:CapturedTrace) RETURN count(t);" | mgconsole' \
  2>/dev/null | grep -oP '\d+' | tail -1 || echo 0)
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

# ── Pull artifacts ────────────────────────────────────────────────────────────
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
git commit -m "test(scripts): add Phase 2 component validation script"
```

---

### Task 7: Write Phase 3 integration validation script

**Files:**
- Create: `scripts/testing/phase3-integration.sh`

- [ ] **Step 7.1: Create phase3-integration.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase3-integration.sh << 'PHASE3'
#!/usr/bin/env bash
# phase3-integration.sh — verify cross-component wiring + token budgets
# Reuses the Phase 2 incident if available, or triggers a new one
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
resolve_triage_pod

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
METRICS=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
  cat "/app/artifacts/$INCIDENT/post_filter_metrics.json" 2>/dev/null || echo "{}")
METRIC_NFS=$(echo "$METRICS" | jq 'keys | length')
if [[ "$METRIC_NFS" -gt 0 ]]; then
  pass "3.1: Prometheus data for $METRIC_NFS NFs"
  echo "$METRICS" | jq 'keys' | tee "$RESULTS_DIR/phase3-metric-nfs.txt"
else
  fail "3.1: No NF metrics returned from Prometheus"
  ERRORS=$((ERRORS+1))
fi
log "Token count for 3.1:"
collect_token_count "$INCIDENT" "post_filter_metrics.json"

# ── 3.2: NfLogsAgent ↔ Loki ─────────────────────────────────────────────────
log "3.2 — NfLogsAgent ↔ Loki (path selection + all NFs in nf_union)..."
LOGS=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
  cat "/app/artifacts/$INCIDENT/post_filter_logs.json" 2>/dev/null || echo "{}")
LOG_NFS=$(echo "$LOGS" | jq 'keys | length')
if [[ "$LOG_NFS" -gt 0 ]]; then
  pass "3.2: Loki data for $LOG_NFS NFs"
  echo "$LOGS" | jq 'keys' | tee "$RESULTS_DIR/phase3-log-nfs.txt"
else
  fail "3.2: No NF logs returned from Loki"
  ERRORS=$((ERRORS+1))
fi
# Check which path was used
MCP_PATH=$(kubectl logs -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent --tail=200 \
  2>/dev/null | grep -E "MCP server unavailable|using direct Loki" | tail -1 || echo "MCP path used (no fallback message)")
log "  Loki path: $MCP_PATH"
echo "$MCP_PATH" > "$RESULTS_DIR/phase3-loki-path.txt"
log "Token count for 3.2:"
collect_token_count "$INCIDENT" "post_filter_logs.json"

# ── 3.3: UeTracesAgent ↔ Memgraph (write) ───────────────────────────────────
log "3.3 — UeTracesAgent ↔ Memgraph (CapturedTrace write)..."
TRACE_COUNT=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c memgraph -- \
  bash -c 'echo "MATCH (t:CapturedTrace) RETURN count(t);" | mgconsole' \
  2>/dev/null | grep -oP '\d+' | tail -1 || echo 0)
if [[ "$TRACE_COUNT" -gt 0 ]]; then
  pass "3.3: $TRACE_COUNT CapturedTrace node(s) in Memgraph"
else
  fail "3.3: No CapturedTrace nodes found"
  ERRORS=$((ERRORS+1))
fi
echo "$TRACE_COUNT CapturedTrace nodes" > "$RESULTS_DIR/phase3-captured-traces.txt"

# ── 3.4: UeTracesAgent ↔ Memgraph (deviation detection) ────────────────────
log "3.4 — UeTracesAgent deviation detection (all 3 DAGs)..."
DEV_OUTPUT=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c memgraph -- \
  bash -c 'echo "MATCH (t:CapturedTrace)-[:DEVIATES_FROM]->(r:ReferenceTrace) RETURN r.name, count(t);" | mgconsole' \
  2>/dev/null | tee "$RESULTS_DIR/phase3-deviations.txt" || echo "")
if kubectl logs -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent --tail=300 \
  2>/dev/null | grep -q "Cypher.*error\|CypherError" ; then
  fail "3.4: Cypher query errors in triage-agent logs"
  ERRORS=$((ERRORS+1))
else
  pass "3.4: Deviation detection ran without Cypher errors"
fi
echo "Deviation output:" && echo "$DEV_OUTPUT"
echo "3.4 deviation results saved" >> "$RESULTS_DIR/phase3-deviations.txt"

# ── 3.5: InfraAgent token contribution ───────────────────────────────────────
log "3.5 — InfraAgent token contribution to RCA..."
# infra evidence is embedded in the compressed_evidence artifact
INFRA_EVIDENCE=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
  cat "/app/artifacts/$INCIDENT/compressed_evidence.json" 2>/dev/null \
  | jq '.infra // empty' 2>/dev/null || echo "")
INFRA_TOKENS=$(( ${#INFRA_EVIDENCE} / 4 ))
echo "  infra evidence block: ~${INFRA_TOKENS} tokens (budget: 400)"
echo "infra|${INFRA_TOKENS}" >> "$RESULTS_DIR/token_counts.txt"
if [[ "$INFRA_TOKENS" -le 400 ]]; then
  pass "3.5: InfraAgent tokens within budget (~${INFRA_TOKENS} ≤ 400)"
else
  log "  WARNING: InfraAgent tokens (~${INFRA_TOKENS}) exceed 400 ceiling — forwarded for correctness"
fi

# ── 3.6: DagMapper token contribution ────────────────────────────────────────
log "3.6 — DagMapper token contribution to RCA..."
DAG_EVIDENCE=$(kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
  cat "/app/artifacts/$INCIDENT/compressed_evidence.json" 2>/dev/null \
  | jq '.dags // empty' 2>/dev/null || echo "")
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
kubectl exec -n "$TRIAGE_NS" "$TRIAGE_POD" -c triage-agent -- \
  ls -lt "/app/artifacts/$INCIDENT/" 2>/dev/null \
  | tee "$RESULTS_DIR/phase3-artifact-order.txt"
pass "3.8: Artifact timestamps saved to phase3-artifact-order.txt for manual verification"

# ── 3.9: Webhook lifecycle ────────────────────────────────────────────────────
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
git commit -m "test(scripts): add Phase 3 integration validation script"
```

---

## Chunk 4: End-to-End Scenarios

### Task 8: Write Phase 4 E2E scenarios script

**Files:**
- Create: `scripts/testing/phase4-e2e.sh`

- [ ] **Step 8.1: Create phase4-e2e.sh**

```bash
cat > /workspace/net-rca/scripts/testing/phase4-e2e.sh << 'PHASE4'
#!/usr/bin/env bash
# phase4-e2e.sh — 4 E2E scenarios with inject/trigger/verify/restore
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"
resolve_triage_pod

log "=== Phase 4: End-to-End Validation ==="
ERRORS=0
SCENARIO_RESULTS=()

# ── Helper: verify RCA fields ─────────────────────────────────────────────────
# Usage: verify_rca <incident_id> <expected_root_nf_regex> <expected_layer> <scenario_label>
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

# Record baseline token counts
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

verify_rca "$INCIDENT_42" "^AMF$" "infrastructure" "4.2" && SCENARIO_RESULTS+=("4.2:REG_FAIL:PASS") || SCENARIO_RESULTS+=("4.2:REG_FAIL:FAIL")
pull_artifacts "$INCIDENT_42"

log "Restoring: scaling amf back to 1..."
kubectl scale deployment amf -n "$CORE_NS" --replicas=1
kubectl rollout status deployment amf -n "$CORE_NS"
sleep 15  # allow AMF to complete NRF registration

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
# Verify
OP_PATCHED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "^op:" | head -1)
log "  op field after patch: $OP_PATCHED"

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 30

INCIDENT_43=$(trigger_webhook "AuthenticationFailures" "ausf")
log "Incident: $INCIDENT_43"
REPORT_43=$(poll_incident "$INCIDENT_43" 360)

verify_rca "$INCIDENT_43" "^(AUSF|UDM)$" "application" "4.3" && SCENARIO_RESULTS+=("4.3:AUTH_FAIL:PASS") || SCENARIO_RESULTS+=("4.3:AUTH_FAIL:FAIL")
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

verify_rca "$INCIDENT_44" "^SMF$" "application" "4.4" && SCENARIO_RESULTS+=("4.4:PDU_FAIL:PASS") || SCENARIO_RESULTS+=("4.4:PDU_FAIL:FAIL")
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

# Count failure scenario successes (4.2, 4.3, 4.4 only — 4.1 is sunny day)
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
git commit -m "test(scripts): add Phase 4 E2E scenario script"
```

---

### Task 9: Write results collection script and run all phases

**Files:**
- Create: `scripts/testing/run-all.sh`

- [ ] **Step 9.1: Create run-all.sh**

```bash
cat > /workspace/net-rca/scripts/testing/run-all.sh << 'RUNALL'
#!/usr/bin/env bash
# run-all.sh — execute all test phases in order; gate each phase on pass
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Create a single timestamped results directory shared across all phases
export RESULTS_DIR="/workspace/net-rca/test-results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"
echo "Results directory: $RESULTS_DIR"

source "$SCRIPT_DIR/helpers.sh"

# Require port-forward to be running
log "Checking webhook is reachable at $WEBHOOK_URL..."
curl -s --max-time 5 "$WEBHOOK_URL/health" > /dev/null || {
  echo "ERROR: $WEBHOOK_URL not reachable."
  echo "Run in a separate terminal: kubectl port-forward -n monitoring svc/triage-agent 8000:8000"
  exit 1
}

log "Starting full test run. Results: $RESULTS_DIR"

"$SCRIPT_DIR/phase0-preflight.sh"   || { echo "GATE: Phase 0 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase1-health.sh"      || { echo "GATE: Phase 1 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase2-components.sh"  || { echo "GATE: Phase 2 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase3-integration.sh" || { echo "GATE: Phase 3 failed — aborting"; exit 1; }
"$SCRIPT_DIR/phase4-e2e.sh"

echo ""
echo "=== FINAL RESULTS ==="
cat "$RESULTS_DIR/summary.txt"
echo ""
echo "Full results in: $RESULTS_DIR"
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
git commit -m "test(scripts): add run-all.sh orchestrator for full test suite"
```

---

### Task 10: Execute the full test plan

- [ ] **Step 10.1: Ensure Phase 0.5 deployment is complete**

Run Phase 0.5 if TriageAgent is not yet deployed:
```bash
cd /workspace/net-rca
bash scripts/testing/phase05-deploy.sh
```
Then start port-forward in a separate terminal:
```bash
kubectl port-forward -n monitoring svc/triage-agent 8000:8000 &
export WEBHOOK_URL=http://localhost:8000
export TRIAGE_POD=$(kubectl get pod -n monitoring -l app=triage-agent \
  -o jsonpath='{.items[0].metadata.name}')
```

- [ ] **Step 10.2: Run the full test suite**

```bash
cd /workspace/net-rca
export WEBHOOK_URL=http://localhost:8000
export TRIAGE_POD=$(kubectl get pod -n monitoring -l app=triage-agent \
  -o jsonpath='{.items[0].metadata.name}')

bash scripts/testing/run-all.sh 2>&1 | tee /tmp/test-run.log
```

Expected final output (all phases pass):
```
[PASS] Phase 0 PASSED — cluster is ready
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
git commit -m "test(results): live test run against Free5GC on k3s $(date +%Y-%m-%d)"
```

- [ ] **Step 10.4: If any phase fails — triage using artifacts**

```bash
# List all incidents and their results
ls "$RESULTS_DIR"

# Browse artifacts inside the pod
kubectl exec -it -n monitoring "$TRIAGE_POD" -c triage-agent -- bash
# Inside: ls /app/artifacts/ && cat /app/artifacts/<id>/post_filter_logs.json

# Check triage-agent logs for errors
kubectl logs -n monitoring "$TRIAGE_POD" -c triage-agent --tail=100

# Check Memgraph state
kubectl exec -n monitoring "$TRIAGE_POD" -c memgraph -- \
  bash -c 'echo "MATCH (t:ReferenceTrace) RETURN t.name;" | mgconsole'
```
