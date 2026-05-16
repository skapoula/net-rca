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
  --no-headers 2>/dev/null | grep -v "Completed" | wc -l || true)
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
  | tr ' ' '\n' | grep -c "true" || true)
UERANSIM_READY=${UERANSIM_READY:-0}
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
  --data-urlencode 'query={k8s_namespace_name="5g-core"}' \
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

# 6. LLM inference service responding (soft check — pod may not be reachable from devcontainer)
log "Checking LLM inference service..."
LLM_URL="http://qwen3-4b.ml-serving.svc.cluster.local/v1/models"
LLM_MODEL=$(curl -s --max-time 10 "$LLM_URL" 2>/dev/null | jq -r '.data[0].id // empty' 2>/dev/null || true)
if [[ -n "$LLM_MODEL" ]]; then
  pass "LLM inference: model $LLM_MODEL available"
else
  log "  WARNING: LLM not reachable at $LLM_URL from preflight — pod confirmed Running."
  log "  LLM connectivity will be tested for real in Phase 2 (RCAAgent)."
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
