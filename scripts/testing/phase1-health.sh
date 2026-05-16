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

# 2. Port 8000 responding
log "Checking port 8000 is responding..."
if curl -s --max-time 3 "http://localhost:8000/health" > /dev/null 2>&1; then
  pass "Port 8000 is responding"
else
  fail "Port 8000 is not responding — TriageAgent may not be listening"
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

# 5. /health status=healthy (readiness confirmation)
log "Checking /health reports status=healthy..."
READY_STATUS=$(curl -s "$WEBHOOK_URL/health" | jq -r '.status // "unknown"')
[[ "$READY_STATUS" == "healthy" ]] && \
  pass "/health: status=healthy (ready)" || { fail "/health: status=$READY_STATUS (not ready)"; ERRORS=$((ERRORS+1)); }

# Summary
echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 1 PASSED — TriageAgent healthy and ready"
  exit 0
else
  fail "Phase 1 FAILED — $ERRORS check(s) failed"
  exit 1
fi
