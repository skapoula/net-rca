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

log "Polling for completion (up to 1500s)..."
REPORT=$(poll_incident "$INCIDENT" 1500) || { fail "Incident did not complete"; exit 1; }

# ── Step 2.1: DagMapper ───────────────────────────────────────────────────────
log "Step 2.1 — Checking DagMapper output..."
MAPPING_CONF=$(echo "$REPORT" | jq -r '.final_report.mapping_confidence // empty')
PROC_NAMES=$(echo "$REPORT" | jq -r '.final_report.procedure_names // [] | @json')
MAP_OK=$(python3 -c "print(1 if float('${MAPPING_CONF:-0}') >= 0.7 else 0)" 2>/dev/null || echo 0)
if [[ "$MAP_OK" -eq 1 ]] && echo "$PROC_NAMES" | grep -q "Registration_General"; then
  pass "DagMapper: mapping_confidence=$MAPPING_CONF (≥0.7), Registration_General mapped"
else
  fail "DagMapper: mapping_confidence=$MAPPING_CONF (<0.7), procedures=$PROC_NAMES"
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
EQ_OK=$(python3 -c "print(1 if float('${EQ_SCORE:-0}') >= 0.50 else 0)" 2>/dev/null || echo 0)
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
collect_traces "2" "$INCIDENT"

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 2 PASSED — all 7 components perform as designed"
  exit 0
else
  fail "Phase 2 FAILED — $ERRORS component check(s) failed"
  exit 1
fi
