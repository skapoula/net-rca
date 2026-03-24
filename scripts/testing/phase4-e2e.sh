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
  local conf_ok; conf_ok=$(python3 -c "print(1 if float('${confidence:-0}') >= 0.70 else 0)" 2>/dev/null || echo 0)
  [[ "$conf_ok" -eq 1 ]] || { fail "$label: confidence=$confidence < 0.70"; ok=false; }
  local eq_ok; eq_ok=$(python3 -c "print(1 if float('${eq_score:-0}') >= 0.50 else 0)" 2>/dev/null || echo 0)
  [[ "$eq_ok" -eq 1 ]] || { fail "$label: evidence_quality=$eq_score < 0.50"; ok=false; }

  $ok && { pass "$label PASSED (root_nf=$root_nf layer=$layer confidence=$confidence)"; return 0; }
  ERRORS=$((ERRORS+1)); return 1
}

# ── 4.1: Sunny Day ────────────────────────────────────────────────────────────
log ""
log "=== Scenario 4.1: Sunny Day ==="
INCIDENT_41=$(trigger_webhook "RegistrationFailures" "amf" "warning")
log "Incident: $INCIDENT_41"
REPORT_41=$(poll_incident "$INCIDENT_41" 1500)

INFRA_41=$(echo "$REPORT_41" | jq -r '.final_report.infra_score // 1')
INFRA_OK=$(python3 -c "print(1 if float('${INFRA_41:-1}') < 0.3 else 0)" 2>/dev/null || echo 0)
[[ "$INFRA_OK" -eq 1 ]] && pass "4.1: infra_score=$INFRA_41 < 0.3 (no false positive)" \
  || fail "4.1: infra_score=$INFRA_41 ≥ 0.3 — possible false positive"

FAIL_41=$(echo "$REPORT_41" | jq -r '.final_report.failure_mode // ""')
[[ "$FAIL_41" != "llm_timeout" ]] && pass "4.1: LLM responded without timeout" \
  || { fail "4.1: llm_timeout"; ERRORS=$((ERRORS+1)); }

log "Recording baseline token counts..."
for artifact in pre_filter_metrics.json post_filter_metrics.json \
                pre_filter_logs.json post_filter_logs.json; do
  collect_token_count "$INCIDENT_41" "$artifact" || true
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
REPORT_42=$(poll_incident "$INCIDENT_42" 1500)

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

log "Restoring known-good ue-config state before backup (op key + APN) ..."
kubectl get configmap ue-config -n "$CORE_NS" -o yaml \
  | sed -e "s/op: '00000000000000000000000000000000'/op: '8e27b6af0e692e750f32667a3b14605d'/" \
        -e "s/apn: 'invalid-internet'/apn: 'internet'/" \
  | kubectl apply -f - 2>/dev/null || true
OP_PRE=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "^op:" | head -1)
APN_PRE=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "apn:" | head -1)
log "  op field before backup: $OP_PRE"
log "  APN before backup: $APN_PRE"

log "Restarting UERANSIM with clean config so SQN re-syncs before backup..."
kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 60
check_imsi_loki 3 || log "WARNING: not all IMSIs visible after pre-4.3 cleanup"

log "Backing up ue-config (stripping resourceVersion/uid so restore works)..."
kubectl get configmap ue-config -n "$CORE_NS" -o yaml \
  | grep -v '^\s*resourceVersion:\|^\s*uid:\|^\s*creationTimestamp:' \
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
REPORT_43=$(poll_incident "$INCIDENT_43" 1500)

verify_rca "$INCIDENT_43" "^(AUSF|UDM|AMF)$" "application" "4.3" \
  && SCENARIO_RESULTS+=("4.3:AUTH_FAIL:PASS") || SCENARIO_RESULTS+=("4.3:AUTH_FAIL:FAIL")
pull_artifacts "$INCIDENT_43"

log "Restoring: applying ue-config backup..."
kubectl apply -f "$RESULTS_DIR/ue-config-backup.yaml" \
  || log "WARNING: ue-config restore failed; continuing"
OP_RESTORED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "^op:" | head -1)
APN_RESTORED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "apn:" | head -1)
log "  op field after restore: $OP_RESTORED"
log "  APN after restore: $APN_RESTORED"

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"
sleep 30
log "Confirming UEs re-registered after restore..."
check_imsi_loki 3 || log "WARNING: not all IMSIs visible after restore"

# Wait for 4.3 auth-failure logs to age out of the 5-min lookback window
# (alert_lookback_seconds=300) so they don't pollute the 4.4 analysis.
log "Waiting 360s for 4.3 auth-failure logs to clear the lookback window..."
sleep 360

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
# 120s sleep: gives UERANSIM time to register (auth ~30s) then start failing PDU sessions
# so "Select SMF failed: DNN[invalid-internet]" logs appear before the alert fires and
# fall inside the 5-min lookback window.  With 30s the PDU errors arrived after window end.
sleep 120

INCIDENT_44=$(trigger_webhook "PDUSessionFailures" "smf")
log "Incident: $INCIDENT_44"
REPORT_44=$(poll_incident "$INCIDENT_44" 1500)

# In open5GS, AMF rejects PDU sessions with unsupported DNN before contacting SMF.
# The error "Select SMF failed: DNN[invalid-internet] is not supported" is logged at AMF.
# Accept both AMF and SMF as valid root NFs for PDU session failure.
verify_rca "$INCIDENT_44" "^(AMF|SMF)$" "application" "4.4" \
  && SCENARIO_RESULTS+=("4.4:PDU_FAIL:PASS") || SCENARIO_RESULTS+=("4.4:PDU_FAIL:FAIL")
pull_artifacts "$INCIDENT_44"

log "Restoring: applying ue-config backup (reuse from 4.3)..."
kubectl apply -f "$RESULTS_DIR/ue-config-backup.yaml"
APN_RESTORED=$(kubectl get configmap ue-config -n "$CORE_NS" \
  -o jsonpath='{.data.ue-base\.yaml}' | grep "apn:" | head -1)
log "  APN after restore: $APN_RESTORED"

kubectl rollout restart deployment ueransim -n "$CORE_NS"
kubectl rollout status deployment ueransim -n "$CORE_NS"

collect_traces "4" "$INCIDENT_41" "$INCIDENT_42" "$INCIDENT_43" "$INCIDENT_44"

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
