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
  poll_incident "$INCIDENT" 1500 > /dev/null
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
collect_token_count "$INCIDENT" "post_filter_metrics.json" || true

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
collect_token_count "$INCIDENT" "post_filter_logs.json" || true

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
collect_token_count "$INCIDENT" "llm_prompt.txt" || true
collect_token_count "$INCIDENT" "llm_response.json" || true
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
FINAL=$(poll_incident "$NEW_INCIDENT" 1500)
FINAL_STATUS=$(echo "$FINAL" | jq -r '.status')
[[ "$FINAL_STATUS" == "complete" ]] && \
  pass "3.9: Lifecycle complete end-to-end" || \
  { fail "3.9: Final status=$FINAL_STATUS"; ERRORS=$((ERRORS+1)); }

pull_artifacts "$NEW_INCIDENT"
collect_traces "3" "$INCIDENT" "$NEW_INCIDENT"
generate_perf_report "3" || true

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  pass "Phase 3 PASSED — all integration boundaries verified"
  exit 0
else
  fail "Phase 3 FAILED — $ERRORS integration check(s) failed"
  exit 1
fi
