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

  log "Polling incident $incident_id (timeout ${timeout}s)..." >&2
  while [[ $elapsed -lt $timeout ]]; do
    result=$(curl -s "$WEBHOOK_URL/incidents/$incident_id")
    local status
    status=$(echo "$result" | jq -r '.status // "unknown"')

    if [[ "$status" == "complete" ]]; then
      echo "$result" | jq . | tee "$RESULTS_DIR/${incident_id}.json" >&2
      log "Incident $incident_id complete" >&2
      echo "$result"
      return 0
    fi

    log "Status: $status (${elapsed}s elapsed)" >&2
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
    imsi=$(printf "imsi-2089300000000%02d" "$i")
    local count
    count=$(curl -s \
      --data-urlencode "query={k8s_namespace_name=\"$CORE_NS\", k8s_pod_name=~\".*amf.*\"} |= \"$imsi\"" \
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

# ── Trace collector ───────────────────────────────────────────────────────────
# Usage: collect_traces <phase_label> <incident_id> [<incident_id> ...]
# Aggregates trace.json and trace-summary.txt from all given incidents into
# $RESULTS_DIR/traces-phase<N>.json and $RESULTS_DIR/traces-phase<N>-summary.txt
collect_traces() {
  local phase_label="$1"
  shift
  local incident_ids=("$@")
  local found=0
  local missing=0
  local out_json="$RESULTS_DIR/traces-phase${phase_label}.json"
  local out_summary="$RESULTS_DIR/traces-phase${phase_label}-summary.txt"
  local first_entry=true

  printf '[' > "$out_json"
  : > "$out_summary"

  for incident_id in "${incident_ids[@]}"; do
    local trace_file="$ARTIFACTS_DIR/${incident_id}/trace.json"
    local summary_file="$ARTIFACTS_DIR/${incident_id}/trace-summary.txt"

    if [[ ! -f "$trace_file" ]]; then
      log "collect_traces: trace.json not found for $incident_id — skipping"
      missing=$((missing + 1))
      continue
    fi
    if ! jq . "$trace_file" > /dev/null 2>&1; then
      log "collect_traces: trace.json invalid for $incident_id — skipping"
      missing=$((missing + 1))
      continue
    fi

    [[ "$first_entry" == "true" ]] && first_entry=false || printf ',' >> "$out_json"
    jq . "$trace_file" >> "$out_json"

    printf '=== Incident: %s ===\n' "$incident_id" >> "$out_summary"
    if [[ -f "$summary_file" ]]; then
      cat "$summary_file" >> "$out_summary"
    else
      printf '(trace-summary.txt not found)\n' >> "$out_summary"
    fi
    printf '\n' >> "$out_summary"

    found=$((found + 1))
  done

  printf ']' >> "$out_json"

  if [[ "$found" -gt 0 && "$missing" -eq 0 ]]; then
    pass "collect_traces phase${phase_label}: traces for all ${found} incident(s) → $(basename "$out_json")"
  elif [[ "$found" -gt 0 ]]; then
    pass "collect_traces phase${phase_label}: ${found} trace(s) collected, ${missing} missing (partial)"
  else
    fail "collect_traces phase${phase_label}: no trace files found"
  fi
  log "Traces: $out_json"
  log "Summary: $out_summary"
}

# ── Performance report generator ──────────────────────────────────────────────
# Usage: generate_perf_report <phase_label>
# Reads:  $RESULTS_DIR/traces-phase<N>.json  (written by collect_traces)
# Writes: $RESULTS_DIR/lang_perf-report-*.{json,md}
# Never fails the caller — always returns 0.
generate_perf_report() {
  local phase_label="$1"
  local traces_file="$RESULTS_DIR/traces-phase${phase_label}.json"
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local report_script="${script_dir}/../generate_perf_report.py"

  if [[ ! -f "$traces_file" ]]; then
    log "generate_perf_report: traces file not found: $traces_file — skipping"
    return 0
  fi
  if [[ ! -f "$report_script" ]]; then
    log "generate_perf_report: script not found: $report_script — skipping"
    return 0
  fi

  log "Generating LangSmith performance report for phase ${phase_label}..."
  if python3 "$report_script" \
      --traces "$traces_file" \
      --results-dir "$RESULTS_DIR" \
      --phase "$phase_label" 2>&1 | while IFS= read -r line; do log "  perf: $line"; done; then
    pass "generate_perf_report phase${phase_label}: reports written to $RESULTS_DIR"
  else
    log "WARNING: generate_perf_report phase${phase_label} exited non-zero (non-fatal)"
  fi
  return 0
}

# ── Memgraph query helper ─────────────────────────────────────────────────────
# Usage: mgquery <cypher_query>
# Returns: mgconsole output
mgquery() {
  local query="$1"
  echo "$query" | mgconsole -host "$MEMGRAPH_HOST" -port "$MEMGRAPH_PORT" 2>/dev/null
}
