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
