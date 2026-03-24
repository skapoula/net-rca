#!/usr/bin/env bash
# phase05-start-groq.sh — load DAGs into local Memgraph + start TriageAgent with Groq inference
# Groq variant of phase05-start-local.sh.  Sets LLM_PROVIDER=groq and validates
# GROQ_API_KEY before starting uvicorn.  No LLM_BASE_URL needed for cloud inference.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

log "=== Phase 0.5: Load DAGs + Start TriageAgent (Groq variant) ==="
cd /workspace/net-rca

# Load GROQ_API_KEY from .env if not already set in the environment
ENV_FILE="$(cd "$SCRIPT_DIR/../.." && pwd)/.env"
if [[ -z "${GROQ_API_KEY:-}" ]] && [[ -f "$ENV_FILE" ]]; then
  GROQ_API_KEY=$(grep -E '^GROQ_API_KEY=' "$ENV_FILE" | cut -d= -f2- | tr -d '[:space:]')
  export GROQ_API_KEY
fi

# 0. Validate GROQ_API_KEY is available
GROQ_KEY="${GROQ_API_KEY:-}"
if [[ -z "$GROQ_KEY" ]]; then
  fail "GROQ_API_KEY is not set — add it to .env or export before running"
  exit 1
fi
log "GROQ_API_KEY is set (length ${#GROQ_KEY})"

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
if fuser 8000/tcp > /dev/null 2>&1; then
  log "Stopping existing process on port 8000..."
  fuser -k 8000/tcp 2>/dev/null || true
  sleep 2
fi

# 4. Start uvicorn in background with Groq provider
log "Starting TriageAgent with Groq inference (log: $TRIAGE_LOG)..."
mkdir -p "$ARTIFACTS_DIR"
export LLM_PROVIDER=groq
export LLM_MODEL="openai/gpt-oss-20b"
export GROQ_API_KEY="$GROQ_KEY"
export GROQ_REASONING_EFFORT="${GROQ_REASONING_EFFORT:-medium}"
export GROQ_MAX_TOKENS="${GROQ_MAX_TOKENS:-2048}"
export LANGCHAIN_TRACING_V2=true
log "LLM_PROVIDER=$LLM_PROVIDER  LLM_MODEL=$LLM_MODEL  GROQ_REASONING_EFFORT=$GROQ_REASONING_EFFORT"

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
pass "Phase 0.5 COMPLETE — TriageAgent running with Groq inference (PID $UVICORN_PID)"
