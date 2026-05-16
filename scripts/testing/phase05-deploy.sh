#!/usr/bin/env bash
# phase05-deploy.sh — build, import, and deploy TriageAgent to monitoring namespace
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

log "=== Phase 0.5: Build & Deploy TriageAgent ==="
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

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
kubectl delete deployment triage-agent -n "$TRIAGE_NS" --ignore-not-found
kubectl apply -n "$TRIAGE_NS" -f k8s/deployment-with-init.yaml

log "Applying network policies..."
kubectl apply -f k8s/triage-agent-to-qwen3-4b-netpol.yaml

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
