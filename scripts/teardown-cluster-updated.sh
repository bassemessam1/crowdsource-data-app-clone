#!/bin/bash
# ── Crowdsource data app GCP Clone — Session Teardown ────────────────────────
# Gracefully shuts down the GKE cluster to stop billing.
# Safe to run at the end of every learning session.
#
# What is PRESERVED:
#   - GCS buckets and all data
#   - BigQuery datasets and tables
#   - Terraform state (foundation + gke)
#   - VPC, IAM, service accounts, static IP
#   - GCP Secret Manager secrets
#   - Artifact Registry images
#   - GitHub repo and all code
#
# What is LOST (recreated by startup script):
#   - GKE cluster and all pods
#   - Helm releases (cert-manager, Strimzi, ESO, Prometheus)
#   - Kubernetes namespaces and their contents
#   - Kafka topic data in broker PVCs
#   - GCS Sink connector config (re-registered by startup script)
#
# Usage: bash scripts/teardown-cluster.sh

set -e

# ── Colours for output ────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No colour

log()  { echo -e "${BLUE}==>  $1${NC}"; }
ok()   { echo -e "${GREEN}  ✓  $1${NC}"; }
warn() { echo -e "${YELLOW}  !  $1${NC}"; }

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT_ID="crowdsource-data-app-clone"
REGION="europe-west2"
CLUSTER_NAME="crowdsource-data-app-gke"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo ""
echo -e "${RED}╔══════════════════════════════════=════════╗${NC}"
echo -e "${RED}║        CROWDSOURCE CLUSTER TEARDOWN       ║${NC}"
echo -e "${RED}╚══════════════════════════════════=════════╝${NC}"
echo ""
warn "This will destroy the GKE cluster and stop all billing."
warn "GCS data, BigQuery, IAM, and Terraform state are preserved."
echo ""
read -p "  Continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
  echo "  Teardown cancelled."
  exit 0
fi
echo ""

# ── Step 1: Reconnect kubectl ─────────────────────────────────────────────────
log "Step 1/5 — Connecting to cluster..."
gcloud container clusters get-credentials $CLUSTER_NAME \
  --region $REGION \
  --project $PROJECT_ID 2>/dev/null || {
  warn "Could not connect to cluster — may already be destroyed."
  warn "Skipping K8s cleanup steps."
  SKIP_K8S=true
}

if [ -z "$SKIP_K8S" ]; then
  ok "Connected to $CLUSTER_NAME"

  # ── Step 2: Phase 02 — Remove application workloads ──────────────────────
  log "Step 2/5 — Removing Phase 02 resources..."

  # Remove Simulator first (stops new events being generated)
  kubectl delete deployment measurement-simulator -n ingest-api \
    --timeout=60s 2>/dev/null || true
  ok "Simulator removed"

  # Remove Ingest API (stops accepting new events)
  kubectl delete deployment ingest-api -n ingest-api \
    --timeout=60s 2>/dev/null || true
  kubectl delete service ingest-api -n ingest-api \
    --timeout=30s 2>/dev/null || true
  kubectl delete service ingest-api-lb -n ingest-api \
    --timeout=30s 2>/dev/null || true
  ok "Ingest API removed"

  # Delete GCS Sink connector via REST API before killing the pod
  kubectl run connector-delete \
    --image=curlimages/curl \
    --namespace=kafka \
    --restart=Never \
    --rm \
    -- curl -s -X DELETE \
       http://kafka-connect:8083/connectors/gcs-sink-raw-measurements \
    2>/dev/null || true

  # Delete Kafka Connect deployment and service
  kubectl delete deployment kafka-connect -n kafka \
    --timeout=60s 2>/dev/null || true
  kubectl delete service kafka-connect -n kafka \
    --timeout=30s 2>/dev/null || true
  kubectl delete configmap kafka-connect-startup -n kafka \
    2>/dev/null || true
  # Delete gcs-credentials secret
  kubectl delete secret gcs-credentials -n kafka \
    2>/dev/null || true
  ok "Kafka Connect removed"

  # Delete Schema Registry
  kubectl delete deployment schema-registry -n kafka \
    --timeout=60s 2>/dev/null || true
  kubectl delete service schema-registry -n kafka \
    --timeout=30s 2>/dev/null || true
  ok "Schema Registry removed"

  # Delete topics first (fewest dependencies)
  kubectl delete kafkatopic --all -n kafka \
    --timeout=30s 2>/dev/null || true

  # Remove topic finalizers if still stuck
  kubectl get kafkatopic -n kafka -o name 2>/dev/null | \
    while read name; do
      kubectl patch $name -n kafka \
        --type='json' \
        -p='[{"op":"remove","path":"/metadata/finalizers"}]' \
        2>/dev/null || true
    done

  # Delete Kafka cluster and node pool
  kubectl delete kafka --all -n kafka \
    --timeout=60s 2>/dev/null || true
  kubectl delete kafkanodepool --all -n kafka \
    --timeout=60s 2>/dev/null || true

  # Remove Kafka finalizers if still stuck
  kubectl get kafka -n kafka -o name 2>/dev/null | \
    while read name; do
      kubectl patch $name -n kafka \
        --type='json' \
        -p='[{"op":"remove","path":"/metadata/finalizers"}]' \
        2>/dev/null || true
    done

  kubectl get kafkanodepool -n kafka -o name 2>/dev/null | \
    while read name; do
      kubectl patch $name -n kafka \
        --type='json' \
        -p='[{"op":"remove","path":"/metadata/finalizers"}]' \
        2>/dev/null || true
    done

  # Wait for Kafka pods to terminate
  log "  Waiting for Kafka pods to terminate..."
  kubectl wait pod --all \
    --for=delete \
    --timeout=120s \
    -n kafka 2>/dev/null || true

  # Delete PVCs — GCP Persistent Disks bill even after cluster is gone
  kubectl delete pvc --all -n kafka \
    --timeout=60s 2>/dev/null || true
  ok "Kafka cluster and PVCs removed"

  # Safety net — force remove namespace finalizers if anything is stuck
  REMAINING=$(kubectl get all -n kafka 2>/dev/null | \
    grep -v "^NAME\|strimzi-cluster-operator" | wc -l)
  if [ "$REMAINING" -gt "0" ]; then
    warn "Some resources remain in kafka namespace — forcing cleanup"
    kubectl patch namespace kafka \
      --type='json' \
      -p='[{"op":"remove","path":"/metadata/finalizers"}]' \
      2>/dev/null || true
  fi

  # ── Step 3: Delete kubectl manifests ────────────────────────────────────────
  log "Step 3/5 — Removing kubectl-managed resources..."

  kubectl delete clustersecretstore gcp-secret-manager \
    2>/dev/null && ok "Deleted ClusterSecretStore" || \
    warn "ClusterSecretStore not found — skipping"

  sleep 5
  ok "Resources cleaned up"

  # ── Step 4: Uninstall Helm releases (Strimzi LAST) ───────────────────────────
  log "Step 4/5 — Uninstalling Helm releases..."

  # Strimzi must be LAST — it processes CRD finalizers during deletion.
  # Removing it first leaves Kafka CRD finalizers stuck forever.
  for release_ns in \
    "kube-prometheus:monitoring" \
    "external-secrets:external-secrets" \
    "cert-manager:cert-manager" \
    "strimzi-operator:kafka"; do

    release="${release_ns%%:*}"
    namespace="${release_ns##*:}"

    if helm list -n "$namespace" 2>/dev/null | grep -q "$release"; then
      helm uninstall "$release" -n "$namespace" \
        --timeout 5m 2>/dev/null && \
        ok "Uninstalled $release from $namespace" || \
        warn "Could not uninstall $release — continuing"
    else
      warn "$release not found in $namespace — skipping"
    fi
  done

fi

# ── Step 5: Terraform destroy GKE module ─────────────────────────────────────
log "Step 5/5 — Destroying GKE cluster via Terraform..."
cd "$REPO_ROOT/terraform/gke"

terraform destroy -auto-approve
ok "GKE cluster destroyed"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           TEARDOWN COMPLETE              ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${GREEN}✓${NC}  GKE cluster destroyed — billing stopped"
echo -e "  ${GREEN}✓${NC}  Kafka PVCs deleted — no orphaned disks"
echo -e "  ${GREEN}✓${NC}  GCS data preserved"
echo -e "  ${GREEN}✓${NC}  BigQuery data preserved"
echo -e "  ${GREEN}✓${NC}  Terraform state preserved"
echo -e "  ${GREEN}✓${NC}  IAM + VPC preserved"
echo -e "  ${GREEN}✓${NC}  Artifact Registry images preserved"
echo ""
echo -e "  ${YELLOW}→${NC}  Run ${BLUE}bash scripts/startup-cluster.sh${NC} to resume tomorrow"
echo ""
