#!/bin/bash
# ── Crowdsource data app GCP Clone — Session Teardown ───────────────────────────────────
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
  warn "Skipping Helm uninstall steps."
  SKIP_K8S=true
}

if [ -z "$SKIP_K8S" ]; then
  ok "Connected to $CLUSTER_NAME"

  # ── Step 2: Uninstall Helm releases ─────────────────────────────────────────
  log "Step 2/5 — Uninstalling Helm releases..."

  # Uninstall in reverse dependency order
  for release_ns in \
    "kube-prometheus:monitoring" \
    "strimzi-operator:kafka" \
    "external-secrets:external-secrets" \
    "cert-manager:cert-manager"; do

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

  # ── Step 3: Delete kubectl manifests ────────────────────────────────────────
  log "Step 3/5 — Removing kubectl-managed resources..."

  # ClusterSecretStore
  kubectl delete clustersecretstore gcp-secret-manager \
    2>/dev/null && ok "Deleted ClusterSecretStore" || \
    warn "ClusterSecretStore not found — skipping"

  # Wait for namespaces to drain
  sleep 10
  ok "Resources cleaned up"
fi

# log "Removing Kafka cluster resources..."

# # Delete Kafka CRD resources — entity operator cleans up internally
# kubectl delete kafka --all -n kafka \
#   --timeout=60s 2>/dev/null || true

# kubectl delete kafkanodepool --all -n kafka \
#   --timeout=60s 2>/dev/null || true

# kubectl delete kafkatopic --all -n kafka \
#   --timeout=30s 2>/dev/null || true

# # Wait for pods to terminate
# sleep 15

# # Delete PVCs — these are GCP Persistent Disks that bill even when cluster is down
# kubectl delete pvc --all -n kafka \
#   --timeout=60s 2>/dev/null || true

# ── Phase 02: Remove Kafka resources (with finalizer cleanup) ─────────────────
log "Removing Kafka cluster resources..."

# Step A — delete topics first (least dependencies)
kubectl delete kafkatopic --all -n kafka \
  --timeout=30s 2>/dev/null || true

# Step B — remove topic finalizers if still stuck
kubectl get kafkatopic -n kafka -o name 2>/dev/null | \
  while read name; do
    kubectl patch $name -n kafka \
      --type='json' \
      -p='[{"op":"remove","path":"/metadata/finalizers"}]' \
      2>/dev/null || true
  done

# Step C — delete Kafka cluster and node pool
kubectl delete kafka --all -n kafka \
  --timeout=60s 2>/dev/null || true
kubectl delete kafkanodepool --all -n kafka \
  --timeout=60s 2>/dev/null || true

# Step D — remove Kafka finalizers if still stuck
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

# Step E — delete PVCs
kubectl delete pvc --all -n kafka \
  --timeout=60s 2>/dev/null || true

# Step F — wait for pods to terminate
log "  Waiting for Kafka pods to terminate..."
kubectl wait pod --all \
  --for=delete \
  --timeout=120s \
  -n kafka 2>/dev/null || true

# Step G — remove schema registry
kubectl delete deployment schema-registry -n kafka \
  2>/dev/null || true
kubectl delete service schema-registry -n kafka \
  2>/dev/null || true

# Step H — verify namespace is clean before Terraform destroys it
REMAINING=$(kubectl get all -n kafka 2>/dev/null | \
  grep -v "^NAME\|strimzi-cluster-operator" | wc -l)

if [ "$REMAINING" -gt "0" ]; then
  warn "Some resources remain in kafka namespace — forcing namespace cleanup"
  kubectl patch namespace kafka \
    --type='json' \
    -p='[{"op":"remove","path":"/metadata/finalizers"}]' \
    2>/dev/null || true
fi

ok "Kafka resources removed"


# ── Step 4: Terraform destroy GKE module ─────────────────────────────────────
log "Step 4/5 — Destroying GKE cluster via Terraform..."
cd "$REPO_ROOT/terraform/gke"

terraform destroy -auto-approve
ok "GKE cluster destroyed"

# ── Step 5: Summary ───────────────────────────────────────────────────────────
log "Step 5/5 — Teardown summary"
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           TEARDOWN COMPLETE              ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${GREEN}✓${NC}  GKE cluster destroyed — billing stopped"
echo -e "  ${GREEN}✓${NC}  GCS data preserved"
echo -e "  ${GREEN}✓${NC}  BigQuery data preserved"
echo -e "  ${GREEN}✓${NC}  Terraform state preserved"
echo -e "  ${GREEN}✓${NC}  IAM + VPC preserved"
echo ""
echo -e "  ${YELLOW}→${NC}  Run ${BLUE}bash scripts/startup-cluster.sh${NC} to resume tomorrow"
echo ""
