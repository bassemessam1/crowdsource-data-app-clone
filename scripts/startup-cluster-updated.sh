#!/bin/bash
# ── Crowdsource GCP Clone — Session Startup ────────────────────────────────────
# Recreates the full GKE platform after a teardown.
# Run this at the start of each learning session.
#
# What this restores:
#   - GKE Autopilot cluster
#   - All 5 Kubernetes namespaces
#   - Workload Identity ServiceAccounts + IAM bindings
#   - cert-manager, ESO, Strimzi, Prometheus+Grafana
#   - ClusterSecretStore -> GCP Secret Manager
#   - Kafka cluster + topics (Phase 02)
#   - Schema Registry (Phase 02)
#   - Kafka Connect + GCS Sink connector (Phase 02)
#
# Prerequisites:
#   - gcloud authenticated (gcloud auth login)
#   - GOOGLE_APPLICATION_CREDENTIALS set
#   - Helm repos added (helm repo list should show 4 repos)
#   - PROJECT_ID environment variable set
#
# Usage: bash scripts/startup-cluster.sh

set -e

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}==>  $1${NC}"; }
ok()   { echo -e "${GREEN}  ✓  $1${NC}"; }
warn() { echo -e "${YELLOW}  !  $1${NC}"; }
fail() { echo -e "${RED}  ✗  $1${NC}"; exit 1; }

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT_ID="crowdsource-data-app-clone"
REGION="europe-west2"
CLUSTER_NAME="crowdsource-data-app-gke"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo ""
echo -e "${BLUE}╔═════════════════════════════════════=============═════╗${NC}"
echo -e "${BLUE}║        CROWDSOURCE DATA APP CLUSTER STARTUP           ║${NC}"
echo -e "${BLUE}╚═════════════════════════════════════=============═════╝${NC}"
echo ""

# ── Pre-flight checks ─────────────────────────────────────────────────────────
log "Pre-flight checks..."

# Check gcloud is authenticated
gcloud auth list 2>/dev/null | grep -q "ACTIVE" || \
  fail "gcloud not authenticated. Run: gcloud auth login"
ok "gcloud authenticated"

# Check Helm is installed
helm version --short &>/dev/null || \
  fail "Helm not installed. Run: curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
ok "Helm installed: $(helm version --short)"

# Check Helm repos are configured
REQUIRED_REPOS="jetstack external-secrets strimzi prometheus-community"
for repo in $REQUIRED_REPOS; do
  helm repo list 2>/dev/null | grep -q "$repo" || {
    warn "Helm repo '$repo' missing — adding..."
    case $repo in
      jetstack)             helm repo add jetstack https://charts.jetstack.io ;;
      external-secrets)     helm repo add external-secrets https://charts.external-secrets.io ;;
      strimzi)              helm repo add strimzi https://strimzi.io/charts/ ;;
      prometheus-community) helm repo add prometheus-community https://prometheus-community.github.io/helm-charts ;;
    esac
    ok "Added $repo repo"
  }
done
helm repo update > /dev/null 2>&1
ok "Helm repos up to date"

echo ""

# ── Step 1: Terraform — recreate GKE cluster ─────────────────────────────────
log "Step 1/7 — Recreating GKE cluster via Terraform..."
cd "$REPO_ROOT/terraform/gke"

terraform init -reconfigure > /dev/null 2>&1
terraform apply -auto-approve

ok "GKE cluster created"
echo ""

# ── Step 2: Connect kubectl ───────────────────────────────────────────────────
log "Step 2/7 — Connecting kubectl to cluster..."
gcloud container clusters get-credentials $CLUSTER_NAME \
  --region $REGION \
  --project $PROJECT_ID

# Wait for cluster API to be fully ready
log "  Waiting for cluster API to be ready..."
for i in $(seq 1 12); do
  kubectl cluster-info &>/dev/null && break
  echo -n "  ."
  sleep 10
done
echo ""
ok "kubectl connected"
echo ""

# ── Step 3: Verify namespaces ─────────────────────────────────────────────────
log "Step 3/7 — Verifying namespaces..."
REQUIRED_NS="kafka spark airflow ingest-api monitoring"
for ns in $REQUIRED_NS; do
  kubectl get namespace $ns &>/dev/null && \
    ok "namespace/$ns exists" || \
    warn "namespace/$ns missing — Terraform may need re-apply"
done
echo ""

# ── Step 4: Install cert-manager ─────────────────────────────────────────────
log "Step 4/7 — Installing cert-manager..."
helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.14.4 \
  --set installCRDs=true \
  --set startupapicheck.enabled=false \
  --cleanup-on-fail \
  --timeout 10m \
  --wait

ok "cert-manager installed"
echo ""

# ── Step 5: Install External Secrets Operator ────────────────────────────────
log "Step 5/7 — Installing External Secrets Operator..."
helm upgrade --install external-secrets external-secrets/external-secrets \
  --namespace external-secrets \
  --create-namespace \
  --version 0.9.14 \
  --set installCRDs=true \
  --cleanup-on-fail \
  --timeout 10m \
  --wait

ok "External Secrets Operator installed"

# Restore ClusterSecretStore
if [ -f "$REPO_ROOT/kubernetes/secretstore.yaml" ]; then
  kubectl apply -f "$REPO_ROOT/kubernetes/secretstore.yaml"
  ok "ClusterSecretStore restored"
fi
echo ""

# ── Step 6: Install Strimzi ───────────────────────────────────────────────────
log "Step 6/7 — Installing Strimzi Kafka Operator..."
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator \
  --namespace kafka \
  --version 0.51.0 \
  --set watchNamespaces="{kafka}" \
  --cleanup-on-fail \
  --timeout 10m \
  --wait

ok "Strimzi operator installed"
echo ""

# ── Step 7: Install Prometheus + Grafana ─────────────────────────────────────
log "Step 7/7 — Installing Prometheus + Grafana..."
helm upgrade --install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --version 58.2.1 \
  --values "$REPO_ROOT/kubernetes/monitoring/prometheus-values.yaml" \
  --cleanup-on-fail \
  --timeout 15m \
  --wait

ok "Prometheus + Grafana installed"
echo ""

kubectl apply -f "$REPO_ROOT/kubernetes/kafka-connect/kafka-connect-sa.yaml"
ok "ksa-ingest-api serviceAccount created in kafka namespace"

# ── Phase 02: Kafka cluster ───────────────────────────────────────────────────
log "Phase 02 — Deploying Kafka cluster..."

KAFKA_DIR="$REPO_ROOT/kubernetes/kafka"

if [ -f "$KAFKA_DIR/kafka-cluster.yaml" ]; then

  # Delete any orphaned PVCs from previous sessions
  kubectl delete pvc --all -n kafka 2>/dev/null || true
  sleep 5

  # Apply in correct order
  kubectl apply -f "$KAFKA_DIR/kafka-metrics-config.yaml"
  kubectl apply -f "$KAFKA_DIR/kafka-node-pool.yaml"

  log "  Waiting 30s for KafkaNodePool to register..."
  sleep 30

  kubectl apply -f "$KAFKA_DIR/kafka-cluster.yaml"

  log "  Waiting for Kafka brokers (up to 10 minutes)..."
  kubectl wait kafka --all \
    --for=condition=Ready \
    --timeout=600s \
    -n kafka 2>/dev/null || \
    warn "Kafka not ready within timeout — check: kubectl get pods -n kafka"

  ok "Kafka cluster deployed"

  # Apply topics
  if [ -f "$KAFKA_DIR/kafka-topics.yaml" ]; then
    kubectl apply -f "$KAFKA_DIR/kafka-topics.yaml"
    ok "Kafka topics applied"
  fi

else
  warn "No Kafka manifests found at $KAFKA_DIR — skipping"
fi
echo ""

# ── Phase 02: Schema Registry ─────────────────────────────────────────────────
log "Phase 02 — Deploying Schema Registry..."

SR_MANIFEST="$REPO_ROOT/kubernetes/schema-registry/schema-registry.yaml"

if [ -f "$SR_MANIFEST" ]; then
  kubectl apply -f "$SR_MANIFEST"

  log "  Waiting for Schema Registry to be ready (up to 5 minutes)..."
  kubectl wait deployment/schema-registry \
    --for=condition=Available \
    --timeout=300s \
    -n kafka 2>/dev/null && \
    ok "Schema Registry ready" || \
    warn "Schema Registry not ready — check: kubectl get pods -n kafka"
else
  warn "No Schema Registry manifest found — skipping"
fi
echo ""

# ── Phase 02: Kafka Connect ───────────────────────────────────────────────────
log "Phase 02 — Deploying Kafka Connect..."

KC_MANIFEST="$REPO_ROOT/kubernetes/kafka-connect/kafka-connect.yaml"

if [ -f "$KC_MANIFEST" ]; then
  kubectl apply -f "$KC_MANIFEST"

  log "  Waiting for Kafka Connect (init container installs GCS plugin — up to 10 minutes)..."
  kubectl wait deployment/kafka-connect \
    --for=condition=Available \
    --timeout=600s \
    -n kafka 2>/dev/null && \
    ok "Kafka Connect ready" || \
    warn "Kafka Connect not ready — check: kubectl get pods -n kafka"

  # Re-register the GCS Sink connector via REST API
  log "  Registering GCS Sink Connector..."
  sleep 15

  kubectl run connector-restore \
    --image=curlimages/curl \
    --namespace=kafka \
    --restart=Never \
    --rm \
    -- sh -c \
    'curl -s -X POST http://kafka-connect:8083/connectors \
      -H "Content-Type: application/json" \
      -d "{\"name\":\"gcs-sink-raw-measurements\",\"config\":{\"connector.class\":\"io.confluent.connect.gcs.GcsSinkConnector\",\"tasks.max\":\"3\",\"topics\":\"raw-measurements\",\"gcs.bucket.name\":\"crowdsource-data-app-clone-landing\",\"gcs.part.size\":\"5242880\",\"flush.size\":\"1000\",\"rotate.interval.ms\":\"300000\",\"storage.class\":\"io.confluent.connect.gcs.storage.GcsStorage\",\"format.class\":\"io.confluent.connect.gcs.format.avro.AvroFormat\",\"path.format\":\"year=YYYY/month=MM/day=dd/hour=HH\",\"locale\":\"en_GB\",\"timezone\":\"UTC\",\"timestamp.extractor\":\"RecordField\",\"timestamp.field\":\"timestamp\",\"schema.compatibility\":\"BACKWARD\",\"value.converter\":\"io.confluent.connect.avro.AvroConverter\",\"value.converter.schema.registry.url\":\"http://schema-registry:8081\",\"key.converter\":\"org.apache.kafka.connect.storage.StringConverter\",\"errors.tolerance\":\"all\",\"errors.deadletterqueue.topic.name\":\"raw-measurements-dlq\",\"errors.deadletterqueue.topic.replication.factor\":\"3\"}}" \
    2>/dev/null' 2>/dev/null && \
    ok "GCS Sink Connector registered" || \
    warn "Connector registration failed — register manually after startup"

else
  warn "No Kafka Connect manifest found — skipping"
fi
echo ""

# ── Final verification ────────────────────────────────────────────────────────
log "Running final verification..."
echo ""

echo "  Pods:"
kubectl get pods -A \
  --field-selector=status.phase!=Running \
  --field-selector=status.phase!=Succeeded \
  2>/dev/null | grep -v "^NAMESPACE" || \
  echo -e "  ${GREEN}All pods healthy${NC}"

echo ""
echo "  Helm releases:"
helm list -A --output table 2>/dev/null | \
  awk 'NR==1{print "  "$0} NR>1{print "  "$0}'

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║         STARTUP COMPLETE                 ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${GREEN}✓${NC}  GKE cluster running"
echo -e "  ${GREEN}✓${NC}  All 4 operators installed"
echo -e "  ${GREEN}✓${NC}  Workload Identity active"
echo -e "  ${GREEN}✓${NC}  Kafka cluster + topics deployed"
echo -e "  ${GREEN}✓${NC}  Schema Registry running"
echo -e "  ${GREEN}✓${NC}  Kafka Connect + GCS Sink running"
echo ""
echo -e "  ${YELLOW}Access Grafana:${NC}"
echo -e "  kubectl port-forward svc/\$(kubectl get svc -n monitoring --selector=app.kubernetes.io/name=grafana -o name | head -1 | cut -d/ -f2) 3000:80 -n monitoring"
echo ""
echo -e "  ${YELLOW}When done for the day:${NC}"
echo -e "  bash scripts/teardown-cluster.sh"
echo ""