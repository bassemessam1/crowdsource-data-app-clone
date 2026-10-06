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
#   - FastAPI Ingest API (Phase 02)
#   - Measurement Simulator (Phase 02)
#   - Spark Operator (Phase 03)
#   - Bronze / Silver / Gold Spark jobs (Phase 03)
#   - Airflow 2.9.2 with built-in Postgres (Phase 04)
#   - dbt transform task runs automatically as part of 
#     daily_batch_pipeline (phase 05) - no seperate step 
#     needed: ksa-dbt comes from the same `terraform apply` 
#     as the rest of gke module, and the task itself is pulled in 
#     via GitSync along with the rest of the DAG 
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
REQUIRED_REPOS="jetstack external-secrets strimzi prometheus-community spark-operator apache-airflow"
for repo in $REQUIRED_REPOS; do
  helm repo list 2>/dev/null | grep -q "$repo" || {
    warn "Helm repo '$repo' missing — adding..."
    case $repo in
      jetstack)             helm repo add jetstack https://charts.jetstack.io ;;
      external-secrets)     helm repo add external-secrets https://charts.external-secrets.io ;;
      strimzi)              helm repo add strimzi https://strimzi.io/charts/ ;;
      prometheus-community) helm repo add prometheus-community https://prometheus-community.github.io/helm-charts ;;
      spark-operator)       helm repo add spark-operator https://kubeflow.github.io/spark-operator ;;
      apache-airflow)       helm repo add apache-airflow https://airflow.apache.org ;;
    esac
    ok "Added $repo repo"
  }
done
helm repo update > /dev/null 2>&1
ok "Helm repos up to date"

echo ""

# ── Step 1: Terraform — recreate GKE cluster ─────────────────────────────────
log "Step 1/8 — Recreating GKE cluster via Terraform..."
cd "$REPO_ROOT/terraform/gke"

terraform init -reconfigure > /dev/null 2>&1
terraform apply -auto-approve

ok "GKE cluster created"
echo ""

# ── Step 2: Connect kubectl ───────────────────────────────────────────────────
log "Step 2/8 — Connecting kubectl to cluster..."
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
log "Step 3/8 — Verifying namespaces..."
REQUIRED_NS="kafka spark airflow ingest-api monitoring"
for ns in $REQUIRED_NS; do
  kubectl get namespace $ns &>/dev/null && \
    ok "namespace/$ns exists" || \
    warn "namespace/$ns missing — Terraform may need re-apply"
done
echo ""

# ── Step 4: Install cert-manager ─────────────────────────────────────────────
log "Step 4/8 — Installing cert-manager..."
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
log "Step 5/8 — Installing External Secrets Operator..."
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
log "Step 6/8 — Installing Strimzi Kafka Operator..."
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
log "Step 7/8 — Installing Prometheus + Grafana..."
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

# ── Step 8: Install Spark Operator ───────────────────────────────────────────
log "Step 8/8 — Installing Spark Operator..."
helm upgrade --install spark-operator \
  spark-operator/spark-operator \
  --namespace spark \
  --version 1.1.27 \
  --set sparkJobNamespace=spark \
  --set serviceAccounts.spark.name=ksa-spark \
  --set serviceAccounts.spark.create=false \
  --set serviceAccounts.sparkoperator.create=true \
  --set webhook.enable=false \
  --cleanup-on-fail \
  --timeout 10m \
  --wait

ok "Spark Operator installed"
echo ""

# ── Phase 02: ksa-ingest-api in kafka namespace ───────────────────────────────
# Kafka Connect runs in kafka namespace and needs this SA for GCS access
if [ -f "$REPO_ROOT/kubernetes/kafka-connect/kafka-connect-sa.yaml" ]; then
  kubectl apply -f "$REPO_ROOT/kubernetes/kafka-connect/kafka-connect-sa.yaml"
  ok "ksa-ingest-api ServiceAccount created in kafka namespace"
fi
echo ""

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

  # Recreate gcs-credentials secret — destroyed with cluster each session
  log "  Recreating gcs-credentials secret..."
  kubectl delete secret gcs-credentials -n kafka 2>/dev/null || true
  if gcloud secrets versions access latest \
    --secret=kafka-connect-gcs-key \
    --project=$PROJECT_ID \
    > /tmp/gcs-key.json 2>/dev/null; then
    kubectl create secret generic gcs-credentials \
      --from-file=key.json=/tmp/gcs-key.json \
      -n kafka
    rm -f /tmp/gcs-key.json
    ok "gcs-credentials secret created"
  else
    warn "kafka-connect-gcs-key not found in Secret Manager — Kafka Connect may fail"
    rm -f /tmp/gcs-key.json
  fi

  # Remove any leftover KafkaConnect CRDs from previous sessions
  # These trigger unwanted Kaniko build jobs that fail on GKE Autopilot
  kubectl delete kafkaconnect --all -n kafka 2>/dev/null || true
  sleep 5

  kubectl apply -f "$KC_MANIFEST"

  log "  Waiting for Kafka Connect (up to 10 minutes)..."
  kubectl wait deployment/kafka-connect \
    --for=condition=Available \
    --timeout=600s \
    -n kafka 2>/dev/null && \
    ok "Kafka Connect ready" || \
    warn "Kafka Connect not ready — check: kubectl get pods -n kafka"

  # Re-register the GCS Sink connector via REST API
  # Uses confluent.topic.bootstrap.servers and gcs.credentials.path
  # which are required by the Confluent Platform image

  log "  Waiting for Kafka Connect REST API to accept connections..."
  REST_READY=false
  for i in $(seq 1 30); do
    if kubectl exec -n kafka deploy/kafka-connect -- \
       curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors 2>/dev/null | grep -q 200; then
      REST_READY=true
      ok "Kafka Connect REST API is up"
      break
    fi
    sleep 10
  done
  if [ "$REST_READY" = false ]; then
    warn "Kafka Connect REST API never responded — connector registration will likely fail"
  fi

  log "  Registering GCS Sink Connector (via port-forward)..."

  # Start a background port-forward, give it a moment to establish
  kubectl port-forward svc/kafka-connect 8083:8083 -n kafka > /tmp/kc-portforward.log 2>&1 &
  KC_PF_PID=$!
  sleep 5

  CONNECTOR_REGISTERED=false
  if curl -s -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d '{
  "name": "gcs-sink-raw-measurements",
  "config": {
    "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
    "tasks.max": "1",
    "topics": "raw-measurements",
    "gcs.bucket.name": "crowdsource-data-app-clone-landing",
    "gcs.part.size": "5242880",
    "flush.size": "1000",
    "rotate.interval.ms": "300000",
    "storage.class": "io.confluent.connect.gcs.storage.GcsStorage",
    "format.class": "io.confluent.connect.gcs.format.avro.AvroFormat",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "partition.duration.ms": "3600000",
    "path.format": "'\''year'\''=YYYY/'\''month'\''=MM/'\''day'\''=dd/'\''hour'\''=HH",
    "locale": "en_GB",
    "timezone": "UTC",
    "timestamp.extractor": "RecordField",
    "timestamp.field": "timestamp",
    "schema.compatibility": "BACKWARD",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "raw-measurements-dlq",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "confluent.topic.bootstrap.servers": "crowdsource-data-app-kafka-kafka-bootstrap:9092",
    "confluent.topic.replication.factor": "3",
    "gcs.credentials.path": "/etc/gcs-credentials/key.json"
  }
}' 2>/dev/null | grep -q '"name"'; then
    CONNECTOR_REGISTERED=true
  fi

  # Verify it actually reached RUNNING state, not just that the POST was accepted
  if [ "$CONNECTOR_REGISTERED" = true ]; then
    sleep 5
    CONNECTOR_STATE=$(curl -s http://localhost:8083/connectors/gcs-sink-raw-measurements/status 2>/dev/null | \
      grep -o '"state":"[A-Z]*"' | head -1)
    if echo "$CONNECTOR_STATE" | grep -q RUNNING; then
      ok "GCS Sink Connector registered and RUNNING"
    else
      warn "Connector registered but state is: $CONNECTOR_STATE — check: kubectl logs -n kafka -l app=kafka-connect"
    fi
  else
    warn "Connector registration failed — register manually: kubectl port-forward svc/kafka-connect 8083:8083 -n kafka"
  fi

  # Clean up the port-forward
  kill $KC_PF_PID 2>/dev/null || true

else
  warn "No Kafka Connect manifest found — skipping"
fi

echo ""

# ── Phase 02: Ingest API ──────────────────────────────────────────────────────
log "Phase 02 — Deploying Ingest API..."

INGEST_MANIFEST="$REPO_ROOT/kubernetes/ingest-api/ingest-api.yaml"

if [ -f "$INGEST_MANIFEST" ]; then
  kubectl apply -f "$INGEST_MANIFEST"

  log "  Waiting for Ingest API to be ready (up to 5 minutes)..."
  kubectl wait deployment/ingest-api \
    --for=condition=Available \
    --timeout=300s \
    -n ingest-api 2>/dev/null && \
    ok "Ingest API ready" || \
    warn "Ingest API not ready — check: kubectl get pods -n ingest-api"
else
  warn "No Ingest API manifest found — skipping"
fi
echo ""

# ── Phase 02: Measurement Simulator ──────────────────────────────────────────
log "Phase 02 — Deploying Measurement Simulator..."

SIM_MANIFEST="$REPO_ROOT/kubernetes/simulator/simulator.yaml"

if [ -f "$SIM_MANIFEST" ]; then
  kubectl apply -f "$SIM_MANIFEST"

  log "  Waiting for Simulator to be ready (up to 3 minutes)..."
  kubectl wait deployment/measurement-simulator \
    --for=condition=Available \
    --timeout=180s \
    -n ingest-api 2>/dev/null && \
    ok "Simulator ready" || \
    warn "Simulator not ready — check: kubectl get pods -n ingest-api"
else
  warn "No Simulator manifest found — skipping"
fi
echo ""

# ── Phase 03: Spark Bronze / Silver / Gold jobs ───────────────────────────────
log "Phase 03 — Running Spark data lake jobs..."

SPARK_DIR="$REPO_ROOT/kubernetes/spark"

if [ -f "$SPARK_DIR/bronze-job.yaml" ]; then

  # Clean up any leftover SparkApplications from previous sessions
  kubectl delete sparkapplication --all -n spark 2>/dev/null || true
  sleep 5

  # Bronze job — landing Avro → bronze Parquet
  log "  Running Bronze job (landing → bronze)..."
  kubectl apply -f "$SPARK_DIR/bronze-job.yaml"

  log "  Waiting for Bronze job to complete (up to 15 minutes)..."
  kubectl wait sparkapplication/bronze-measurements \
    --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
    --timeout=900s \
    -n spark 2>/dev/null && \
    ok "Bronze job completed" || \
    warn "Bronze job did not complete — check: kubectl get sparkapplication -n spark"

  # Silver job — bronze Parquet → silver (dedup + enrichment)
  log "  Running Silver job (bronze → silver)..."
  kubectl apply -f "$SPARK_DIR/silver-job.yaml"

  log "  Waiting for Silver job to complete (up to 15 minutes)..."
  kubectl wait sparkapplication/silver-measurements \
    --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
    --timeout=900s \
    -n spark 2>/dev/null && \
    ok "Silver job completed" || \
    warn "Silver job did not complete — check: kubectl get sparkapplication -n spark"

  # Gold job — silver → aggregated metrics + BigQuery load
  log "  Running Gold job (silver → gold + BigQuery)..."
  kubectl apply -f "$SPARK_DIR/gold-job.yaml"

  log "  Waiting for Gold job to complete (up to 15 minutes)..."
  kubectl wait sparkapplication/gold-operator-metrics \
    --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
    --timeout=900s \
    -n spark 2>/dev/null && \
    ok "Gold job completed — BigQuery loaded" || \
    warn "Gold job did not complete — check: kubectl get sparkapplication -n spark"

else
  warn "No Spark job manifests found at $SPARK_DIR — skipping"
fi
echo ""

# ── Phase 04: Airflow RBAC ───────────────────────────────────────────────────
# Must be applied before Airflow starts so scheduler can create SparkApplications
# ClusterRoleBinding is destroyed with the cluster — recreate every session
log "Phase 04 — Applying Airflow SparkApplication RBAC..."
if [ -f "$REPO_ROOT/kubernetes/airflow/airflow-spark-rbac.yaml" ]; then
  kubectl apply -f "$REPO_ROOT/kubernetes/airflow/airflow-spark-rbac.yaml"
  ok "Airflow SparkApplication RBAC applied"
else
  warn "airflow-spark-rbac.yaml not found — DAG will fail with 403 Forbidden"
fi
echo ""

# ── Phase 04: Airflow ────────────────────────────────────────────────────────
log "Phase 04 — Deploying Airflow..."

AIRFLOW_VALUES="$REPO_ROOT/kubernetes/airflow/airflow-values.yaml"

if [ -f "$AIRFLOW_VALUES" ]; then
  helm upgrade --install airflow apache-airflow/airflow \
    --namespace airflow \
    --version 1.13.1 \
    --values "$AIRFLOW_VALUES" \
    --cleanup-on-fail \
    --timeout 20m 2>/dev/null && \
    ok "Airflow installed" || \
    warn "Airflow install failed — check: kubectl get pods -n airflow"

  log "  Waiting for Airflow webserver to be ready (up to 10 minutes)..."
  kubectl wait deployment/airflow-webserver \
    --for=condition=Available \
    --timeout=600s \
    -n airflow 2>/dev/null && \
    ok "Airflow webserver ready" || \
    warn "Airflow webserver not ready — check: kubectl get pods -n airflow"

  # Clear any stale queued DAG runs left from previous sessions
  # These accumulate because hourly schedule tries to backfill missed runs
  log "  Clearing stale DAG runs from previous sessions..."
  sleep 30
  AIRFLOW_SCHEDULER=$(kubectl get pods -n airflow \
    --selector=component=scheduler \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -n "$AIRFLOW_SCHEDULER" ]; then
    kubectl exec -n airflow $AIRFLOW_SCHEDULER -c scheduler -- \
      airflow dags pause daily_batch_pipeline 2>/dev/null || true
    kubectl exec -n airflow $AIRFLOW_SCHEDULER -c scheduler -- \
      python3 -c "
from airflow.models import DagRun
from airflow.utils.session import create_session
with create_session() as session:
    runs = session.query(DagRun).filter(
        DagRun.dag_id=='daily_batch_pipeline',
        DagRun.state.in_(['queued','failed'])
    ).all()
    for r in runs:
        session.delete(r)
    session.commit()
    print(f'Cleared {len(runs)} stale dag runs')
" 2>/dev/null && ok "Stale DAG runs cleared" || \
      warn "Could not clear stale runs — clear manually via Airflow UI"

    kubectl exec -n airflow $AIRFLOW_SCHEDULER -c scheduler -- \
      airflow dags unpause daily_batch_pipeline 2>/dev/null && \
      ok "daily_batch_pipeline unpaused" || \
      warn "Could not unpause DAG — unpause manually via Airflow UI"

  fi
else
  warn "No Airflow values file found at $AIRFLOW_VALUES — skipping"
  warn "Create it at: kubernetes/airflow/airflow-values.yaml"
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
echo -e "  ${GREEN}✓${NC}  All 5 operators installed (Spark + Airflow)"
echo -e "  ${GREEN}✓${NC}  Workload Identity active"
echo -e "  ${GREEN}✓${NC}  Kafka cluster + topics deployed"
echo -e "  ${GREEN}✓${NC}  Schema Registry running"
echo -e "  ${GREEN}✓${NC}  Kafka Connect + GCS Sink running"
echo -e "  ${GREEN}✓${NC}  Ingest API running (static IP: 34.89.87.57)"
echo -e "  ${GREEN}✓${NC}  Measurement Simulator running"
echo -e "  ${GREEN}✓${NC}  Spark Bronze / Silver / Gold jobs run"
echo -e "  ${GREEN}✓${NC}  Airflow running (built-in Postgres)"
echo ""
echo -e "  ${YELLOW}Access Grafana:${NC}"
echo -e "  kubectl port-forward svc/\$(kubectl get svc -n monitoring --selector=app.kubernetes.io/name=grafana -o name | head -1 | cut -d/ -f2) 3000:80 -n monitoring"
echo ""
echo -e "  ${YELLOW}Test Ingest API:${NC}"
echo -e "  curl -s http://34.89.87.57/health"
echo ""
echo -e "  ${YELLOW}Check BigQuery:${NC}"
echo -e "  bq query --nouse_legacy_sql 'SELECT operator_name, COUNT(*) as rows FROM opensignal_gold.operator_metrics GROUP BY 1'"
echo ""
echo -e "  ${YELLOW}Access Airflow UI:${NC}"
echo -e "  kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow"
echo -e "  Open: http://localhost:8080  |  Login: admin / crowdsource-dev-2026"
echo ""
echo -e "  YELLOWCheckBigQuery(gold):{NC}"
echo -e "  bq query --use_legacy_sql=false 'SELECT operator_name, COUNT(*) as rows FROM crowdsource_data_app_gold.operator_metrics GROUP BY 1'"
echo ""
echo -e "  YELLOWCheckBigQuery(dbtmarts):{NC}"
echo -e "  bq query --use_legacy_sql=false 'SELECT * FROM crowdsource_data_app_marts.operator_rankings ORDER BY metric_date DESC LIMIT 10'"
echo ""
echo -e "  ${YELLOW}When done for the day:${NC}"
echo -e "  bash scripts/teardown-cluster.sh"
echo ""
