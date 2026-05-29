# ── Workload Identity Setup ───────────────────────────────────────────────────
#
# Dependency chain:
#   GKE cluster created
#     → time_sleep (60s) — wait for WI pool to register
#       → IAM bindings — pool now guaranteed to exist
#         → K8s namespaces created
#           → K8s ServiceAccounts — namespace guaranteed to exist

# ── Ingest API ────────────────────────────────────────────────────────────────

resource "kubernetes_service_account" "ingest_api" {
  metadata {
    name      = "ksa-ingest-api"
    namespace = "ingest-api"
    annotations = {
      "iam.gke.io/gcp-service-account" = "sa-ingest-api@${var.project_id}.iam.gserviceaccount.com"
    }
  }
  depends_on = [
    kubernetes_namespace.namespaces,
    time_sleep.wait_for_wi_pool
  ]
}

resource "google_service_account_iam_member" "ingest_api_wi" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/sa-ingest-api@${var.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[ingest-api/ksa-ingest-api]"

  # Wait for pool to exist before creating bindings
  depends_on = [time_sleep.wait_for_wi_pool]
}

# ── Spark ─────────────────────────────────────────────────────────────────────

resource "kubernetes_service_account" "spark" {
  metadata {
    name      = "ksa-spark"
    namespace = "spark"
    annotations = {
      "iam.gke.io/gcp-service-account" = "sa-spark@${var.project_id}.iam.gserviceaccount.com"
    }
  }
  depends_on = [
    kubernetes_namespace.namespaces,
    time_sleep.wait_for_wi_pool
  ]
}

resource "google_service_account_iam_member" "spark_wi" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/sa-spark@${var.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[spark/ksa-spark]"

  depends_on = [time_sleep.wait_for_wi_pool]
}

# ── Airflow ───────────────────────────────────────────────────────────────────

resource "kubernetes_service_account" "airflow" {
  metadata {
    name      = "ksa-airflow"
    namespace = "airflow"
    annotations = {
      "iam.gke.io/gcp-service-account" = "sa-airflow@${var.project_id}.iam.gserviceaccount.com"
    }
  }
  depends_on = [
    kubernetes_namespace.namespaces,
    time_sleep.wait_for_wi_pool
  ]
}

resource "google_service_account_iam_member" "airflow_wi" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/sa-airflow@${var.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[airflow/ksa-airflow]"

  depends_on = [time_sleep.wait_for_wi_pool]
}

# ── dbt ───────────────────────────────────────────────────────────────────────

resource "kubernetes_service_account" "dbt" {
  metadata {
    name      = "ksa-dbt"
    namespace = "airflow"
    annotations = {
      "iam.gke.io/gcp-service-account" = "sa-dbt@${var.project_id}.iam.gserviceaccount.com"
    }
  }
  depends_on = [
    kubernetes_namespace.namespaces,
    time_sleep.wait_for_wi_pool
  ]
}

resource "google_service_account_iam_member" "dbt_wi" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/sa-dbt@${var.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[airflow/ksa-dbt]"

  depends_on = [time_sleep.wait_for_wi_pool]
}
