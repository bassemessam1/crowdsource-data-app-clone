# ── Service Accounts ─────────────────────────────────────────────────────────

resource "google_service_account" "ingest_api" {
  account_id   = "sa-ingest-api"
  display_name = "Ingest API Service Account"
  description  = "Used by the FastAPI ingest service — writes to GCS landing, reads secrets"
  project      = var.project_id
}

resource "google_service_account" "spark" {
  account_id   = "sa-spark"
  display_name = "Spark Jobs Service Account"
  description  = "Used by PySpark jobs — reads/writes all GCS lake zones, loads BigQuery"
  project      = var.project_id
}

resource "google_service_account" "airflow" {
  account_id   = "sa-airflow"
  display_name = "Airflow Orchestrator Service Account"
  description  = "Used by Airflow — submits Spark K8s jobs, reads BQ for quality checks"
  project      = var.project_id
}

resource "google_service_account" "dbt" {
  account_id   = "sa-dbt"
  display_name = "dbt Transformer Service Account"
  description  = "Used by dbt — reads and writes BigQuery datasets only"
  project      = var.project_id
}

resource "google_service_account" "terraform" {
  account_id   = "sa-terraform"
  display_name = "Terraform Infrastructure Service Account"
  description  = "Used locally by Terraform — provisions all GCP infrastructure"
  project      = var.project_id
}

# ── IAM Bindings — sa-ingest-api ─────────────────────────────────────────────

resource "google_project_iam_member" "ingest_api_gcs_writer" {
  project = var.project_id
  role    = "roles/storage.objectCreator"
  member  = "serviceAccount:${google_service_account.ingest_api.email}"
}

resource "google_project_iam_member" "ingest_api_secret_reader" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.ingest_api.email}"
}

# ── IAM Bindings — sa-spark ───────────────────────────────────────────────────

resource "google_project_iam_member" "spark_gcs_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_project_iam_member" "spark_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_project_iam_member" "spark_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

# ── IAM Bindings — sa-airflow ─────────────────────────────────────────────────

resource "google_project_iam_member" "airflow_container_developer" {
  project = var.project_id
  role    = "roles/container.developer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_gcs_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# ── IAM Bindings — sa-dbt ─────────────────────────────────────────────────────

resource "google_project_iam_member" "dbt_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_project_iam_member" "dbt_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

# ── IAM Bindings — sa-terraform ───────────────────────────────────────────────

resource "google_project_iam_member" "terraform_editor" {
  project = var.project_id
  role    = "roles/editor"
  member  = "serviceAccount:${google_service_account.terraform.email}"
}

resource "google_project_iam_member" "terraform_security_admin" {
  project = var.project_id
  role    = "roles/iam.securityAdmin"
  member  = "serviceAccount:${google_service_account.terraform.email}"
}