terraform {
  required_version = ">= 1.7.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.20"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  backend "gcs" {
    bucket = "crowdsource-app-tfstate-crowdsource-data-app-clone"
    prefix = "cloudsql"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type    = string
  default = "crowdsource-data-app-clone"
}

variable "region" {
  type    = string
  default = "europe-west2"
}

# ── Random suffix to ensure unique instance name ──────────────────────────────
resource "random_id" "db_suffix" {
  byte_length = 4
}

# ── Cloud SQL PostgreSQL instance ─────────────────────────────────────────────
resource "google_sql_database_instance" "airflow" {
  name             = "airflow-db-${random_id.db_suffix.hex}"
  database_version = "POSTGRES_15"
  region           = var.region

  deletion_protection = false

  settings {
    tier              = "db-f1-micro"
    availability_type = "ZONAL"
    disk_size         = 10
    disk_type         = "PD_SSD"

    backup_configuration {
      enabled = true
    }

    ip_configuration {
      ipv4_enabled    = false
      private_network = "projects/${var.project_id}/global/networks/crowdsource-aap-vpc"
      enable_private_path_for_google_cloud_services = true
    }
  }
}

# ── Airflow database ──────────────────────────────────────────────────────────
resource "google_sql_database" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.airflow.name
}

# ── Airflow database user ─────────────────────────────────────────────────────
resource "random_password" "airflow_db" {
  length  = 24
  special = false
}

resource "google_sql_user" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.airflow.name
  password = random_password.airflow_db.result
}

# ── Store password in Secret Manager ─────────────────────────────────────────
resource "google_secret_manager_secret" "airflow_db_password" {
  secret_id = "airflow-db-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "airflow_db_password" {
  secret      = google_secret_manager_secret.airflow_db_password.id
  secret_data = random_password.airflow_db.result
}

# ── Grant sa-airflow access to read the secret ────────────────────────────────
resource "google_secret_manager_secret_iam_member" "airflow_db_secret" {
  secret_id = google_secret_manager_secret.airflow_db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:sa-airflow@${var.project_id}.iam.gserviceaccount.com"
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "instance_name"        { value = google_sql_database_instance.airflow.name }
output "instance_private_ip"  { value = google_sql_database_instance.airflow.private_ip_address }
output "database_name"        { value = google_sql_database.airflow.name }
output "database_user"        { value = google_sql_user.airflow.name }
output "connection_name"      { value = google_sql_database_instance.airflow.connection_name }
