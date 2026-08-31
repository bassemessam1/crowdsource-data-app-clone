# ── GCS Data Lake Buckets ─────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.7.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.20"
    }
  }
  backend "gcs" {
    bucket = "crowdsource-app-tfstate-crowdsource-data-app-clone"
    prefix = "lake"
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

# ── Landing Zone ──────────────────────────────────────────────────────────────
# Raw events from Kafka Connect land here
# Immutable archive — never modified after write
resource "google_storage_bucket" "landing" {
  name          = "${var.project_id}-landing"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = false

  # Versioning — every overwrite creates a new version
  versioning {
    enabled = true
  }

  # Lifecycle — move to Nearline after 30 days, delete after 90
  lifecycle_rule {
    condition { age = 90 }
    action { type = "Delete" }
  }

  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  uniform_bucket_level_access = true
}

# ── Bronze ────────────────────────────────────────────────────────────────────
# Raw events converted to Parquet, partitioned by date
# Immutable — Spark writes once, never modifies
resource "google_storage_bucket" "bronze" {
  name          = "${var.project_id}-bronze"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = false

  lifecycle_rule {
    condition { age = 180 }
    action { type = "Delete" }
  }
  lifecycle_rule {
    condition { age = 60 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  uniform_bucket_level_access = true
}

# ── Silver ────────────────────────────────────────────────────────────────────
# Deduplicated, enriched with H3 geohash and operator name
resource "google_storage_bucket" "silver" {
  name          = "${var.project_id}-silver"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = false

  lifecycle_rule {
    condition { age = 365 }
    action { type = "Delete" }
  }

  uniform_bucket_level_access = true
}

# ── Gold ──────────────────────────────────────────────────────────────────────
# Aggregated operator metrics — source of truth for BigQuery
resource "google_storage_bucket" "gold" {
  name          = "${var.project_id}-gold"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = false

  uniform_bucket_level_access = true
}

# ── IAM — sa-spark can read/write all lake buckets ───────────────────────────
locals {
  lake_buckets = [
    google_storage_bucket.landing.name,
    google_storage_bucket.bronze.name,
    google_storage_bucket.silver.name,
    google_storage_bucket.gold.name,
  ]
}

resource "google_storage_bucket_iam_member" "spark_lake_admin" {
  for_each = toset(local.lake_buckets)
  bucket   = each.value
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:sa-spark@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "spark_lake_bucket_reader" {
  for_each = toset(local.lake_buckets)
  bucket   = each.value
  role     = "roles/storage.legacyBucketReader"
  member   = "serviceAccount:sa-spark@${var.project_id}.iam.gserviceaccount.com"
}

# ── BigQuery dataset ──────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "gold" {
  dataset_id  = "crowdsource_data_app_gold"
  location    = var.region
  description = "Gold layer — aggregated operator metrics loaded by Spark"

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset_iam_member" "spark_bq_editor" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:sa-spark@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "spark_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:sa-spark@${var.project_id}.iam.gserviceaccount.com"
}


# ── Grant Kafka Connect (sa-ingest-api) write access ─────────────────────────
resource "google_storage_bucket_iam_member" "landing_writer" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:sa-ingest-api@${var.project_id}.iam.gserviceaccount.com"
}

output "landing_bucket" { value = google_storage_bucket.landing.name }
output "bronze_bucket" { value = google_storage_bucket.bronze.name }
output "silver_bucket" { value = google_storage_bucket.silver.name }
output "gold_bucket" { value = google_storage_bucket.gold.name }
output "bq_dataset" { value = google_bigquery_dataset.gold.dataset_id }

