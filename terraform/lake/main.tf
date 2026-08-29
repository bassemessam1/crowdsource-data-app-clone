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
    action    { type = "Delete" }
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

# ── Grant Kafka Connect (sa-ingest-api) write access ─────────────────────────
resource "google_storage_bucket_iam_member" "landing_writer" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:sa-ingest-api@${var.project_id}.iam.gserviceaccount.com"
}

output "landing_bucket" {
  value = google_storage_bucket.landing.name
}
