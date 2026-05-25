data "google_project" "project" {
  project_id = var.project_id
}

locals {
  required_apis = [
    "container.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "sqladmin.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "servicenetworking.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "pubsub.googleapis.com",
    "dataplex.googleapis.com",
    "datacatalog.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.required_apis)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_project" "project" {
  name       = var.project_id
  project_id = var.project_id
  labels = {
    environment = var.environment
    managed_by  = "terraform"
    team        = "data-engineering"
  }

  lifecycle {
    prevent_destroy = true
    ignore_changes = [billing_account]
  }
}