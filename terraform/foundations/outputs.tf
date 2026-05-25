output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "Primary region"
  value       = var.region
}

output "vpc_name" {
  description = "VPC network name"
  value       = google_compute_network.vpc.name
}

output "vpc_self_link" {
  description = "VPC self-link for use in GKE module"
  value       = google_compute_network.vpc.self_link
}

output "subnet_name" {
  description = "GKE subnet name"
  value       = google_compute_subnetwork.gke.name
}

output "subnet_self_link" {
  description = "GKE subnet self-link"
  value       = google_compute_subnetwork.gke.self_link
}

output "ingest_api_static_ip" {
  description = "Static external IP reserved for the Ingest API load balancer"
  value       = google_compute_address.ingest_api.address
}

output "sa_ingest_api_email" {
  description = "Ingest API service account email"
  value       = google_service_account.ingest_api.email
}

output "sa_spark_email" {
  description = "Spark service account email"
  value       = google_service_account.spark.email
}

output "sa_airflow_email" {
  description = "Airflow service account email"
  value       = google_service_account.airflow.email
}

output "sa_dbt_email" {
  description = "dbt service account email"
  value       = google_service_account.dbt.email
}

output "project_number" {
  description = "GCP project number - needed for workload Identity bindings"
  value       = data.google_project.project.number
}