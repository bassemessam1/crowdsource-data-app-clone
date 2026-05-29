output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.main.name
}

output "cluster_endpoint" {
  description = "GKE cluster API endpoint — used by kubectl"
  value       = google_container_cluster.main.endpoint
  sensitive   = true
}

output "cluster_location" {
  description = "GKE cluster region"
  value       = google_container_cluster.main.location
}

output "workload_pool" {
  description = "Workload Identity pool — used in IAM member bindings"
  value       = "${var.project_id}.svc.id.goog"
}

output "kubectl_command" {
  description = "Run this command to configure kubectl after apply"
  value       = "gcloud container clusters get-credentials ${var.cluster_name} --region ${var.region} --project ${var.project_id}"
}
