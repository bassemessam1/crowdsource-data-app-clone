variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for the GKE cluster"
  type        = string
  default     = "europe-west2"
}

variable "cluster_name" {
  description = "Name of the GKE Autopilot cluster"
  type        = string
  default     = "crowdsource-data-app-gke"
}

variable "environment" {
  description = "Environment label"
  type        = string
  default     = "dev"
}

# These reference the VPC resources created in the foundation module.
# We read them from the foundation's Terraform state rather than
# hardcoding them — this is how Terraform modules share values.
variable "vpc_name" {
  description = "VPC network name from foundation module"
  type        = string
  default     = "crowdsource-aap-vpc"
}

variable "subnet_name" {
  description = "GKE subnet name from foundation module"
  type        = string
  default     = "crowdsource-aap-gke-subnet"
}

variable "pods_range_name" {
  description = "Secondary range name for pods"
  type        = string
  default     = "pods"
}

variable "services_range_name" {
  description = "Secondary range name for services"
  type        = string
  default     = "services"
}
