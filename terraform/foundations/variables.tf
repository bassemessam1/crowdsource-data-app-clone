
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Primary GCP region for all resources"
  type        = string
  default     = "europe-west2"
}

variable "zone" {
  description = "Primary GCP zone"
  type        = string
  default     = "europe-west2-a"
}

variable "environment" {
  description = "Environment label applied to all resources"
  type        = string
  default     = "dev"
}

variable "vpc_name" {
  description = "Name of the VPC network"
  type        = string
  default     = "crowdsource-aap-vpc"

}
variable "subnet_name" {
  description = "Primary IP ranges for GKE nodes"
  type        = string
  default     = "crowdsource-aap-gke-subnet"
}

variable "subnet_cidr" {
  description = "Primary IP range for GKE nodes"
  type        = string
  default     = "10.0.0.0/20"

}

variable "pods_cidr" {
  description = "Secondary IP range for K8s services"
  type        = string
  default     = "10.4.0.0/14"
}

variable "services_cidr" {
  description = "Secondary IP range for K*s services"
  type        = string
  default     = "10.0.16.0/20"
}

variable "github_owner" {
  description = "GitHub username or organisation that owns the repository"
  type        = string
}
