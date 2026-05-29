# ── Data Sources ──────────────────────────────────────────────────────────────

# Reads the current GCP auth token — used by the kubernetes provider
# to authenticate to the cluster API Server
data "google_client_config" "default" {}

# Reads project metadata — we need the project number for
# Workload Identity bindings
data "google_project" "project" {
  project_id = var.project_id
}

# ── GKE Autopilot Cluster ─────────────────────────────────────────────────────
resource "google_container_cluster" "main" {
  provider = google-beta

  name     = var.cluster_name
  location = var.region
  project  = var.project_id

  # AUTOPILOT MODE
  # This single flag enables Autopilot — Google manages all node
  # provisioning, scaling, patching, and security hardening.
  # You pay per pod request, not per node.
  enable_autopilot = true

  # DELETION PROTECTION
  # Prevents accidental terraform destroy of the cluster.
  # Must be set to false before you can delete it intentionally.
  deletion_protection = false

  # NETWORK CONFIGURATION
  # Points the cluster at the VPC and subnet we created in Phase 00.
  # The secondary ranges (pods, services) were defined on the subnet
  # specifically for this cluster.
  network    = var.vpc_name
  subnetwork = var.subnet_name


  

  ip_allocation_policy {
    # These names must exactly match the secondary range names
    # on the subnet defined in vpc.tf in the foundation module
    cluster_secondary_range_name  = var.pods_range_name
    services_secondary_range_name = var.services_range_name
  }

  # PRIVATE CLUSTER
  # Nodes have no public IP addresses — they're only reachable
  # from within the VPC. This is the secure production pattern.
  # enable_private_nodes = true means node VMs are private.
  # master_ipv4_cidr_block is a /28 range for the control plane's
  # internal VPC peering — must not overlap with anything else.
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  # WORKLOAD IDENTITY
  # This is the cluster-level enablement of Workload Identity.
  # Without this, pods cannot exchange K8s tokens for GCP tokens.
  # Individual pod authentication is configured in workload_identity.tf.
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # MASTER AUTHORISED NETWORKS
  # Controls which IP ranges can reach the K8s API Server (control plane).
  # 0.0.0.0/0 allows access from anywhere — acceptable for learning.
  # In production, restrict to your VPN or office IP range.
  master_authorized_networks_config {
    cidr_blocks {
      cidr_block   = "0.0.0.0/0"
      display_name = "all-for-dev"
    }
  }

  # RELEASE CHANNEL
  # REGULAR = stable GKE version updates, tested before rollout.
  # Alternatives: RAPID (newest features), STABLE (slowest updates).
  release_channel {
    channel = "REGULAR"
  }

  # CLUSTER LABELS
  # Applied to the GKE cluster resource for cost tracking and filtering.
  resource_labels = {
    environment = var.environment
    managed_by  = "terraform"
    project     = "crowdsource-data-app-clone"
  }

  # VERTICAL POD AUTOSCALING
  # Automatically adjusts pod CPU/memory requests based on actual usage.
  # Helps Autopilot right-size pods and reduce costs over time.
  vertical_pod_autoscaling {
    enabled = true
  }
}

# ── Workload Identity Pool Readiness Wait ─────────────────────────────────────
# GKE creates the WI pool (PROJECT.svc.id.goog) asynchronously after
# the cluster API reports RUNNING. Without this wait, IAM bindings that
# reference the pool fail with "Identity Pool does not exist".
# 60 seconds is sufficient for the pool registration to propagate.
resource "time_sleep" "wait_for_wi_pool" {
  create_duration = "60s"

  # Only starts counting after the cluster is fully created
  depends_on = [google_container_cluster.main]
}


# ── Kubernetes Namespaces ─────────────────────────────────────────────────────
# Create all project namespaces declaratively in Terraform.
# Each namespace isolates a workload domain — separate access controls,
# resource quotas, and network policies per namespace.

locals {
  namespaces = [
    "kafka",
    "spark",
    "airflow",
    "ingest-api",
    "monitoring",
  ]
}

resource "kubernetes_namespace" "namespaces" {
  for_each = toset(local.namespaces)

  metadata {
    name = each.value

    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
      "environment"                  = var.environment
    }
  }

  # The namespace resource depends on the cluster existing first.
  # Terraform infers this automatically because the kubernetes provider
  # is configured with the cluster's endpoint.
  depends_on = [google_container_cluster.main]
}
