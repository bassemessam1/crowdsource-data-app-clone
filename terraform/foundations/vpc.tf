# ── VPC Network ───────────────────────────────────────────────────────────────

resource "google_compute_network" "vpc" {
  name                    = var.vpc_name
  auto_create_subnetworks = false
  mtu                     = 1460
  project                 = var.project_id

  #description = "Crowdsource Data App clone VPC — custom mode, all subnets explicit"
}

# ── GKE Subnet with Secondary Ranges ─────────────────────────────────────────

resource "google_compute_subnetwork" "gke" {
  name          = var.subnet_name
  network       = google_compute_network.vpc.id
  region        = var.region
  ip_cidr_range = var.subnet_cidr
  project       = var.project_id

  # Enables pods to reach GCS/BigQuery/Secret Manager without public IPs
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }
}

# ── Firewall Rules ────────────────────────────────────────────────────────────

resource "google_compute_firewall" "allow_internal" {
  name    = "crowdsource-app-allow-internal"
  network = google_compute_network.vpc.name
  project = var.project_id
  direction = "INGRESS"
  priority = 1000

  #description = "Allow all internal traffic between nodes, pods, and services"

  allow {
    protocol = "tcp"
  }
  allow {
    protocol = "udp"
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = ["10.0.0.0/8"]
}

resource "google_compute_firewall" "allow_ssh" {
  name    = "crowdsource-app-allow-ssh"
  network = google_compute_network.vpc.name
  project = var.project_id

  #description = "Allow SSH to tagged VMs only — not applied to GKE nodes"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["allow-ssh"]
}

resource "google_compute_firewall" "allow_health_checks" {
  name    = "crowdsource-app-allow-health-checks"
  network = google_compute_network.vpc.name
  project = var.project_id

  #description = "Allow GCP load balancer health checks — required for Ingest API LB"

  allow {
    protocol = "tcp"
  }

  # Google's fixed health-checker source ranges — do not modify
  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]
}

# ── Static IP for Ingest API Load Balancer ────────────────────────────────────

resource "google_compute_address" "ingest_api" {
  name        = "crowdsource-app-ingest-ip"
  region      = var.region
  project     = var.project_id
  #description = "Static external IP for the Ingest API load balancer"
}

# ── Cloud Router ──────────────────────────────────────────────────────────────
# Cloud Router is required by Cloud NAT.
# It advertises routes and manages the NAT configuration.
resource "google_compute_router" "router" {
  name    = "crowdsource-data-app-router"
  network = google_compute_network.vpc.name
  region  = var.region
  project = var.project_id

  bgp {
    asn = 64514
  }
}

# ── Cloud NAT ─────────────────────────────────────────────────────────────────
# Allows private GKE nodes (no public IPs) to initiate outbound
# connections to the internet — for pulling container images from
# quay.io, docker.io, ghcr.io, and other external registries.
# Inbound connections are still blocked — NAT is outbound only.
resource "google_compute_router_nat" "nat" {
  name                               = "crowdsource-data-app-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  project                            = var.project_id

  # AUTO_ONLY = GCP manages the external IPs for NAT automatically
  # No need to reserve static IPs for NAT
  nat_ip_allocate_option             = "AUTO_ONLY"

  # Apply NAT to ALL subnets in the VPC — covers our GKE subnet
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  # Log only errors — reduces log noise and cost
  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}