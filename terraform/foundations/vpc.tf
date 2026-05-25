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