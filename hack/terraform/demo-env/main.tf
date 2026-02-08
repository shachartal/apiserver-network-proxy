provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# VPC & Subnet
# -----------------------------------------------------------------------------

resource "google_compute_network" "vpc" {
  name                    = "bucket-demo-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "bucket-demo-subnet"
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
}

# -----------------------------------------------------------------------------
# Firewall Rules
# -----------------------------------------------------------------------------

# Deny all egress — baseline rule at lowest custom priority.
resource "google_compute_firewall" "deny_all_egress" {
  name      = "bucket-demo-deny-all-egress"
  network   = google_compute_network.vpc.id
  direction = "EGRESS"
  priority  = 65534

  deny {
    protocol = "all"
  }

  destination_ranges = ["0.0.0.0/0"]
}

# Allow egress to the PSC endpoint IP on TCP 443.
resource "google_compute_firewall" "allow_egress_to_psc" {
  name      = "bucket-demo-allow-egress-psc"
  network   = google_compute_network.vpc.id
  direction = "EGRESS"
  priority  = 1000

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  destination_ranges = ["${google_compute_global_address.psc_apis.address}/32"]
}

# Allow all traffic within the subnet.
resource "google_compute_firewall" "allow_internal" {
  name      = "bucket-demo-allow-internal"
  network   = google_compute_network.vpc.id
  direction = "INGRESS"
  priority  = 1000

  allow {
    protocol = "all"
  }

  source_ranges = [var.subnet_cidr]
}

# Allow SSH from IAP range (for accessing VMs without external IPs).
resource "google_compute_firewall" "allow_iap_ssh" {
  name      = "bucket-demo-allow-iap-ssh"
  network   = google_compute_network.vpc.id
  direction = "INGRESS"
  priority  = 1000

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
}

# -----------------------------------------------------------------------------
# Private Service Connect (PSC) Endpoint for Google APIs
# -----------------------------------------------------------------------------

resource "google_compute_global_address" "psc_apis" {
  name         = "bucket-demo-psc-apis"
  purpose      = "PRIVATE_SERVICE_CONNECT"
  address_type = "INTERNAL"
  address      = var.psc_endpoint_ip
  network      = google_compute_network.vpc.id
}

resource "google_compute_global_forwarding_rule" "psc_apis" {
  name                  = "bktdemopscapis"
  target                = "all-apis"
  network               = google_compute_network.vpc.id
  ip_address            = google_compute_global_address.psc_apis.id
  load_balancing_scheme = ""
}

# -----------------------------------------------------------------------------
# Private DNS — resolve *.googleapis.com to the PSC endpoint
# -----------------------------------------------------------------------------

resource "google_dns_managed_zone" "googleapis" {
  name        = "bucket-demo-googleapis"
  dns_name    = "googleapis.com."
  description = "Private DNS zone for Google APIs via PSC"
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.vpc.id
    }
  }
}

resource "google_dns_record_set" "googleapis_wildcard" {
  managed_zone = google_dns_managed_zone.googleapis.name
  name         = "*.googleapis.com."
  type         = "A"
  ttl          = 300
  rrdatas      = [google_compute_global_address.psc_apis.address]
}

# -----------------------------------------------------------------------------
# Service Account for Worker Node VMs
# -----------------------------------------------------------------------------

resource "google_service_account" "worker_node" {
  account_id   = "bucket-demo-worker"
  display_name = "Bucket Demo Worker Node"
}

resource "google_project_iam_member" "worker_gcs" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.worker_node.email}"
}
