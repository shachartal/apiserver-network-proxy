provider "google" {
  project = var.project_id
  region  = var.region

  # Optional labels on every labelable resource (see var.labels).
  default_labels = var.labels
}

data "google_project" "this" {
  project_id = var.project_id
}

# -----------------------------------------------------------------------------
# VPC & Subnet — fully separate from bucket-demo-vpc, no peering.
#
# Nodes get no external IPs and there is no Cloud NAT, so the only egress path
# is Private Google Access (GCS, Artifact Registry, other Google APIs) via the
# VPC's auto-created default route. Do not delete that default route: PGA
# depends on it despite it looking like an internet route.
# -----------------------------------------------------------------------------

resource "google_compute_network" "vpc" {
  name                    = "gke-demo-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "gke-demo-subnet"
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
}

# -----------------------------------------------------------------------------
# GKE Autopilot cluster (regional — Autopilot does not support zonal).
#
# - Private nodes (no external IPs).
# - Public control-plane endpoint restricted to var.global_authorized_networks
#   so kubectl works from your workstation (or VPN egress) only.
# - Pod/Service secondary ranges are auto-allocated by GKE.
# - Workload Identity is always on for Autopilot.
# -----------------------------------------------------------------------------

resource "google_container_cluster" "autopilot" {
  name     = "gke-demo"
  location = var.region

  enable_autopilot    = true
  deletion_protection = false

  # Cluster resource labels are propagated by GKE to the node VMs and disks it
  # creates (default_labels alone does not reach them).
  resource_labels = var.labels

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.subnet.id

  ip_allocation_policy {}

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
  }

  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.global_authorized_networks
      content {
        cidr_block   = cidr_blocks.value.cidr_block
        display_name = cidr_blocks.value.display_name
      }
    }
  }
}

# -----------------------------------------------------------------------------
# Artifact Registry — holds the bucket-proxy-server image and mirrors of the
# registry.k8s.io control-plane images. The private nodes cannot reach
# registry.k8s.io (no NAT), but *.pkg.dev is covered by Private Google Access.
# Images are pushed by hack/bucket-dev/setup-control-plane.sh and
# redeploy-server.sh (UNDERLAY=gke).
# -----------------------------------------------------------------------------

resource "google_artifact_registry_repository" "images" {
  repository_id = var.artifact_registry_repo
  location      = var.region
  format        = "DOCKER"
  description   = "bucket-dev overlay control-plane images"
}

# Autopilot nodes run as the Compute Engine default SA. Grant it pull access
# explicitly: some organizations disable the automatic Editor grant.
resource "google_artifact_registry_repository_iam_member" "nodes_pull" {
  repository = google_artifact_registry_repository.images.name
  location   = google_artifact_registry_repository.images.location
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${data.google_project.this.number}-compute@developer.gserviceaccount.com"
}

# -----------------------------------------------------------------------------
# Comms bucket — the transport between control plane and nodes.
#
# Same project and region as the cluster, so the project-level binding below
# (and bucket-demo-worker's project-level storage.admin in demo-env) cover it,
# and there is no cross-region egress.
#
# Bucket names are globally unique, so the project ID is prefixed.
# -----------------------------------------------------------------------------

resource "google_storage_bucket" "comms" {
  name     = "${var.project_id}-comms-bucket"
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Demo: allow `terraform destroy` even when transport messages remain.
  force_destroy = true
}

# -----------------------------------------------------------------------------
# GCS access for pods via Workload Identity Federation (direct principal
# binding — no GSA, no KSA annotation). Any pod in var.k8s_namespace can use
# GCS through ADC.
#
# The <project>.svc.id.goog pool is created with the first WI-enabled cluster,
# hence the explicit dependency.
#
# bucket-proxy-server needs object read/write/delete/list AND
# storage.buckets.get: NewGCSStore validates the bucket with Attrs() at startup
# (pkg/bucket/gcsstore.go). Neither roles/storage.objectAdmin nor
# roles/storage.objectUser includes buckets.get. storage.admin does, and matches
# what bucket-demo-worker already has in demo-env (broad, but demo-only).
resource "google_project_iam_member" "pods_gcs" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "principalSet://iam.googleapis.com/projects/${data.google_project.this.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/namespace/${var.k8s_namespace}"

  depends_on = [google_container_cluster.autopilot]
}
