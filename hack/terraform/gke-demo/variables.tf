variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for the VPC subnet and the (regional) Autopilot cluster"
  type        = string
  default     = "us-east1"
}

variable "subnet_cidr" {
  description = "Primary CIDR range for the GKE node subnet (kept distinct from bucket-demo-vpc's 10.0.0.0/28)"
  type        = string
  default     = "10.10.0.0/24"
}

variable "global_authorized_networks" {
  description = "CIDRs allowed to reach the public GKE control-plane endpoint (e.g. your workstation's or VPN's egress IP as x.x.x.x/32). Required: with private nodes and an authorized-networks list, nothing else can reach the API server."
  type = list(object({
    cidr_block   = string
    display_name = string
  }))

  validation {
    condition     = length(var.global_authorized_networks) > 0
    error_message = "Set at least one global_authorized_networks entry, e.g. [{ cidr_block = \"203.0.113.7/32\", display_name = \"my-laptop\" }]."
  }

  validation {
    condition     = alltrue([for n in var.global_authorized_networks : can(cidrhost(n.cidr_block, 0))])
    error_message = "Every global_authorized_networks entry must have a valid cidr_block, e.g. 203.0.113.7/32."
  }
}

variable "labels" {
  description = "Labels applied to all labelable resources (provider default_labels and cluster resource_labels; GKE propagates the latter to node VMs and disks). Useful when an organization policy requires specific labels on resource creation."
  type        = map(string)
  default     = {}
}

variable "k8s_namespace" {
  description = "Kubernetes namespace whose pods are granted GCS access via Workload Identity (where hack/bucket-dev deploys the overlay control plane)"
  type        = string
  default     = "overlay-system"
}

variable "artifact_registry_repo" {
  description = "Artifact Registry Docker repository ID for the bucket-proxy-server image and mirrored control-plane images"
  type        = string
  default     = "bucket-dev"
}
