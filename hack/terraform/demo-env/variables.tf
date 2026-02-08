variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-east1"
}

variable "subnet_cidr" {
  description = "CIDR range for the subnet"
  type        = string
  default     = "10.0.0.0/28"
}

variable "psc_endpoint_ip" {
  description = "Internal IP address for the PSC endpoint (must not overlap with subnet_cidr)"
  type        = string
  default     = "10.0.1.0"
}
