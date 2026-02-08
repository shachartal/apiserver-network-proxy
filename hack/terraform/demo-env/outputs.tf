output "vpc_name" {
  description = "Name of the VPC"
  value       = google_compute_network.vpc.name
}

output "vpc_self_link" {
  description = "Self-link of the VPC"
  value       = google_compute_network.vpc.self_link
}

output "subnet_self_link" {
  description = "Self-link of the subnet"
  value       = google_compute_subnetwork.subnet.self_link
}

output "psc_endpoint_ip" {
  description = "IP address of the PSC endpoint for Google APIs"
  value       = google_compute_global_address.psc_apis.address
}

output "worker_service_account_email" {
  description = "Email of the worker node service account"
  value       = google_service_account.worker_node.email
}
