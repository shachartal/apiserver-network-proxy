output "vpc_name" {
  description = "Name of the GKE VPC"
  value       = google_compute_network.vpc.name
}

output "subnet_self_link" {
  description = "Self-link of the GKE node subnet"
  value       = google_compute_subnetwork.subnet.self_link
}

output "cluster_name" {
  description = "Name of the GKE Autopilot cluster"
  value       = google_container_cluster.autopilot.name
}

output "cluster_location" {
  description = "Region of the GKE Autopilot cluster"
  value       = google_container_cluster.autopilot.location
}

output "get_credentials_command" {
  description = "Command to configure kubectl for this cluster"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.autopilot.name} --region ${google_container_cluster.autopilot.location} --project ${var.project_id}"
}

output "comms_bucket_name" {
  description = "Name of the comms bucket (set GCS_BUCKET in hack/bucket-dev/demo.env to this)"
  value       = google_storage_bucket.comms.name
}

output "artifact_registry_url" {
  description = "Image path prefix (set GKE_AR_REPO in hack/bucket-dev/demo.env to this)"
  value       = "${google_artifact_registry_repository.images.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "gcs_principal" {
  description = "Workload Identity principal granted GCS access"
  value       = google_project_iam_member.pods_gcs.member
}
