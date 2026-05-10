variable "project_id" {
  type        = string
  description = "GCP project id."
}

variable "region" {
  type        = string
  description = "Primary GCP region."
  default     = "us-central1"
}

variable "environment" {
  type        = string
  description = "Deployment environment name."
  default     = "prod"
}
