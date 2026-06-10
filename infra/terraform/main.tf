terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_artifact_registry_repository" "market_data" {
  location      = var.region
  repository_id = "market-data"
  description   = "Container images for the low-latency market data platform."
  format        = "DOCKER"
}

resource "google_service_account" "workloads" {
  account_id   = "market-data-workloads"
  display_name = "Market data workload identity"
}

resource "google_redis_instance" "hot_cache" {
  name           = "market-data-redis-${var.environment}"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = var.region
  redis_version  = "REDIS_7_0"
}

resource "google_sql_database_instance" "postgres" {
  name             = "market-data-postgres-${var.environment}"
  database_version = "POSTGRES_16"
  region           = var.region
  settings {
    tier = "db-custom-2-7680"
    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }
  }
}

resource "google_sql_database" "market_ops" {
  name     = "market_ops"
  instance = google_sql_database_instance.postgres.name
}

resource "google_container_cluster" "market_data" {
  name                = "market-data-${var.environment}"
  location            = var.region
  enable_autopilot    = true
  deletion_protection = true
}

resource "google_storage_bucket" "flink_checkpoints" {
  name                        = "${var.project_id}-flink-checkpoints-${var.environment}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }
}

resource "google_storage_bucket_iam_member" "flink_checkpoints_writer" {
  bucket = google_storage_bucket.flink_checkpoints.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.workloads.email}"
}

resource "google_secret_manager_secret" "required" {
  for_each = toset([
    "POSTGRES_DSN",
    "RAG_POSTGRES_DSN",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "PROVIDER_API_KEY",
    "OPENAI_API_KEY",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "API_KEYS"
  ])
  secret_id = "market-data-${lower(each.key)}-${var.environment}"
  replication {
    auto {}
  }
}
