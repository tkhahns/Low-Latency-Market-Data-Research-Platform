output "artifact_registry_repository" {
  value = google_artifact_registry_repository.market_data.name
}

output "gke_cluster" {
  value = google_container_cluster.market_data.name
}

output "redis_host" {
  value = google_redis_instance.hot_cache.host
}

output "postgres_connection_name" {
  value = google_sql_database_instance.postgres.connection_name
}
