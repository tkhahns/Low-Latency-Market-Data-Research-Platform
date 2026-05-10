import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def load_yaml(path):
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


def load_yaml_all(path):
    return [doc for doc in yaml.safe_load_all((ROOT / path).read_text(encoding="utf-8")) if doc]


def test_ci_workflow_covers_python_docker_and_flink():
    workflow = load_yaml(".github/workflows/ci.yml")
    jobs = workflow["jobs"]

    assert {"python", "docker", "flink"} <= set(jobs)
    python_steps = "\n".join(str(step) for step in jobs["python"]["steps"])
    assert "ruff check" in python_steps
    assert "pytest" in python_steps
    assert "validate-production-artifacts.py" in python_steps
    docker_steps = "\n".join(str(step) for step in jobs["docker"]["steps"])
    assert "infra/python-service.Dockerfile" in docker_steps
    assert "infra/flink-job.Dockerfile" in docker_steps

    dockerfile = (ROOT / "infra/python-service.Dockerfile").read_text(encoding="utf-8")
    assert "COPY contracts" in dockerfile
    assert "COPY lakehouse/contracts" in dockerfile


def test_kubernetes_manifests_define_runtime_services_and_secrets():
    deployments = [doc for doc in load_yaml_all("infra/kubernetes/base/deployments.yaml") if doc["kind"] == "Deployment"]
    names = {deployment["metadata"]["name"] for deployment in deployments}
    assert {"feed-simulator", "feed-handler", "stream-processor", "market-data-api", "mcp-ops-server"} <= names

    services = [doc for doc in load_yaml_all("infra/kubernetes/base/services.yaml") if doc["kind"] == "Service"]
    assert {service["metadata"]["name"] for service in services} == {"market-data-api", "mcp-ops-server"}

    secret = load_yaml("infra/kubernetes/base/secret-template.yaml")
    assert {"POSTGRES_DSN", "RAG_POSTGRES_DSN", "DATABRICKS_TOKEN", "PROVIDER_API_KEY"} <= set(secret["stringData"])


def test_observability_assets_include_dashboards_alerts_and_log_schema():
    dashboard = json.loads((ROOT / "observability/grafana/market-data-dashboard.json").read_text(encoding="utf-8"))
    assert dashboard["title"] == "Market Data Platform Health"
    assert len(dashboard["panels"]) >= 6

    alerts = load_yaml("observability/alerts/market-data-alerts.yml")
    alert_names = {rule["alert"] for group in alerts["groups"] for rule in group["rules"]}
    assert {
        "SymbolFreshnessStale",
        "SequenceGapDetected",
        "FlinkCheckpointFailures",
        "RedisDivergence",
        "DatabricksJobFailure",
        "McpToolFailure",
    } <= alert_names

    metrics = load_yaml("observability/metrics.yml")
    metric_names = {metric["name"] for metric in metrics["metrics"]}
    assert {"market_feed_events_total", "market_symbol_freshness_lag_ms", "http_server_duration_seconds"} <= metric_names

    log_schema = json.loads((ROOT / "observability/logging-schema.json").read_text(encoding="utf-8"))
    assert {"timestamp", "level", "service", "message"} <= set(log_schema["required"])


def test_runbooks_backup_and_gcp_docs_exist():
    for path in [
        "docs/runbooks/stale-symbol.md",
        "docs/runbooks/sequence-gap.md",
        "docs/runbooks/flink-failure.md",
        "docs/runbooks/redis-divergence.md",
        "docs/runbooks/databricks-job-failure.md",
        "docs/runbooks/mcp-tool-failure.md",
        "docs/backup-recovery.md",
        "docs/production-readiness.md",
        "infra/gcp/README.md",
        "infra/secrets/README.md",
        "infra/terraform/main.tf",
    ]:
        assert (ROOT / path).exists(), path
