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
    assert {"feed-ingestor", "feed-handler", "stream-processor", "market-data-api", "mcp-ops-server", "research-ingestor"} <= names

    services = [doc for doc in load_yaml_all("infra/kubernetes/base/services.yaml") if doc["kind"] == "Service"]
    assert {"market-data-api", "mcp-ops-server", "feed-ingestor", "research-ingestor"} <= {service["metadata"]["name"] for service in services}

    secret = load_yaml("infra/kubernetes/base/secret-template.yaml")
    assert {"POSTGRES_DSN", "RAG_POSTGRES_DSN", "DATABRICKS_TOKEN", "PROVIDER_API_KEY", "ANTHROPIC_API_KEY"} <= set(secret["stringData"])


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

    research_alerts = load_yaml("observability/alerts/research-alerts.yml")
    research_alert_names = {rule["alert"] for group in research_alerts["groups"] for rule in group["rules"]}
    assert {"ResearchIngestStalled", "ResearchBudgetExhausted"} <= research_alert_names

    metrics = load_yaml("observability/metrics.yml")
    metric_names = {metric["name"] for metric in metrics["metrics"]}
    assert {"market_feed_events_total", "market_symbol_freshness_lag_ms", "http_server_duration_seconds"} <= metric_names
    assert {"research_docs_ingested_total", "research_llm_spend_usd_total", "research_ingest_lag_seconds"} <= metric_names

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
        "docs/runbooks/research-ingest-stalled.md",
        "docs/backup-recovery.md",
        "docs/production-readiness.md",
        "infra/gcp/README.md",
        "infra/secrets/README.md",
        "infra/terraform/main.tf",
    ]:
        assert (ROOT / path).exists(), path


def test_research_contracts_and_fixture_exist():
    for path in [
        "contracts/research/research.document.v1.schema.json",
        "contracts/research/research.insight.v1.schema.json",
        "market_platform/fixtures/research-sample.jsonl",
        "docs/decisions/research-intelligence.md",
    ]:
        assert (ROOT / path).exists(), path


def test_research_mcp_tools_registered():
    import yaml as _yaml
    from pathlib import Path as _Path

    tools_list_url = "market_platform/services/mcp_ops_server/app.py"
    content = (_Path(__file__).resolve().parents[2] / tools_list_url).read_text(encoding="utf-8")
    for tool in ("research_search", "symbol_research_context", "research_digest"):
        assert tool in content, f"MCP tool {tool!r} not found in mcp_ops_server/app.py"
