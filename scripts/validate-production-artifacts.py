from __future__ import annotations

import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def read_yaml(path: str) -> object:
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


def read_yaml_all(path: str) -> list[object]:
    return [doc for doc in yaml.safe_load_all((ROOT / path).read_text(encoding="utf-8")) if doc]


def read_json(path: str) -> object:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def require(path: str) -> Path:
    full_path = ROOT / path
    if not full_path.exists():
        raise AssertionError(f"Missing required artifact: {path}")
    return full_path


def validate_ci() -> None:
    workflow = read_yaml(".github/workflows/ci.yml")
    jobs = workflow["jobs"]
    for job in ["python", "docker", "flink"]:
        if job not in jobs:
            raise AssertionError(f"Missing CI job: {job}")


def validate_kubernetes() -> None:
    kustomization = read_yaml("infra/kubernetes/base/kustomization.yaml")
    resources = set(kustomization["resources"])
    expected = {"namespace.yaml", "configmap.yaml", "secret-template.yaml", "deployments.yaml", "services.yaml"}
    if expected - resources:
        raise AssertionError(f"Kustomization missing resources: {sorted(expected - resources)}")
    deployments = [doc for doc in read_yaml_all("infra/kubernetes/base/deployments.yaml") if doc.get("kind") == "Deployment"]
    names = {deployment["metadata"]["name"] for deployment in deployments}
    expected_names = {"feed-simulator", "feed-handler", "stream-processor", "market-data-api", "mcp-ops-server"}
    if expected_names - names:
        raise AssertionError(f"Missing deployments: {sorted(expected_names - names)}")


def validate_observability() -> None:
    dashboard = read_json("observability/grafana/market-data-dashboard.json")
    if len(dashboard.get("panels", [])) < 6:
        raise AssertionError("Grafana dashboard must include core health panels")
    alerts = read_yaml("observability/alerts/market-data-alerts.yml")
    rule_names = {rule["alert"] for group in alerts["groups"] for rule in group["rules"]}
    expected = {
        "SymbolFreshnessStale",
        "SequenceGapDetected",
        "FlinkCheckpointFailures",
        "RedisDivergence",
        "DatabricksJobFailure",
        "McpToolFailure",
    }
    if expected - rule_names:
        raise AssertionError(f"Missing alert rules: {sorted(expected - rule_names)}")
    metrics = read_yaml("observability/metrics.yml")
    metric_names = {metric["name"] for metric in metrics["metrics"]}
    required_metrics = {"market_feed_events_total", "market_symbol_freshness_lag_ms", "http_server_duration_seconds", "mcp_tool_calls_total"}
    if required_metrics - metric_names:
        raise AssertionError(f"Missing metrics: {sorted(required_metrics - metric_names)}")
    read_json("observability/logging-schema.json")


def validate_docs() -> None:
    for path in [
        "docs/runbooks/stale-symbol.md",
        "docs/runbooks/sequence-gap.md",
        "docs/runbooks/flink-failure.md",
        "docs/runbooks/redis-divergence.md",
        "docs/runbooks/databricks-job-failure.md",
        "docs/runbooks/mcp-tool-failure.md",
        "docs/backup-recovery.md",
        "docs/performance.md",
        "docs/production-readiness.md",
        "infra/secrets/README.md",
        "infra/gcp/README.md",
        "infra/images/README.md",
    ]:
        require(path)


def validate_terraform_and_secrets() -> None:
    for path in [
        "infra/terraform/main.tf",
        "infra/terraform/variables.tf",
        "infra/terraform/outputs.tf",
        "infra/kubernetes/base/secret-template.yaml",
    ]:
        require(path)


def main() -> None:
    validate_ci()
    validate_kubernetes()
    validate_observability()
    validate_docs()
    validate_terraform_and_secrets()
    print("production-artifacts-ok")


if __name__ == "__main__":
    main()
