from pathlib import Path

import yaml


def test_lakehouse_table_contracts_define_required_zones_and_lineage():
    contract = yaml.safe_load(Path("lakehouse/contracts/tables.yml").read_text())
    tables = contract["tables"]

    for table in [
        "bronze_market_events",
        "silver_trades",
        "silver_quotes",
        "silver_sequence_gaps",
        "gold_bars_1s",
        "gold_spread_features",
        "gold_volatility_features",
        "gold_quality_annotations",
    ]:
        assert table in tables
        assert "event_date" in tables[table]["partition_columns"]

    source_lineage_tables = {
        "bronze_market_events",
        "silver_trades",
        "silver_quotes",
        "silver_sequence_gaps",
        "gold_quality_annotations",
    }
    range_lineage_tables = {"gold_bars_1s", "gold_spread_features"}

    for table, spec in tables.items():
        required = set(spec["required_columns"])
        assert {"schema_version", "symbol", "exchange", "job_run_id", "processed_at"} <= required
        if table in source_lineage_tables:
            assert {"source_topic", "source_partition", "source_offset"} <= required
        if table in range_lineage_tables:
            assert {"first_source_offset", "last_source_offset"} <= required


def test_databricks_bundle_has_cold_path_jobs():
    bundle = yaml.safe_load(Path("lakehouse/databricks/bundle.yml").read_text())
    jobs = bundle["resources"]["jobs"]

    assert {
        "bronze_ingest",
        "silver_normalize",
        "gold_features",
        "quality_report",
        "replay_dry_run",
    } <= set(jobs)
