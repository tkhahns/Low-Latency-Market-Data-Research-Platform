# Gold Pipeline

Research-ready tables for backtesting and feature engineering.

## Tables

- `gold_bars_1s`
- `gold_spread_features`
- `gold_volatility_features`
- `gold_quality_annotations`

`gold_bars_1s` provides OHLCV and VWAP at symbol-exchange-second grain. `gold_spread_features` summarizes quote spreads over the same one-second grain. `gold_volatility_features` computes daily realized volatility from bar closes. `gold_quality_annotations` merges sequence gaps and quality alerts with table/source lineage.

Gold tables are stable research contracts and remain outside the live API request path.

Entrypoint: `lakehouse/jobs/gold_features.py`.
