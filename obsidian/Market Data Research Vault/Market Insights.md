---
title: Market Insights
tags: [research, backtesting, features]
---

# Market Insights

Research workflows should separate market-data infrastructure validation from trading signal claims.

Useful feature tables:

- Gold 1s bars for symbol-level replay.
- Spread features for liquidity and quality checks.
- Volatility features for short-window regime analysis.
- Data-quality annotations for filtering replay windows.

The cold path is designed for historical tick replay, backtesting-ready features, and research notebooks. It should not sit in the live API request path.
