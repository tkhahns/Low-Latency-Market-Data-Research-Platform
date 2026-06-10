from __future__ import annotations

from market_platform.config import FEED_SOURCE


def make_feed_source(source: str | None = None):
    name = source or FEED_SOURCE
    if name == "synthetic":
        from market_platform.feeds.synthetic import SyntheticFeedSource
        return SyntheticFeedSource()
    if name == "coinbase":
        from market_platform.feeds.coinbase import CoinbaseFeedSource
        return CoinbaseFeedSource()
    if name == "databento":
        from market_platform.feeds.databento import DatabentoFeedSource
        return DatabentoFeedSource()
    if name == "replay":
        from market_platform.feeds.replay import ReplayFeedSource
        return ReplayFeedSource()
    raise ValueError(f"Unknown FEED_SOURCE: {name!r}. Choose: synthetic, coinbase, databento, replay")
