"""Watchlist symbol → keyword mapping for research tagging."""
from __future__ import annotations

import json
import re
from typing import Dict, List, Optional

from market_platform.config import COINBASE_PRODUCTS, RESEARCH_SYMBOL_MAP_JSON

_BUILTIN_MAP: Dict[str, List[str]] = {
    "BTC-USD": ["bitcoin", "btc", "₿", "bitcoin etf", "spot bitcoin", "satoshi", "nakamoto"],
    "ETH-USD": ["ethereum", "eth", "ether", "vitalik", "evm", "smart contract", "defi"],
    "SOL-USD": ["solana", "sol", "solana labs", "phantom"],
    "AAPL": ["apple", "aapl", "tim cook", "iphone", "app store", "mac"],
    "MSFT": ["microsoft", "msft", "satya nadella", "azure", "copilot"],
    "NVDA": ["nvidia", "nvda", "jensen huang", "cuda", "h100", "gpu"],
}


def _load_symbol_map() -> Dict[str, List[str]]:
    if RESEARCH_SYMBOL_MAP_JSON:
        try:
            return json.loads(RESEARCH_SYMBOL_MAP_JSON)
        except (json.JSONDecodeError, ValueError):
            pass
    # Return only entries for configured products/symbols
    configured = set(COINBASE_PRODUCTS)
    from market_platform.config import SYMBOLS
    configured.update(SYMBOLS)
    result = {sym: kws for sym, kws in _BUILTIN_MAP.items() if sym in configured}
    if not result:
        result = dict(_BUILTIN_MAP)
    return result


SYMBOL_MAP: Dict[str, List[str]] = _load_symbol_map()


def symbols_for_text(text: str) -> List[str]:
    """Return watchlist symbols whose keywords appear in text (case-insensitive)."""
    lower = text.lower()
    matched = []
    for symbol, keywords in SYMBOL_MAP.items():
        for kw in keywords:
            if re.search(r"\b" + re.escape(kw) + r"\b", lower):
                matched.append(symbol)
                break
    return matched
