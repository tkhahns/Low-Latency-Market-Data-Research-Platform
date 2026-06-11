def latest_quote(symbol: str) -> str:
    return f"md:latest_quote:{symbol.upper()}"


def top_of_book(symbol: str) -> str:
    return f"md:top_of_book:{symbol.upper()}"


def bar_1s(symbol: str) -> str:
    return f"md:bar:1s:{symbol.upper()}"


def freshness(symbol: str) -> str:
    return f"md:freshness:{symbol.upper()}"


def metrics(symbol: str) -> str:
    return f"md:metrics:{symbol.upper()}"


def alerts(symbol: str) -> str:
    return f"md:alerts:{symbol.upper()}"


def active_symbols() -> str:
    return "md:symbols:active"


def research_symbol(symbol: str) -> str:
    return f"md:research:{symbol.upper()}"


def research_digest() -> str:
    return "md:research:digest"


def research_llm_spend(date_str: str) -> str:
    """date_str: YYYY-MM-DD"""
    return f"md:research:llm_spend:{date_str}"
