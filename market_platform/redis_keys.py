def latest_quote(symbol: str) -> str:
    return f"md:latest_quote:{symbol.upper()}"


def top_of_book(symbol: str) -> str:
    return f"md:top_of_book:{symbol.upper()}"


def bar_1s(symbol: str) -> str:
    return f"md:bar:1s:{symbol.upper()}"


def freshness(symbol: str) -> str:
    return f"md:freshness:{symbol.upper()}"


def alerts(symbol: str) -> str:
    return f"md:alerts:{symbol.upper()}"


def active_symbols() -> str:
    return "md:symbols:active"

