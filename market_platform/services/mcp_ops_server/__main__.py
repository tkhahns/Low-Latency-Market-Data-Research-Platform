from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run("market_platform.services.mcp_ops_server.app:app", host="0.0.0.0", port=8010)


if __name__ == "__main__":
    main()
