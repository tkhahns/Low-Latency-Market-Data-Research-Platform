"""Vercel FastAPI entrypoint (see docs/vercel-deployment.md).

Vercel's FastAPI framework preset looks for an `app` variable in a root-level
main.py. Local/dev runs keep using the package path directly:
uvicorn market_platform.services.market_data_api.app:app
"""
from market_platform.services.market_data_api.app import app

__all__ = ["app"]
