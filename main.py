"""Vercel FastAPI entrypoint (see docs/vercel-deployment.md).

Vercel's FastAPI preset loads `app` from a root-level main.py. The custom
installCommand in vercel.json installs the slim requirements.txt instead of
the full pyproject.toml dependency closure (too heavy for a serverless
bundle). Local/dev runs keep using:
uvicorn market_platform.services.market_data_api.app:app
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from market_platform.services.market_data_api.app import app  # noqa: E402

__all__ = ["app"]
