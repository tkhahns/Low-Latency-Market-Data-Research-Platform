"""Vercel serverless entrypoint for the Market Data API.

Vercel's Python runtime serves any ASGI app exported as `app` from a file
under `api/`. The repo root is added to sys.path so the in-repo
`market_platform` package resolves without an install step.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from market_platform.services.market_data_api.app import app  # noqa: E402,F401
