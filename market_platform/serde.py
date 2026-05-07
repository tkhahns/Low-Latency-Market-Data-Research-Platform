from __future__ import annotations

import json
from typing import Any


def dumps(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def loads(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode("utf-8"))

