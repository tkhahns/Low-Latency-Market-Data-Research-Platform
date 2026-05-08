from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from market_platform.time import utc_now_iso


@dataclass(frozen=True)
class AuditRecord:
    tool_name: str
    caller: str
    parameters: dict[str, Any]
    result_status: str
    duration_ms: int
    timestamp: str


class JsonlAuditLog:
    def __init__(self, path: Path) -> None:
        self.path = path

    def write(self, record: AuditRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.open("a", encoding="utf-8").write(json.dumps(record.__dict__, sort_keys=True) + "\n")


def audit_record(
    *,
    tool_name: str,
    caller: str,
    parameters: dict[str, Any],
    result_status: str,
    duration_ms: int,
) -> AuditRecord:
    return AuditRecord(
        tool_name=tool_name,
        caller=caller,
        parameters=parameters,
        result_status=result_status,
        duration_ms=duration_ms,
        timestamp=utc_now_iso(),
    )
