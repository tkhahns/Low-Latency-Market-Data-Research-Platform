from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional, Union

from fastapi import FastAPI
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from market_platform.config import REDIS_URL
from market_platform.ops.audit import JsonlAuditLog
from market_platform.ops.documents import iter_markdown_chunks
from market_platform.ops.tools import OpsTools
from market_platform.ops.vector_store import InMemoryVectorStore, JsonVectorStore, PostgresVectorStore


class ToolRequest(BaseModel):
    parameters: dict[str, Any] = Field(default_factory=dict)
    caller: str = "http"


class IndexRequest(BaseModel):
    paths: list[str]
    source_type: str = "obsidian"


class JsonRpcRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[Union[str, int]] = None
    method: str
    params: dict[str, Any] = Field(default_factory=dict)


def build_store():
    postgres_dsn = os.getenv("RAG_POSTGRES_DSN")
    if postgres_dsn:
        return PostgresVectorStore(postgres_dsn)
    json_path = os.getenv("RAG_JSON_STORE")
    if json_path:
        return JsonVectorStore(Path(json_path))
    return InMemoryVectorStore()


async def redis_factory():
    return Redis.from_url(REDIS_URL, decode_responses=True)


app = FastAPI(title="MCP Ops Server", version="0.1.0")
app.state.evidence_store = build_store()
app.state.tools = OpsTools(
    evidence_store=app.state.evidence_store,
    redis_factory=redis_factory,
    audit_log=JsonlAuditLog(Path(os.getenv("MCP_AUDIT_LOG", "var/audit/mcp-tools.jsonl"))),
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tools")
async def list_tools() -> dict[str, list[str]]:
    return {
        "tools": [
            "check_symbol_freshness",
            "explain_sequence_gap",
            "run_replay_dry_run",
            "compare_live_vs_replay",
            "summarize_incident",
            "lineage_lookup",
        ]
    }


@app.post("/tools/{tool_name}")
async def call_tool(tool_name: str, request: ToolRequest) -> dict[str, Any]:
    return await app.state.tools.call(tool_name, request.parameters, caller=request.caller)


@app.post("/mcp")
async def mcp_json_rpc(request: JsonRpcRequest) -> dict[str, Any]:
    if request.method == "tools/list":
        result = await list_tools()
    elif request.method == "tools/call":
        tool_name = request.params.get("name")
        arguments = request.params.get("arguments", {})
        caller = request.params.get("caller", "mcp-json-rpc")
        if not tool_name:
            result = {"status": "error", "error_code": "missing_tool_name", "sources": [], "duration_ms": 0}
        else:
            result = await app.state.tools.call(tool_name, arguments, caller=caller)
    else:
        result = {"status": "error", "error_code": "unknown_method", "sources": [], "duration_ms": 0}
    return {"jsonrpc": "2.0", "id": request.id, "result": result}


@app.post("/index")
async def index_documents(request: IndexRequest) -> dict[str, Any]:
    chunks = iter_markdown_chunks([Path(path) for path in request.paths], source_type=request.source_type)
    count = app.state.evidence_store.upsert_chunks(chunks)
    return {"status": "ok", "indexed_chunks": count, "source_type": request.source_type}
