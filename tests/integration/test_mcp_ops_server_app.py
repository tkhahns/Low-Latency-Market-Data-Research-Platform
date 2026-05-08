from fastapi.testclient import TestClient

from market_platform.ops.documents import DocumentChunk
from market_platform.ops.tools import OpsTools
from market_platform.ops.vector_store import InMemoryVectorStore
from market_platform.services.mcp_ops_server.app import app
from market_platform.time import utc_now_iso


def test_mcp_json_rpc_tools_list_and_call():
    store = InMemoryVectorStore()
    store.upsert_chunks(
        [
            DocumentChunk(
                source_uri="lakehouse:tables.yml",
                title="Lakehouse Tables",
                content="gold_bars_1s lineage from silver_trades and bronze_market_events.",
                tags=("lineage",),
                source_type="lakehouse",
                chunk_index=0,
                indexed_at=utc_now_iso(),
            )
        ]
    )
    original_tools = app.state.tools
    app.state.tools = OpsTools(evidence_store=store)
    try:
        client = TestClient(app)
        listed = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).json()
        assert "lineage_lookup" in listed["result"]["tools"]

        called = client.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "lineage_lookup", "arguments": {"table": "gold_bars_1s"}},
            },
        ).json()
        assert called["id"] == 2
        assert called["result"]["status"] == "ok"
        assert called["result"]["upstream"] == ["silver_trades"]
    finally:
        app.state.tools = original_tools
