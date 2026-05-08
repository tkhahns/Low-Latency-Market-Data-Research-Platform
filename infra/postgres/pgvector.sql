CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rag_documents (
  id BIGSERIAL PRIMARY KEY,
  source_uri TEXT NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  tags TEXT[] NOT NULL DEFAULT '{}',
  source_type TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL,
  embedding VECTOR(384) NOT NULL,
  UNIQUE (source_uri, chunk_index)
);

CREATE INDEX IF NOT EXISTS rag_documents_embedding_idx
  ON rag_documents USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS rag_documents_source_type_idx
  ON rag_documents (source_type);

CREATE TABLE IF NOT EXISTS mcp_tool_audit (
  id BIGSERIAL PRIMARY KEY,
  tool_name TEXT NOT NULL,
  caller TEXT NOT NULL,
  parameters JSONB NOT NULL,
  result_status TEXT NOT NULL,
  duration_ms INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
