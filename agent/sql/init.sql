-- AeroFlux Analyst: RAG corpus schema
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS doc_chunks (
    id            BIGSERIAL PRIMARY KEY,
    source_name   TEXT NOT NULL,        -- e.g. "FAA_SWIM_Overview.pdf"
    source_url    TEXT,                 -- original source, for citations
    chunk_index   INT NOT NULL,
    content       TEXT NOT NULL,
    embedding     VECTOR(384),         -- adjust to match embedding model dim
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- Cosine-distance index for fast semantic search
CREATE INDEX IF NOT EXISTS doc_chunks_embedding_idx
    ON doc_chunks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS doc_chunks_source_idx ON doc_chunks (source_name);
