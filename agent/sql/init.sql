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

-- No ANN index for now -- ivfflat's `lists` parameter needs to scale with
-- row count (pgvector's own guidance: roughly rows/1000, minimum ~1); the
-- sample corpus here is 9 rows (3 short .txt docs), and `lists = 100` on
-- that few rows left the index's clusters empty, so ORDER BY ... LIMIT
-- silently returned ZERO rows for every query (found 2026-08-13 wiring
-- the HTTP server -- document_search() looked "working" with no error,
-- just always empty). A plain sequential scan is instant at this corpus
-- size and always correct. Re-add an ivfflat/hnsw index once the real
-- corpus is large enough to size `lists` sensibly -- don't reintroduce it
-- at the current sample scale.

CREATE INDEX IF NOT EXISTS doc_chunks_source_idx ON doc_chunks (source_name);
