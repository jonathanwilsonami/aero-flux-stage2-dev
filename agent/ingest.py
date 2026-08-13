"""
Chunk and embed the sample document corpus into pgvector.

Run this once (and again any time you add/replace docs in data/sample_docs/):
    python ingest.py

Swap `embed_texts()` for Bedrock/Titan or another embedding provider later
without touching the rest of the pipeline -- the DB schema doesn't care.
"""
import os
import glob
import psycopg
from dotenv import load_dotenv
from embeddings import embed_texts

load_dotenv()

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://aeroflux:aeroflux_local_dev@localhost:5432/aeroflux_rag",
)
DOCS_DIR = os.path.join(os.path.dirname(__file__), "data", "sample_docs")
CHUNK_SIZE = 800   # characters; simple splitter is fine for a prototype corpus
CHUNK_OVERLAP = 100


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        chunks.append(text[start:end])
        start = end - overlap
    return [c.strip() for c in chunks if c.strip()]


def main():
    doc_paths = sorted(glob.glob(os.path.join(DOCS_DIR, "*.txt")))
    if not doc_paths:
        print(f"No .txt docs found in {DOCS_DIR}")
        return

    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            for path in doc_paths:
                source_name = os.path.basename(path)
                with open(path, "r", encoding="utf-8") as f:
                    text = f.read()

                chunks = chunk_text(text)
                embeddings = embed_texts(chunks)

                # Clear any previous chunks for this source before re-inserting
                cur.execute("DELETE FROM doc_chunks WHERE source_name = %s", (source_name,))

                for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
                    cur.execute(
                        """
                        INSERT INTO doc_chunks (source_name, source_url, chunk_index, content, embedding)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (source_name, None, i, chunk, emb),
                    )
                print(f"Ingested {len(chunks)} chunks from {source_name}")
        conn.commit()
    print("Done.")


if __name__ == "__main__":
    main()
