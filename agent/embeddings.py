"""
Single place to swap embedding providers. Both ingest.py (write path) and
tools.py (read path) import from here so they can never drift out of sync.
"""
from typing import List

EMBEDDING_DIM = 384  # must match sql/init.sql VECTOR(384)

from sentence_transformers import SentenceTransformer
_model = SentenceTransformer("all-MiniLM-L6-v2")


def embed_texts(texts: List[str]) -> List[List[float]]:
    return _model.encode(texts, normalize_embeddings=True).tolist()
