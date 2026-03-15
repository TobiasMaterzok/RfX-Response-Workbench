from __future__ import annotations

from app.models.entities import (
    CaseProfileItem,
    HistoricalCaseProfile,
    HistoricalQARow,
    PdfChunk,
    ProductTruthChunk,
)
from app.models.vector import EMBEDDING_VECTOR_DIMENSIONS


def test_pgvector_backed_columns_expose_cosine_distance_comparator() -> None:
    probe = [0.0] * EMBEDDING_VECTOR_DIMENSIONS
    expressions = [
        PdfChunk.embedding.cosine_distance(probe),
        HistoricalQARow.embedding.cosine_distance(probe),
        HistoricalCaseProfile.signature_embedding.cosine_distance(probe),
        ProductTruthChunk.embedding.cosine_distance(probe),
        CaseProfileItem.embedding.cosine_distance(probe),
    ]
    assert all(expression is not None for expression in expressions)
