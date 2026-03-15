from __future__ import annotations

from typing import Any

from pgvector.sqlalchemy import Vector  # type: ignore[import-untyped]
from sqlalchemy.types import JSON, TypeDecorator

EMBEDDING_VECTOR_DIMENSIONS = 1536


class EmbeddingVector(TypeDecorator[list[float] | None]):
    """Use pgvector on PostgreSQL and JSON elsewhere for local unit tests."""

    impl = JSON
    cache_ok = True
    comparator_factory = Vector(EMBEDDING_VECTOR_DIMENSIONS).comparator_factory

    def load_dialect_impl(self, dialect):  # type: ignore[no-untyped-def]
        if dialect.name == "postgresql":
            return dialect.type_descriptor(Vector(EMBEDDING_VECTOR_DIMENSIONS))
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: list[float] | None, dialect: Any) -> list[float] | None:
        return value

    def process_result_value(self, value: Any, dialect: Any) -> list[float] | None:
        if value is None:
            return None
        if isinstance(value, list):
            return [float(item) for item in value]
        return value
