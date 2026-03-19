from __future__ import annotations

from db.adapter import DatabaseAdapter
from models import QueryExecutionResult, QueryStreamResult


class QueryExecutor:
    """Thin execution service built around the database adapter."""

    def __init__(self, adapter: DatabaseAdapter):
        self._adapter = adapter

    def execute(self, query: str) -> QueryExecutionResult:
        return self._adapter.execute_query(query)

    def stream(self, query: str, batch_size: int = 10_000) -> QueryStreamResult:
        return self._adapter.stream_query(query, batch_size=batch_size)
