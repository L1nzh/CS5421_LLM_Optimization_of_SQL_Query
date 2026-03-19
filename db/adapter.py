from __future__ import annotations

from abc import ABC, abstractmethod

from models import QueryExecutionResult, QueryStreamResult


class DatabaseAdapter(ABC):
    """Abstract database execution contract."""

    @abstractmethod
    def execute_query(self, query: str) -> QueryExecutionResult:
        """Execute a SQL query and return a structured result."""

    def stream_query(self, query: str, batch_size: int = 10_000) -> QueryStreamResult:
        """Stream a SQL query result. Adapters can override for low-memory execution."""
        result = self.execute_query(query)
        return QueryStreamResult(
            query=result.query,
            success=result.success,
            columns=result.columns,
            rows=iter(result.rows),
            error_message=result.error_message,
        )

    @abstractmethod
    def close(self) -> None:
        """Release database resources."""
