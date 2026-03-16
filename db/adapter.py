from __future__ import annotations

from abc import ABC, abstractmethod

from models import QueryExecutionResult


class DatabaseAdapter(ABC):
    """Abstract database execution contract."""

    @abstractmethod
    def execute_query(self, query: str) -> QueryExecutionResult:
        """Execute a SQL query and return a structured result."""

    @abstractmethod
    def close(self) -> None:
        """Release database resources."""
