from __future__ import annotations

from time import perf_counter

from psycopg import connect
from psycopg.rows import tuple_row

from db.adapter import DatabaseAdapter
from models import QueryExecutionResult


class PostgresAdapter(DatabaseAdapter):
    """psycopg3-backed adapter for PostgreSQL."""

    def __init__(self, dsn: str):
        self._connection = connect(dsn, row_factory=tuple_row)

    def execute_query(self, query: str) -> QueryExecutionResult:
        started = perf_counter()
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc.name for desc in cursor.description] if cursor.description else []
            execution_time_ms = (perf_counter() - started) * 1000
            return QueryExecutionResult(
                query=query,
                success=True,
                columns=columns,
                rows=rows,
                execution_time_ms=execution_time_ms,
                error_message=None,
            )
        except Exception as exc:  # pragma: no cover - adapter integration surface
            execution_time_ms = (perf_counter() - started) * 1000
            return QueryExecutionResult(
                query=query,
                success=False,
                columns=[],
                rows=[],
                execution_time_ms=execution_time_ms,
                error_message=str(exc),
            )

    def close(self) -> None:
        self._connection.close()
