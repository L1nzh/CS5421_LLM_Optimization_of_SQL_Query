from __future__ import annotations

from itertools import count
from time import perf_counter

from psycopg import connect
from psycopg.rows import tuple_row

from db.adapter import DatabaseAdapter
from models import QueryExecutionResult, QueryStreamResult


class PostgresAdapter(DatabaseAdapter):
    """psycopg3-backed adapter for PostgreSQL."""

    _cursor_counter = count()

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

    def stream_query(self, query: str, batch_size: int = 10_000) -> QueryStreamResult:
        cursor = self._connection.cursor(name=f"validator_stream_{next(self._cursor_counter)}")
        try:
            cursor.execute(query)
            columns = [desc.name for desc in cursor.description] if cursor.description else []
        except Exception as exc:  # pragma: no cover - adapter integration surface
            cursor.close()
            return QueryStreamResult(
                query=query,
                success=False,
                columns=[],
                rows=(),
                error_message=str(exc),
            )

        def row_iterator():
            try:
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    for row in batch:
                        yield row
            finally:
                cursor.close()

        return QueryStreamResult(
            query=query,
            success=True,
            columns=columns,
            rows=row_iterator(),
            close=cursor.close,
        )
