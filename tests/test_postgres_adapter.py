from __future__ import annotations

from types import SimpleNamespace

import db.postgres_adapter as postgres_adapter_module
from db.postgres_adapter import PostgresAdapter


class _FakeCursor:
    def __init__(self, connection: "_FakeConnection", *, named: bool = False):
        self._connection = connection
        self._named = named
        self.description = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def execute(self, query: str) -> None:
        self._connection.executed_queries.append(query)
        action = self._connection.actions.pop(0)
        if isinstance(action, Exception):
            self._connection.aborted = True
            raise action

        columns, rows = action
        self.description = [SimpleNamespace(name=name) for name in columns]
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)

    def fetchmany(self, batch_size: int) -> list[tuple[object, ...]]:
        if not self._rows:
            return []
        batch = self._rows[:batch_size]
        self._rows = self._rows[batch_size:]
        return batch

    def close(self) -> None:
        self._connection.closed_cursors += 1


class _FakeConnection:
    def __init__(self, actions: list[object]):
        self.actions = list(actions)
        self.executed_queries: list[str] = []
        self.rollback_calls = 0
        self.closed_cursors = 0
        self.closed = False
        self.broken = False
        self.aborted = False

    def cursor(self, name: str | None = None) -> _FakeCursor:
        return _FakeCursor(self, named=name is not None)

    def rollback(self) -> None:
        self.rollback_calls += 1
        self.aborted = False

    def close(self) -> None:
        self.closed = True


def test_execute_query_rolls_back_after_failure_and_allows_next_query(monkeypatch) -> None:
    fake_connection = _FakeConnection(
        [
            RuntimeError("boom"),
            (["id"], [(1,), (2,)]),
        ]
    )
    monkeypatch.setattr(postgres_adapter_module, "connect", lambda dsn, row_factory=None: fake_connection)

    adapter = PostgresAdapter("postgresql://example")

    failed = adapter.execute_query("select broken")
    succeeded = adapter.execute_query("select ok")

    assert failed.success is False
    assert "boom" in (failed.error_message or "")
    assert fake_connection.rollback_calls == 1
    assert succeeded.success is True
    assert succeeded.columns == ["id"]
    assert succeeded.rows == [(1,), (2,)]


def test_stream_query_rolls_back_after_execute_failure(monkeypatch) -> None:
    fake_connection = _FakeConnection([RuntimeError("stream boom")])
    monkeypatch.setattr(postgres_adapter_module, "connect", lambda dsn, row_factory=None: fake_connection)

    adapter = PostgresAdapter("postgresql://example")
    result = adapter.stream_query("select broken")

    assert result.success is False
    assert "stream boom" in (result.error_message or "")
    assert fake_connection.rollback_calls == 1
