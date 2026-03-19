from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from config.settings import ValidationSettings
from models import QueryExecutionResult
from validator.result_normalizer import ResultNormalizer


def test_normalizer_rounds_floats_from_tolerance() -> None:
    settings = ValidationSettings(float_tolerance=1e-3)
    normalizer = ResultNormalizer(settings)
    result = QueryExecutionResult(
        query="select 1",
        success=True,
        columns=["value"],
        rows=[(1.2349,)],
        execution_time_ms=1.0,
    )

    normalized = normalizer.normalize(result)

    assert normalized.rows == ((1.235,),)


def test_normalizer_sorts_rows_when_unordered() -> None:
    settings = ValidationSettings(preserve_row_order=False)
    normalizer = ResultNormalizer(settings)
    result = QueryExecutionResult(
        query="select id from items",
        success=True,
        columns=["id"],
        rows=[(3,), (1,), (2,)],
        execution_time_ms=1.0,
    )

    normalized = normalizer.normalize(result)

    assert normalized.rows == ((1,), (2,), (3,))


def test_normalizer_normalizes_strings_and_newlines() -> None:
    settings = ValidationSettings(trim_strings=True)
    normalizer = ResultNormalizer(settings)
    result = QueryExecutionResult(
        query="select text",
        success=True,
        columns=[" text "],
        rows=[("  hello\r\nworld  ",)],
        execution_time_ms=1.0,
    )

    normalized = normalizer.normalize(result)

    assert normalized.columns == ("text",)
    assert normalized.rows == (("hello\nworld",),)


def test_normalizer_decodes_bytes() -> None:
    normalizer = ResultNormalizer(ValidationSettings())
    result = QueryExecutionResult(
        query="select blob",
        success=True,
        columns=["blob"],
        rows=[(b"payload",)],
        execution_time_ms=1.0,
    )

    normalized = normalizer.normalize(result)

    assert normalized.rows == (("payload",),)


def test_normalizer_serializes_datetime_and_decimal() -> None:
    normalizer = ResultNormalizer(ValidationSettings(float_tolerance=1e-4))
    timestamp = datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
    result = QueryExecutionResult(
        query="select ts, amount",
        success=True,
        columns=["ts", "amount"],
        rows=[(timestamp, Decimal("4.56789"))],
        execution_time_ms=1.0,
    )

    normalized = normalizer.normalize(result)

    assert normalized.rows == ((timestamp.isoformat(timespec="microseconds"), "4.5679"),)
