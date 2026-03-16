from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from config.settings import ValidationSettings
from models import NormalizedResult, QueryExecutionResult
from utils.serialization import to_canonical_json


class ResultNormalizer:
    """Normalize execution results into deterministic, comparable values."""

    def __init__(self, settings: ValidationSettings):
        self._settings = settings

    def normalize(self, result: QueryExecutionResult) -> NormalizedResult:
        columns = self.normalize_columns(result.columns)
        rows = tuple(self.normalize_row(row) for row in result.rows)
        if not self._settings.preserve_row_order:
            rows = tuple(sorted(rows, key=self._row_sort_key))
        return NormalizedResult(columns=columns, rows=rows)

    def normalize_columns(self, columns: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        return tuple(self._normalize_column(column) for column in columns)

    def normalize_row(self, row: tuple[Any, ...]) -> tuple[Any, ...]:
        return tuple(self._normalize_value(value) for value in row)

    def _normalize_column(self, column: str) -> str:
        normalized = column.strip() if self._settings.trim_strings else column
        return normalized.lower() if self._settings.normalize_column_names else normalized

    def _normalize_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, float):
            return round(value, self._settings.effective_float_precision)
        if isinstance(value, Decimal):
            exponent = Decimal(f"1e-{self._settings.effective_float_precision}")
            return str(value.quantize(exponent, rounding=ROUND_HALF_UP))
        if isinstance(value, datetime):
            return value.isoformat(timespec="microseconds")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat(timespec="microseconds")
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).decode(
                self._settings.bytes_encoding,
                errors=self._settings.bytes_errors,
            )
        if isinstance(value, dict):
            return to_canonical_json(value)
        if isinstance(value, (list, tuple)):
            return tuple(self._normalize_value(item) for item in value)
        if isinstance(value, str):
            normalized = value.replace("\r\n", "\n").replace("\r", "\n")
            return normalized.strip() if self._settings.trim_strings else normalized
        return value

    @staticmethod
    def _row_sort_key(row: tuple[Any, ...]) -> tuple[str, ...]:
        return tuple(repr(value) for value in row)
