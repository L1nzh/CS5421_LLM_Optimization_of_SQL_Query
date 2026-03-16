from __future__ import annotations

import json
from hashlib import sha256
from time import perf_counter
from typing import Any

from config.settings import ValidationSettings
from execution.query_executor import QueryExecutor
from models import HashedResult, QueryStreamResult
from validator.result_normalizer import ResultNormalizer


class ResultHasher:
    """Incrementally hash normalized query results without materializing all rows."""

    def __init__(self, normalizer: ResultNormalizer, settings: ValidationSettings):
        self._normalizer = normalizer
        self._settings = settings

    def hash_stream(self, stream_result: QueryStreamResult) -> HashedResult:
        normalized_columns = self._normalizer.normalize_columns(stream_result.columns)
        if self._settings.preserve_row_order:
            row_count, row_digest = self._ordered_digest(stream_result.rows)
        else:
            row_count, row_digest = self._unordered_digest(stream_result.rows)

        final_payload = json.dumps(
            {
                "columns": normalized_columns,
                "row_count": row_count,
                "rows": row_digest,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return HashedResult(
            columns=normalized_columns,
            row_count=row_count,
            digest=sha256(final_payload.encode("utf-8")).hexdigest(),
        )

    def _ordered_digest(self, rows: Any) -> tuple[int, str]:
        hasher = sha256()
        row_count = 0
        for row in rows:
            normalized_row = self._normalizer.normalize_row(row)
            hasher.update(self._serialize_value(normalized_row))
            hasher.update(b"\n")
            row_count += 1
        return row_count, hasher.hexdigest()

    def _unordered_digest(self, rows: Any) -> tuple[int, str]:
        modulus = 1 << 256
        row_count = 0
        sum_hash = 0
        xor_hash = 0
        square_sum = 0

        for row in rows:
            normalized_row = self._normalizer.normalize_row(row)
            row_digest = sha256(self._serialize_value(normalized_row)).digest()
            row_value = int.from_bytes(row_digest, byteorder="big", signed=False)
            row_count += 1
            sum_hash = (sum_hash + row_value) % modulus
            xor_hash ^= row_value
            square_sum = (square_sum + pow(row_value, 2, modulus)) % modulus

        payload = json.dumps(
            {
                "sum": f"{sum_hash:064x}",
                "xor": f"{xor_hash:064x}",
                "square_sum": f"{square_sum:064x}",
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return row_count, sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _serialize_value(value: Any) -> bytes:
        return json.dumps(
            value,
            default=repr,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")


class HashingQueryExecutor:
    """Executor that computes low-memory result hashes over streamed rows."""

    def __init__(self, executor: QueryExecutor, hasher: ResultHasher, batch_size: int):
        self._executor = executor
        self._hasher = hasher
        self._batch_size = batch_size

    def execute(self, query: str) -> tuple[bool, HashedResult | None, float, str | None]:
        started = perf_counter()
        stream_result = self._executor.stream(query, batch_size=self._batch_size)
        if not stream_result.success:
            return False, None, (perf_counter() - started) * 1000, stream_result.error_message

        try:
            hashed_result = self._hasher.hash_stream(stream_result)
        except Exception as exc:
            return False, None, (perf_counter() - started) * 1000, str(exc)
        finally:
            stream_result.close()

        return True, hashed_result, (perf_counter() - started) * 1000, None
