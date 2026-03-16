from __future__ import annotations

from collections import Counter
from hashlib import sha256
import json

from models import NormalizedResult
from validator.comparison_strategy import ComparisonStrategy


class ResultComparator:
    """Compare normalized results using configurable semantics."""

    def __init__(self, strategy: ComparisonStrategy):
        self._strategy = strategy

    def compare(self, baseline: NormalizedResult, candidate: NormalizedResult) -> tuple[bool, str]:
        if baseline.columns != candidate.columns:
            return False, "Column mismatch"

        if self._strategy == ComparisonStrategy.EXACT_ORDERED:
            matches = baseline.rows == candidate.rows
            return matches, "Equivalent" if matches else "Row mismatch"

        if self._strategy == ComparisonStrategy.EXACT_UNORDERED:
            matches = self._sort_rows(baseline.rows) == self._sort_rows(candidate.rows)
            return matches, "Equivalent" if matches else "Row mismatch"

        if self._strategy == ComparisonStrategy.MULTISET:
            matches = Counter(baseline.rows) == Counter(candidate.rows)
            return matches, "Equivalent" if matches else "Multiset mismatch"

        if self._strategy == ComparisonStrategy.HASH:
            matches = self._result_hash(baseline) == self._result_hash(candidate)
            return matches, "Equivalent" if matches else "Hash mismatch"

        return False, f"Unsupported comparison strategy: {self._strategy}"

    @staticmethod
    def _sort_rows(rows: tuple[tuple[object, ...], ...]) -> list[tuple[object, ...]]:
        return sorted(rows, key=lambda row: tuple(repr(value) for value in row))

    @staticmethod
    def _result_hash(result: NormalizedResult) -> str:
        payload = json.dumps(
            {
                "columns": result.columns,
                "rows": result.rows,
            },
            default=repr,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return sha256(payload.encode("utf-8")).hexdigest()
