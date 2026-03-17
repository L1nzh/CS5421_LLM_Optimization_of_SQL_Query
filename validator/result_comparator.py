from __future__ import annotations

from collections import Counter

from models import HashedResult, NormalizedResult
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

        return False, f"Unsupported comparison strategy: {self._strategy}"

    def compare_hashed(self, baseline: HashedResult, candidate: HashedResult) -> tuple[bool, str]:
        if baseline.columns != candidate.columns:
            return False, "Column mismatch"
        if baseline.row_count != candidate.row_count:
            return False, "Row count mismatch"
        matches = baseline.digest == candidate.digest
        return matches, "Equivalent" if matches else f"Hash mismatch, baseline digest: {baseline.digest}, candidate digest: {candidate.digest}"

    @staticmethod
    def _sort_rows(rows: tuple[tuple[object, ...], ...]) -> list[tuple[object, ...]]:
        return sorted(rows, key=lambda row: tuple(repr(value) for value in row))
