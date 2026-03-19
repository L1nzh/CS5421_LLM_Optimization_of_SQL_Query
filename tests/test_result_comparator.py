from __future__ import annotations

from models import HashedResult, NormalizedResult
from validator.comparison_strategy import ComparisonStrategy
from validator.result_comparator import ResultComparator


def test_comparator_exact_ordered_requires_same_order() -> None:
    comparator = ResultComparator(ComparisonStrategy.EXACT_ORDERED)
    baseline = NormalizedResult(columns=("id",), rows=((1,), (2,)))
    candidate = NormalizedResult(columns=("id",), rows=((2,), (1,)))

    is_valid, reason = comparator.compare(baseline, candidate)

    assert is_valid is False
    assert reason == "Row mismatch"


def test_comparator_exact_unordered_accepts_reordered_rows() -> None:
    comparator = ResultComparator(ComparisonStrategy.EXACT_UNORDERED)
    baseline = NormalizedResult(columns=("id",), rows=((1,), (2,)))
    candidate = NormalizedResult(columns=("id",), rows=((2,), (1,)))

    is_valid, reason = comparator.compare(baseline, candidate)

    assert is_valid is True
    assert reason == "Equivalent"


def test_comparator_multiset_respects_duplicate_counts() -> None:
    comparator = ResultComparator(ComparisonStrategy.MULTISET)
    baseline = NormalizedResult(columns=("id",), rows=((1,), (1,), (2,)))
    candidate = NormalizedResult(columns=("id",), rows=((1,), (2,), (2,)))

    is_valid, reason = comparator.compare(baseline, candidate)

    assert is_valid is False
    assert reason == "Multiset mismatch"


def test_comparator_rejects_column_mismatch() -> None:
    comparator = ResultComparator(ComparisonStrategy.EXACT_UNORDERED)
    baseline = NormalizedResult(columns=("id", "name"), rows=((1, "a"),))
    candidate = NormalizedResult(columns=("name", "id"), rows=(("a", 1),))

    is_valid, reason = comparator.compare(baseline, candidate)

    assert is_valid is False
    assert reason == "Column mismatch"


def test_comparator_hash_accepts_identical_results() -> None:
    comparator = ResultComparator(ComparisonStrategy.HASH)
    baseline = HashedResult(columns=("id",), row_count=2, digest="abc")
    candidate = HashedResult(columns=("id",), row_count=2, digest="abc")

    is_valid, reason = comparator.compare_hashed(baseline, candidate)

    assert is_valid is True
    assert reason == "Equivalent"


def test_comparator_hash_rejects_different_results() -> None:
    comparator = ResultComparator(ComparisonStrategy.HASH)
    baseline = HashedResult(columns=("id",), row_count=2, digest="abc")
    candidate = HashedResult(columns=("id",), row_count=2, digest="def")

    is_valid, reason = comparator.compare_hashed(baseline, candidate)

    assert is_valid is False
    assert reason == "Hash mismatch"


def test_comparator_hash_rejects_different_row_count() -> None:
    comparator = ResultComparator(ComparisonStrategy.HASH)
    baseline = HashedResult(columns=("id",), row_count=2, digest="abc")
    candidate = HashedResult(columns=("id",), row_count=3, digest="abc")

    is_valid, reason = comparator.compare_hashed(baseline, candidate)

    assert is_valid is False
    assert reason == "Row count mismatch"
