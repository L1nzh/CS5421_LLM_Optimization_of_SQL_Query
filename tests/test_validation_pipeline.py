from __future__ import annotations

from config.settings import ValidationSettings
from db.adapter import DatabaseAdapter
from execution.query_executor import QueryExecutor
from models import QueryExecutionResult, QueryStreamResult
from validator.comparison_strategy import ComparisonStrategy
from validator.validation_pipeline import ValidationPipeline


class StubAdapter(DatabaseAdapter):
    def __init__(
        self,
        results: dict[str, QueryExecutionResult],
        streamed_rows: dict[str, list[tuple[object, ...]]] | None = None,
    ):
        self._results = results
        self._streamed_rows = streamed_rows or {}
        self.closed = False

    def execute_query(self, query: str) -> QueryExecutionResult:
        return self._results[query]

    def stream_query(self, query: str, batch_size: int = 10_000) -> QueryStreamResult:
        result = self._results[query]
        if not result.success:
            return QueryStreamResult(
                query=query,
                success=False,
                columns=[],
                rows=(),
                error_message=result.error_message,
            )

        rows = self._streamed_rows.get(query, result.rows)
        return QueryStreamResult(
            query=query,
            success=True,
            columns=result.columns,
            rows=iter(rows),
        )

    def close(self) -> None:
        self.closed = True


def build_pipeline(
    results: dict[str, QueryExecutionResult],
    strategy: ComparisonStrategy = ComparisonStrategy.EXACT_UNORDERED,
    streamed_rows: dict[str, list[tuple[object, ...]]] | None = None,
    preserve_row_order: bool = False,
) -> ValidationPipeline:
    adapter = StubAdapter(results, streamed_rows=streamed_rows)
    settings = ValidationSettings(
        comparison_strategy=strategy,
        preserve_row_order=preserve_row_order,
        stream_batch_size=2,
    )
    return ValidationPipeline(QueryExecutor(adapter), settings)


def test_pipeline_marks_valid_candidate() -> None:
    pipeline = build_pipeline(
        {
            "baseline": QueryExecutionResult("baseline", True, ["id"], [(1,), (2,)], 5.0),
            "candidate": QueryExecutionResult("candidate", True, ["id"], [(2,), (1,)], 3.0),
        }
    )

    report = pipeline.validate("baseline", ["candidate"])

    assert report.baseline_row_count == 2
    assert report.results[0].is_valid is True
    assert report.results[0].reason == "Equivalent"


def test_pipeline_marks_invalid_candidate() -> None:
    pipeline = build_pipeline(
        {
            "baseline": QueryExecutionResult("baseline", True, ["id"], [(1,), (2,)], 5.0),
            "candidate": QueryExecutionResult("candidate", True, ["id"], [(3,), (4,)], 3.0),
        }
    )

    report = pipeline.validate("baseline", ["candidate"])

    assert report.results[0].is_valid is False
    assert report.results[0].reason == "Row mismatch"


def test_pipeline_handles_candidate_failure() -> None:
    pipeline = build_pipeline(
        {
            "baseline": QueryExecutionResult("baseline", True, ["id"], [(1,), (2,)], 5.0),
            "candidate": QueryExecutionResult(
                "candidate",
                False,
                [],
                [],
                2.0,
                "syntax error",
            ),
        }
    )

    report = pipeline.validate("baseline", ["candidate"])

    assert report.results[0].is_valid is False
    assert report.results[0].reason == "Candidate execution failed"
    assert report.results[0].error_message == "syntax error"


def test_pipeline_handles_baseline_failure() -> None:
    pipeline = build_pipeline(
        {
            "baseline": QueryExecutionResult(
                "baseline",
                False,
                [],
                [],
                1.0,
                "permission denied",
            ),
            "candidate": QueryExecutionResult("candidate", True, ["id"], [(1,)], 2.0),
        }
    )

    report = pipeline.validate("baseline", ["candidate"])

    assert report.baseline_error_message == "permission denied"
    assert report.results[0].is_valid is False
    assert report.results[0].reason == "Baseline execution failed"


def test_pipeline_hash_validates_large_results_without_materialized_compare() -> None:
    results = {
        "baseline": QueryExecutionResult("baseline", True, ["id"], [], 0.0),
        "candidate": QueryExecutionResult("candidate", True, ["id"], [], 0.0),
    }
    streamed_rows = {
        "baseline": [(1,), (2,), (3,)],
        "candidate": [(3,), (2,), (1,)],
    }
    pipeline = build_pipeline(
        results,
        strategy=ComparisonStrategy.HASH,
        streamed_rows=streamed_rows,
        preserve_row_order=False,
    )

    report = pipeline.validate("baseline", ["candidate"])

    assert report.baseline_row_count == 3
    assert report.results[0].is_valid is True
    assert report.results[0].reason == "Equivalent"


def test_pipeline_hash_detects_mismatch() -> None:
    results = {
        "baseline": QueryExecutionResult("baseline", True, ["id"], [], 0.0),
        "candidate": QueryExecutionResult("candidate", True, ["id"], [], 0.0),
    }
    streamed_rows = {
        "baseline": [(1,), (2,), (3,)],
        "candidate": [(1,), (2,), (4,)],
    }
    pipeline = build_pipeline(
        results,
        strategy=ComparisonStrategy.HASH,
        streamed_rows=streamed_rows,
        preserve_row_order=False,
    )

    report = pipeline.validate("baseline", ["candidate"])

    assert report.results[0].is_valid is False
    assert report.results[0].reason == "Hash mismatch"
