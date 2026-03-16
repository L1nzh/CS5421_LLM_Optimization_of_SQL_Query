from __future__ import annotations

from config.settings import ValidationSettings
from db.adapter import DatabaseAdapter
from execution.query_executor import QueryExecutor
from models import QueryExecutionResult
from validator.comparison_strategy import ComparisonStrategy
from validator.validation_pipeline import ValidationPipeline


class StubAdapter(DatabaseAdapter):
    def __init__(self, results: dict[str, QueryExecutionResult]):
        self._results = results
        self.closed = False

    def execute_query(self, query: str) -> QueryExecutionResult:
        return self._results[query]

    def close(self) -> None:
        self.closed = True


def build_pipeline(results: dict[str, QueryExecutionResult]) -> ValidationPipeline:
    adapter = StubAdapter(results)
    settings = ValidationSettings(comparison_strategy=ComparisonStrategy.EXACT_UNORDERED)
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
