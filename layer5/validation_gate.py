from __future__ import annotations

from config.settings import ValidationSettings
from execution.query_executor import QueryExecutor
from models import CandidateValidationResult, ValidationReport
from pipeline.models import NormalizedCandidate
from validator.validation_pipeline import ValidationPipeline


class ValidatorValidationGateLayer:
    """Layer 5 adapter around the existing semantic validator."""

    def __init__(self, executor: QueryExecutor | None, settings: ValidationSettings):
        self._executor = executor
        self._settings = settings

    def validate(self, raw_query: str, candidates: list[NormalizedCandidate]) -> ValidationReport:
        executable_candidates = [candidate for candidate in candidates if candidate.sql]

        if self._executor is None:
            return ValidationReport(
                raw_query=raw_query,
                baseline_execution_time_ms=0.0,
                baseline_row_count=0,
                baseline_columns=[],
                results=[
                    CandidateValidationResult(
                        query=candidate.sql or "",
                        is_valid=False if not candidate.sql else True,
                        reason=(
                            candidate.normalization_error
                            or "Candidate normalization failed"
                            if not candidate.sql
                            else "Validation skipped (placeholder)"
                        ),
                        execution_time_ms=0.0,
                        error_message=candidate.normalization_error if not candidate.sql else None,
                    )
                    for candidate in candidates
                ],
            )

        pipeline = ValidationPipeline(self._executor, self._settings)
        executable_report = pipeline.validate(raw_query, [candidate.sql for candidate in executable_candidates if candidate.sql])
        executable_results = iter(executable_report.results)
        merged_results: list[CandidateValidationResult] = []
        for candidate in candidates:
            if not candidate.sql:
                merged_results.append(
                    CandidateValidationResult(
                        query="",
                        is_valid=False,
                        reason=candidate.normalization_error or "Candidate normalization failed",
                        execution_time_ms=0.0,
                        error_message=candidate.normalization_error,
                    )
                )
            else:
                merged_results.append(next(executable_results))
        report = executable_report
        report.results = merged_results
        return report
