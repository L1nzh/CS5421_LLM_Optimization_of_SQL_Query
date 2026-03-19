from __future__ import annotations

from config.settings import ValidationSettings
from execution.query_executor import QueryExecutor
from models import CandidateValidationResult, ValidationReport
from validator.comparison_strategy import ComparisonStrategy
from validator.result_comparator import ResultComparator
from validator.result_hasher import HashingQueryExecutor, ResultHasher
from validator.result_normalizer import ResultNormalizer


class ValidationPipeline:
    """Run baseline and candidate queries, then classify semantic equivalence."""

    def __init__(self, executor: QueryExecutor, settings: ValidationSettings):
        self._settings = settings
        self._executor = executor
        self._normalizer = ResultNormalizer(settings)
        self._comparator = ResultComparator(settings.comparison_strategy)
        self._hashing_executor = HashingQueryExecutor(
            executor=executor,
            hasher=ResultHasher(self._normalizer, settings),
            batch_size=settings.stream_batch_size,
        )

    def validate(self, raw_query: str, candidate_queries: list[str]) -> ValidationReport:
        if self._settings.comparison_strategy == ComparisonStrategy.HASH:
            return self._validate_hashed(raw_query, candidate_queries)

        baseline_result = self._executor.execute(raw_query)
        if not baseline_result.success:
            return ValidationReport(
                raw_query=raw_query,
                baseline_execution_time_ms=baseline_result.execution_time_ms,
                baseline_row_count=0,
                baseline_columns=[],
                baseline_error_message=baseline_result.error_message or "Baseline execution failed",
                results=[
                    CandidateValidationResult(
                        query=query,
                        is_valid=False,
                        reason="Baseline execution failed",
                        execution_time_ms=0.0,
                    )
                    for query in candidate_queries
                ],
            )

        normalized_baseline = self._normalizer.normalize(baseline_result)
        report = ValidationReport(
            raw_query=raw_query,
            baseline_execution_time_ms=baseline_result.execution_time_ms,
            baseline_row_count=len(baseline_result.rows),
            baseline_columns=baseline_result.columns,
        )

        for query in candidate_queries:
            candidate_result = self._executor.execute(query)
            if not candidate_result.success:
                report.results.append(
                    CandidateValidationResult(
                        query=query,
                        is_valid=False,
                        reason="Candidate execution failed",
                        execution_time_ms=candidate_result.execution_time_ms,
                        error_message=candidate_result.error_message,
                    )
                )
                continue

            normalized_candidate = self._normalizer.normalize(candidate_result)
            is_valid, reason = self._comparator.compare(normalized_baseline, normalized_candidate)
            report.results.append(
                CandidateValidationResult(
                    query=query,
                    is_valid=is_valid,
                    reason=reason,
                    execution_time_ms=candidate_result.execution_time_ms,
                    error_message=candidate_result.error_message,
                )
            )

        return report

    def _validate_hashed(self, raw_query: str, candidate_queries: list[str]) -> ValidationReport:
        success, baseline_hash, baseline_time_ms, baseline_error = self._hashing_executor.execute(raw_query)
        if not success or baseline_hash is None:
            return ValidationReport(
                raw_query=raw_query,
                baseline_execution_time_ms=baseline_time_ms,
                baseline_row_count=0,
                baseline_columns=[],
                baseline_error_message=baseline_error or "Baseline execution failed",
                results=[
                    CandidateValidationResult(
                        query=query,
                        is_valid=False,
                        reason="Baseline execution failed",
                        execution_time_ms=0.0,
                    )
                    for query in candidate_queries
                ],
            )

        report = ValidationReport(
            raw_query=raw_query,
            baseline_execution_time_ms=baseline_time_ms,
            baseline_row_count=baseline_hash.row_count,
            baseline_columns=list(baseline_hash.columns),
        )

        for query in candidate_queries:
            candidate_success, candidate_hash, candidate_time_ms, candidate_error = self._hashing_executor.execute(query)
            if not candidate_success or candidate_hash is None:
                report.results.append(
                    CandidateValidationResult(
                        query=query,
                        is_valid=False,
                        reason="Candidate execution failed",
                        execution_time_ms=candidate_time_ms,
                        error_message=candidate_error,
                    )
                )
                continue

            is_valid, reason = self._comparator.compare_hashed(baseline_hash, candidate_hash)
            report.results.append(
                CandidateValidationResult(
                    query=query,
                    is_valid=is_valid,
                    reason=reason,
                    execution_time_ms=candidate_time_ms,
                    error_message=candidate_error,
                )
            )

        return report
