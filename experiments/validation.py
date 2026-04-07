from __future__ import annotations

from config.settings import ValidationSettings
from execution.query_executor import QueryExecutor
from models import CandidateValidationResult, ValidationReport
from pipeline.models import NormalizedCandidate
from validator.comparison_strategy import ComparisonStrategy
from validator.result_comparator import ResultComparator
from validator.result_hasher import HashingQueryExecutor, ResultHasher
from validator.result_normalizer import ResultNormalizer


class CachedValidationGateLayer:
    """Experiment validation layer that caches baseline results per raw query."""

    def __init__(self, executor: QueryExecutor, settings: ValidationSettings):
        self._executor = executor
        self._settings = settings
        self._normalizer = ResultNormalizer(settings)
        self._comparator = ResultComparator(settings.comparison_strategy)
        self._hashing_executor = HashingQueryExecutor(
            executor=executor,
            hasher=ResultHasher(self._normalizer, settings),
            batch_size=settings.stream_batch_size,
        )
        self._baseline_cache: dict[str, dict[str, object]] = {}

    def validate(self, raw_query: str, candidates: list[NormalizedCandidate]) -> ValidationReport:
        if self._settings.comparison_strategy == ComparisonStrategy.HASH:
            return self._validate_hashed(raw_query, candidates)
        return self._validate_normalized(raw_query, candidates)

    def _validate_normalized(self, raw_query: str, candidates: list[NormalizedCandidate]) -> ValidationReport:
        baseline = self._baseline_cache.get(raw_query)
        if baseline is None:
            baseline_result = self._executor.execute(raw_query)
            if not baseline_result.success:
                report = ValidationReport(
                    raw_query=raw_query,
                    baseline_execution_time_ms=baseline_result.execution_time_ms,
                    baseline_row_count=0,
                    baseline_columns=[],
                    baseline_error_message=baseline_result.error_message or "Baseline execution failed",
                    results=[],
                )
                self._baseline_cache[raw_query] = {"failed_report": report}
                baseline = self._baseline_cache[raw_query]
            else:
                baseline = {
                    "normalized_result": self._normalizer.normalize(baseline_result),
                    "execution_time_ms": baseline_result.execution_time_ms,
                    "row_count": len(baseline_result.rows),
                    "columns": baseline_result.columns,
                }
                self._baseline_cache[raw_query] = baseline

        if "failed_report" in baseline:
            failed_report = baseline["failed_report"]
            return ValidationReport(
                raw_query=raw_query,
                baseline_execution_time_ms=failed_report.baseline_execution_time_ms,
                baseline_row_count=failed_report.baseline_row_count,
                baseline_columns=failed_report.baseline_columns,
                baseline_error_message=failed_report.baseline_error_message,
                results=[
                    CandidateValidationResult(
                        query=candidate.sql or "",
                        is_valid=False,
                        reason="Baseline execution failed",
                        execution_time_ms=0.0,
                    )
                    for candidate in candidates
                ],
            )

        report = ValidationReport(
            raw_query=raw_query,
            baseline_execution_time_ms=baseline["execution_time_ms"],
            baseline_row_count=baseline["row_count"],
            baseline_columns=baseline["columns"],
            results=[],
        )
        for candidate in candidates:
            if not candidate.sql:
                report.results.append(
                    CandidateValidationResult(
                        query="",
                        is_valid=False,
                        reason=candidate.normalization_error or "Candidate normalization failed",
                        execution_time_ms=0.0,
                        error_message=candidate.normalization_error,
                    )
                )
                continue

            candidate_result = self._executor.execute(candidate.sql)
            if not candidate_result.success:
                report.results.append(
                    CandidateValidationResult(
                        query=candidate.sql,
                        is_valid=False,
                        reason="Candidate execution failed",
                        execution_time_ms=candidate_result.execution_time_ms,
                        error_message=candidate_result.error_message,
                    )
                )
                continue

            normalized_candidate = self._normalizer.normalize(candidate_result)
            is_valid, reason = self._comparator.compare(baseline["normalized_result"], normalized_candidate)
            report.results.append(
                CandidateValidationResult(
                    query=candidate.sql,
                    is_valid=is_valid,
                    reason=reason,
                    execution_time_ms=candidate_result.execution_time_ms,
                    error_message=candidate_result.error_message,
                )
            )
        return report

    def _validate_hashed(self, raw_query: str, candidates: list[NormalizedCandidate]) -> ValidationReport:
        baseline = self._baseline_cache.get(raw_query)
        if baseline is None:
            success, baseline_hash, baseline_time_ms, baseline_error = self._hashing_executor.execute(raw_query)
            if not success or baseline_hash is None:
                report = ValidationReport(
                    raw_query=raw_query,
                    baseline_execution_time_ms=baseline_time_ms,
                    baseline_row_count=0,
                    baseline_columns=[],
                    baseline_error_message=baseline_error or "Baseline execution failed",
                    results=[],
                )
                self._baseline_cache[raw_query] = {"failed_report": report}
                baseline = self._baseline_cache[raw_query]
            else:
                baseline = {
                    "hashed_result": baseline_hash,
                    "execution_time_ms": baseline_time_ms,
                }
                self._baseline_cache[raw_query] = baseline

        if "failed_report" in baseline:
            failed_report = baseline["failed_report"]
            return ValidationReport(
                raw_query=raw_query,
                baseline_execution_time_ms=failed_report.baseline_execution_time_ms,
                baseline_row_count=failed_report.baseline_row_count,
                baseline_columns=failed_report.baseline_columns,
                baseline_error_message=failed_report.baseline_error_message,
                results=[
                    CandidateValidationResult(
                        query=candidate.sql or "",
                        is_valid=False,
                        reason="Baseline execution failed",
                        execution_time_ms=0.0,
                    )
                    for candidate in candidates
                ],
            )

        baseline_hash = baseline["hashed_result"]
        report = ValidationReport(
            raw_query=raw_query,
            baseline_execution_time_ms=baseline["execution_time_ms"],
            baseline_row_count=baseline_hash.row_count,
            baseline_columns=list(baseline_hash.columns),
            results=[],
        )
        for candidate in candidates:
            if not candidate.sql:
                report.results.append(
                    CandidateValidationResult(
                        query="",
                        is_valid=False,
                        reason=candidate.normalization_error or "Candidate normalization failed",
                        execution_time_ms=0.0,
                        error_message=candidate.normalization_error,
                    )
                )
                continue

            success, candidate_hash, candidate_time_ms, candidate_error = self._hashing_executor.execute(candidate.sql)
            if not success or candidate_hash is None:
                report.results.append(
                    CandidateValidationResult(
                        query=candidate.sql,
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
                    query=candidate.sql,
                    is_valid=is_valid,
                    reason=reason,
                    execution_time_ms=candidate_time_ms,
                    error_message=candidate_error,
                )
            )
        return report
