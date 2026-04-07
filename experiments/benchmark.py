from __future__ import annotations

from statistics import median

from layer6.benchmark import PostgresExplainBenchmarkLayer
from pipeline.models import BenchmarkReport, CandidateBenchmarkResult, NormalizedCandidate


class CachedPostgresBenchmarkLayer(PostgresExplainBenchmarkLayer):
    """Experiment benchmark layer that caches baseline results per raw query."""

    def __init__(self, dsn: str, repeats: int = 5, statement_timeout_ms: int | None = None):
        super().__init__(dsn=dsn, repeats=repeats, statement_timeout_ms=statement_timeout_ms)
        self._baseline_cache: dict[str, object] = {}

    def benchmark(self, raw_query: str, candidates: list[NormalizedCandidate]) -> BenchmarkReport:
        baseline_result = self._baseline_cache.get(raw_query)
        if baseline_result is None:
            baseline_result = self._benchmark_one(raw_query)
            self._baseline_cache[raw_query] = baseline_result

        baseline_exec_median = median(baseline_result.execution_ms_list) if baseline_result.execution_ms_list else None
        baseline_plan_median = median(baseline_result.planning_ms_list) if baseline_result.planning_ms_list else None
        baseline_buffers = baseline_result.buffer_stats
        baseline_mem_score = baseline_buffers.memory_score if baseline_buffers else None

        results: list[CandidateBenchmarkResult] = []
        for candidate in candidates:
            if not candidate.sql:
                results.append(
                    CandidateBenchmarkResult(
                        candidate_id=candidate.candidate_id,
                        query="",
                        success=False,
                        execution_time_ms=None,
                        planning_time_ms=None,
                        speedup=None,
                        error_message=candidate.normalization_error or "Candidate normalization failed",
                        buffer_stats=None,
                        memory_score=None,
                    )
                )
                continue

            result = self._benchmark_one(candidate.sql)
            execution_median = median(result.execution_ms_list) if result.execution_ms_list else None
            planning_median = median(result.planning_ms_list) if result.planning_ms_list else None
            speedup = (baseline_exec_median / execution_median) if (baseline_exec_median and execution_median) else None
            cand_buffers = result.buffer_stats
            cand_mem_score = cand_buffers.memory_score if cand_buffers else None
            results.append(
                CandidateBenchmarkResult(
                    candidate_id=candidate.candidate_id,
                    query=candidate.sql,
                    success=result.error is None and bool(result.execution_ms_list),
                    execution_time_ms=execution_median,
                    planning_time_ms=planning_median,
                    speedup=speedup,
                    error_message=result.error,
                    buffer_stats=cand_buffers,
                    memory_score=cand_mem_score,
                )
            )

        return BenchmarkReport(
            baseline_query=raw_query,
            baseline_execution_time_ms=baseline_exec_median,
            baseline_planning_time_ms=baseline_plan_median,
            baseline_buffer_stats=baseline_buffers,
            baseline_memory_score=baseline_mem_score,
            candidate_results=tuple(results),
        )
