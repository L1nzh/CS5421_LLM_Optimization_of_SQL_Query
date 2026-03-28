from __future__ import annotations

import json
from statistics import median
from typing import Any, Optional

from psycopg import connect

from pipeline.models import BenchmarkReport, CandidateBenchmarkResult, NormalizedCandidate


class PlaceholderBenchmarkLayer:
    """Layer 6 placeholder for environments where benchmarking is not available."""

    def benchmark(self, raw_query: str, candidates: list[NormalizedCandidate]) -> BenchmarkReport:
        return BenchmarkReport(
            baseline_query=raw_query,
            baseline_execution_time_ms=None,
            baseline_planning_time_ms=None,
            candidate_results=tuple(
                CandidateBenchmarkResult(
                    candidate_id=candidate.candidate_id,
                    query=candidate.sql or "",
                    success=False,
                    execution_time_ms=None,
                    planning_time_ms=None,
                    speedup=None,
                    error_message="Benchmark skipped (placeholder)",
                )
                for candidate in candidates
            ),
        )


class PostgresExplainBenchmarkLayer:
    """Layer 6 implementation using EXPLAIN ANALYZE on PostgreSQL."""

    def __init__(self, dsn: str, repeats: int = 1, statement_timeout_ms: Optional[int] = None):
        self._dsn = dsn
        self._repeats = repeats
        self._statement_timeout_ms = statement_timeout_ms

    def benchmark(self, raw_query: str, candidates: list[NormalizedCandidate]) -> BenchmarkReport:
        baseline_execution_ms, baseline_planning_ms, _ = self._benchmark_one(raw_query)
        baseline_exec_median = median(baseline_execution_ms) if baseline_execution_ms else None
        baseline_plan_median = median(baseline_planning_ms) if baseline_planning_ms else None

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
                    )
                )
                continue

            execution_ms, planning_ms, error = self._benchmark_one(candidate.sql)
            execution_median = median(execution_ms) if execution_ms else None
            planning_median = median(planning_ms) if planning_ms else None
            speedup = (baseline_exec_median / execution_median) if (baseline_exec_median and execution_median) else None
            results.append(
                CandidateBenchmarkResult(
                    candidate_id=candidate.candidate_id,
                    query=candidate.sql,
                    success=error is None and bool(execution_ms),
                    execution_time_ms=execution_median,
                    planning_time_ms=planning_median,
                    speedup=speedup,
                    error_message=error,
                )
            )

        return BenchmarkReport(
            baseline_query=raw_query,
            baseline_execution_time_ms=baseline_exec_median,
            baseline_planning_time_ms=baseline_plan_median,
            candidate_results=tuple(results),
        )

    def _benchmark_one(self, sql: str) -> tuple[list[float], list[float], str | None]:
        run_execution_ms: list[float] = []
        run_planning_ms: list[float] = []
        try:
            with connect(self._dsn) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    if self._statement_timeout_ms is not None:
                        cur.execute(f"SET statement_timeout = {int(self._statement_timeout_ms)};")
                    explain_sql = f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}"
                    for _ in range(self._repeats):
                        cur.execute(explain_sql)
                        row = cur.fetchone()
                        if row is None:
                            raise RuntimeError("EXPLAIN returned no rows")
                        execution_ms, planning_ms = self._extract_explain_times_ms(row[0])
                        run_execution_ms.append(execution_ms)
                        if planning_ms is not None:
                            run_planning_ms.append(planning_ms)
            return run_execution_ms, run_planning_ms, None
        except Exception as exc:
            return run_execution_ms, run_planning_ms, str(exc)

    @staticmethod
    def _extract_explain_times_ms(explain_json: Any) -> tuple[float, Optional[float]]:
        if isinstance(explain_json, str):
            explain_json = json.loads(explain_json)
        if isinstance(explain_json, (bytes, bytearray)):
            explain_json = json.loads(explain_json.decode("utf-8"))
        if not isinstance(explain_json, list) or not explain_json:
            raise ValueError("Unexpected EXPLAIN JSON")
        top = explain_json[0]
        if not isinstance(top, dict) or "Execution Time" not in top:
            raise ValueError("Unexpected EXPLAIN JSON")
        execution_ms = float(top["Execution Time"])
        planning_ms = float(top["Planning Time"]) if "Planning Time" in top else None
        return execution_ms, planning_ms
