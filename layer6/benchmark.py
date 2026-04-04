from __future__ import annotations

import json
from statistics import median
from typing import Any, Optional

from psycopg import connect

from pipeline.models import BenchmarkReport, BufferStats, CandidateBenchmarkResult, NormalizedCandidate


class PlaceholderBenchmarkLayer:
    """Layer 6 placeholder for environments where benchmarking is not available."""

    def benchmark(self, raw_query: str, candidates: list[NormalizedCandidate]) -> BenchmarkReport:
        return BenchmarkReport(
            baseline_query=raw_query,
            baseline_execution_time_ms=None,
            baseline_planning_time_ms=None,
            baseline_buffer_stats=None,
            baseline_memory_score=None,
            candidate_results=tuple(
                CandidateBenchmarkResult(
                    candidate_id=candidate.candidate_id,
                    query=candidate.sql or "",
                    success=False,
                    execution_time_ms=None,
                    planning_time_ms=None,
                    speedup=None,
                    error_message="Benchmark skipped (placeholder)",
                    buffer_stats=None,
                    memory_score=None,  
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
        baseline_result = self._benchmark_one(raw_query)
        baseline_exec_median = median(baseline_result.execution_ms_list) if baseline_result.execution_ms_list else None
        baseline_plan_median = median(baseline_result.planning_ms_list) if baseline_result.planning_ms_list else None
        # Use the last run's buffer stats as representative (buffers stabilize after warm-up)
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

    def _benchmark_one(self, sql: str) -> _BenchmarkRunResult:
        run_execution_ms: list[float] = []
        run_planning_ms: list[float] = []
        last_buffer_stats: Optional[BufferStats] = None
        try:
            with connect(self._dsn) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    if self._statement_timeout_ms is not None:
                        cur.execute(f"SET statement_timeout = {int(self._statement_timeout_ms)};")
                    
                    # KEY CHANGE: added BUFFERS to get memory/IO metrics
                    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
                    
                    for _ in range(self._repeats):
                        cur.execute(explain_sql)
                        row = cur.fetchone()
                        if row is None:
                            raise RuntimeError("EXPLAIN returned no rows")
                        times, buffers = self._extract_explain_data(row[0])
                        run_execution_ms.append(times[0])
                        if times[1] is not None: #planning time
                            run_planning_ms.append(times[1])
                        last_buffer_stats = buffers #replace every time
            return _BenchmarkRunResult(run_execution_ms, run_planning_ms, last_buffer_stats, None)
        except Exception as exc:
            return _BenchmarkRunResult(run_execution_ms, run_planning_ms, last_buffer_stats, str(exc))

    @staticmethod
    def _extract_explain_data(explain_json: Any) -> tuple[tuple[float, Optional[float]], BufferStats]:
        """Extract timing and buffer stats from EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)."""
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
        
        # Recursively aggregate buffer stats from all plan nodes
        plan = top.get("Plan", {})
        buffers = PostgresExplainBenchmarkLayer._aggregate_buffers(plan)

        return (execution_ms, planning_ms), buffers

    
    @staticmethod
    def _aggregate_buffers(node: dict) -> BufferStats:
        """
        Recursively walk the plan tree and sum up buffer counters.
        
        PostgreSQL reports buffer stats per node. We aggregate across 
        the entire plan to get total resource usage for the query.
        """
        shared_hit = node.get("Shared Hit Blocks", 0)
        shared_read = node.get("Shared Read Blocks", 0)
        shared_dirtied = node.get("Shared Dirtied Blocks", 0)
        shared_written = node.get("Shared Written Blocks", 0)
        temp_read = node.get("Temp Read Blocks", 0)
        temp_written = node.get("Temp Written Blocks", 0)

        for child in node.get("Plans", []):
            child_buf = PostgresExplainBenchmarkLayer._aggregate_buffers(child)
            shared_hit += child_buf.shared_hit_blocks
            shared_read += child_buf.shared_read_blocks
            shared_dirtied += child_buf.shared_dirtied_blocks
            shared_written += child_buf.shared_written_blocks
            temp_read += child_buf.temp_read_blocks
            temp_written += child_buf.temp_written_blocks

        return BufferStats(
            shared_hit_blocks=shared_hit,
            shared_read_blocks=shared_read,
            shared_dirtied_blocks=shared_dirtied,
            shared_written_blocks=shared_written,
            temp_read_blocks=temp_read,
            temp_written_blocks=temp_written,
        )

class _BenchmarkRunResult:
    """Internal helper to bundle benchmark run outputs."""
    __slots__ = ("execution_ms_list", "planning_ms_list", "buffer_stats", "error")

    def __init__(
        self,
        execution_ms_list: list[float],
        planning_ms_list: list[float],
        buffer_stats: Optional[BufferStats],
        error: Optional[str],
    ):
        self.execution_ms_list = execution_ms_list
        self.planning_ms_list = planning_ms_list
        self.buffer_stats = buffer_stats
        self.error = error
