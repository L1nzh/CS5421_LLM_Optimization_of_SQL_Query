from __future__ import annotations

from dataclasses import replace

from models import ValidationReport
from pipeline.models import BenchmarkReport, NormalizedCandidate, RankedCandidate


class SpeedupRankingLayer:
    """Layer 7: ranks candidates by validation status and benchmark score.

    The composite score combines speedup (time performance) and memory_score
    (buffer efficiency) with configurable weights.
    """

    def __init__(self, speedup_weight: float = 0.7, memory_weight: float = 0.3):
        self._speedup_weight = speedup_weight
        self._memory_weight = memory_weight

    def rank(
        self,
        normalized_candidates: list[NormalizedCandidate],
        validation_report: ValidationReport,
        benchmark_report: BenchmarkReport,
    ) -> list[RankedCandidate]:
        benchmark_by_id = {result.candidate_id: result for result in benchmark_report.candidate_results}

        ranked: list[RankedCandidate] = []
        for candidate, validation_result in zip(normalized_candidates, validation_report.results):
            benchmark_result = benchmark_by_id.get(candidate.candidate_id)
            is_valid = bool(validation_result and validation_result.is_valid and not candidate.normalization_error)
            score = self._score(is_valid, benchmark_result)
            ranked.append(
                RankedCandidate(
                    candidate_id=candidate.candidate_id,
                    query=candidate.sql,
                    raw_text=candidate.raw_text,
                    model=candidate.model,
                    rank=None,
                    score=score,
                    is_valid=is_valid,
                    validation_reason=validation_result.reason if validation_result else (candidate.normalization_error or "Not evaluated"),
                    normalization_error=candidate.normalization_error,
                    execution_time_ms=benchmark_result.execution_time_ms if benchmark_result else None,
                    planning_time_ms=benchmark_result.planning_time_ms if benchmark_result else None,
                    speedup=benchmark_result.speedup if benchmark_result else None,
                    benchmark_error=benchmark_result.error_message if benchmark_result else None,
                    stage1_text=candidate.stage1_text,
                    # --- NEW: memory metrics ---
                    buffer_stats=benchmark_result.buffer_stats if benchmark_result else None,
                    memory_score=benchmark_result.memory_score if benchmark_result else None,
                )
            )

        sorted_candidates = sorted(
            ranked,
            key=lambda item: (
                item.score is None,
                -(item.score or float("-inf")),
                item.candidate_id,
            ),
        )

        reranked: list[RankedCandidate] = []
        next_rank = 1
        for candidate in sorted_candidates:
            reranked.append(replace(candidate, rank=next_rank if candidate.score is not None else None))
            if candidate.score is not None:
                next_rank += 1
        return reranked

    def _score(self, is_valid: bool, benchmark_result) -> float | None:
        """Compute composite score: weighted combination of speedup and memory_score.

        Score = speedup_weight * speedup + memory_weight * memory_score

        If memory_score is unavailable, falls back to speedup only.
        """
        if not is_valid:
            return None
        if benchmark_result and benchmark_result.success and benchmark_result.speedup is not None:
            speedup = float(benchmark_result.speedup)
            memory = benchmark_result.memory_score

            if memory is not None:
                return self._speedup_weight * speedup + self._memory_weight * memory
            return speedup
        return 0.0
