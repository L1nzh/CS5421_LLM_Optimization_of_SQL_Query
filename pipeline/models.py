from __future__ import annotations
from dataclasses import asdict, dataclass, field
from typing import Any, Optional
from config.settings import ValidationSettings


@dataclass(slots=True, frozen=True)
class PipelineRequest:
    raw_queries: tuple[str, ...] = ()
    query_files: tuple[str, ...] = ()
    engine: str = "postgresql"
    schema_text: Optional[str] = None
    schema_file: Optional[str] = None
    index_text: Optional[str] = None
    index_file: Optional[str] = None
    prompt_strategy: str = "P1_ENGINE"
    reasoning_mode: str = "DIRECT"
    model: str = "doubao-seed-2-0-pro-260215"
    candidate_count: int = 3
    benchmark_repeats: int = 1
    statement_timeout_ms: Optional[int] = None
    validation_settings: ValidationSettings = field(default_factory=ValidationSettings)
    extra_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class WorkloadItem:
    query_id: str
    raw_query: str
    engine: str
    source_path: Optional[str] = None
    schema_text: Optional[str] = None
    index_text: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class PromptPackage:
    query_id: str
    raw_query: str
    model: str
    candidate_count: int
    prompt_strategy: str
    reasoning_mode: str
    prompt_text: str
    stage1_prompt_text: Optional[str] = None
    stage2_prompt_template: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class GeneratedCandidate:
    candidate_id: str
    raw_text: str
    model: str
    stage1_text: Optional[str] = None


@dataclass(slots=True, frozen=True)
class NormalizedCandidate:
    candidate_id: str
    raw_text: str
    sql: Optional[str]
    model: str
    normalization_error: Optional[str] = None
    stage1_text: Optional[str] = None


@dataclass(slots=True, frozen=True)
class BufferStats:
    """Aggregated buffer statistics from EXPLAIN (ANALYZE, BUFFERS)."""
    shared_hit_blocks: int = 0
    shared_read_blocks: int = 0
    shared_dirtied_blocks: int = 0
    shared_written_blocks: int = 0
    temp_read_blocks: int = 0
    temp_written_blocks: int = 0

    @property
    def total_shared_blocks(self) -> int:
        """Total shared buffer accesses (hit + read)."""
        return self.shared_hit_blocks + self.shared_read_blocks

    @property
    def cache_hit_ratio(self) -> Optional[float]:
        """Shared buffer cache hit ratio, None if no buffer access."""
        total = self.total_shared_blocks
        if total == 0:
            return None
        return self.shared_hit_blocks / total

    @property
    def total_temp_blocks(self) -> int:
        """Total temp blocks (read + written), indicates spill to disk."""
        return self.temp_read_blocks + self.temp_written_blocks

    @property
    def memory_score(self) -> float:
        """
        Composite memory efficiency score (0~1, higher is better).

        Penalizes disk reads (shared_read) and temp usage heavily,
        rewards high cache hit ratio.
        """
        total_shared = self.total_shared_blocks
        if total_shared == 0 and self.total_temp_blocks == 0:
            return 1.0  # No buffer usage at all — trivial query

        hit_ratio = self.cache_hit_ratio if self.cache_hit_ratio is not None else 0.0

        # Temp block penalty: any spill to disk is bad
        # Normalize by total work done (shared + temp)
        total_all = total_shared + self.total_temp_blocks
        temp_ratio = self.total_temp_blocks / total_all if total_all > 0 else 0.0

        # Score: 70% weight on cache hit ratio, 30% weight on avoiding temp spill
        score = 0.7 * hit_ratio + 0.3 * (1.0 - temp_ratio)
        return round(score, 4)


@dataclass(slots=True, frozen=True)
class CandidateBenchmarkResult:
    candidate_id: str
    query: str
    success: bool
    execution_time_ms: Optional[float]
    planning_time_ms: Optional[float]
    speedup: Optional[float]
    error_message: Optional[str] = None
    # --- NEW: buffer/memory metrics ---
    buffer_stats: Optional[BufferStats] = None
    memory_score: Optional[float] = None


@dataclass(slots=True, frozen=True)
class BenchmarkReport:
    baseline_query: str
    baseline_execution_time_ms: Optional[float]
    baseline_planning_time_ms: Optional[float]
    candidate_results: tuple[CandidateBenchmarkResult, ...]
    # --- NEW: baseline buffer metrics ---
    baseline_buffer_stats: Optional[BufferStats] = None
    baseline_memory_score: Optional[float] = None


@dataclass(slots=True, frozen=True)
class RankedCandidate:
    candidate_id: str
    query: Optional[str]
    raw_text: str
    model: str
    rank: Optional[int]
    score: Optional[float]
    is_valid: bool
    validation_reason: str
    normalization_error: Optional[str] = None
    execution_time_ms: Optional[float] = None
    planning_time_ms: Optional[float] = None
    speedup: Optional[float] = None
    benchmark_error: Optional[str] = None
    stage1_text: Optional[str] = None
    # --- NEW: memory metrics ---
    buffer_stats: Optional[BufferStats] = None
    memory_score: Optional[float] = None


@dataclass(slots=True, frozen=True)
class AnalysisReport:
    summary: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class QueryOptimizationResult:
    query_id: str
    raw_query: str
    selected_query: Optional[str]
    ranked_candidates: tuple[RankedCandidate, ...]
    analysis_report: AnalysisReport


@dataclass(slots=True, frozen=True)
class PipelineRunResult:
    request: PipelineRequest
    results: tuple[QueryOptimizationResult, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
