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
class CandidateBenchmarkResult:
    candidate_id: str
    query: str
    success: bool
    execution_time_ms: Optional[float]
    planning_time_ms: Optional[float]
    speedup: Optional[float]
    error_message: Optional[str] = None


@dataclass(slots=True, frozen=True)
class BenchmarkReport:
    baseline_query: str
    baseline_execution_time_ms: Optional[float]
    baseline_planning_time_ms: Optional[float]
    candidate_results: tuple[CandidateBenchmarkResult, ...]


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
