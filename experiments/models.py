from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from pipeline.models import RankedCandidate


@dataclass(slots=True, frozen=True)
class ExperimentCombo:
    prompt_strategy: str
    reasoning_strategy: str
    model: str

    @property
    def combo_id(self) -> str:
        return f"{self.prompt_strategy}__{self.reasoning_strategy}__{self.model}".replace("/", "_")


@dataclass(slots=True, frozen=True)
class ExperimentPhaseConfig:
    phase_name: str
    sample_size: int
    benchmark_repeats: int
    random_seed: int


@dataclass(slots=True, frozen=True)
class ExperimentQueryTrace:
    phase_name: str
    query_id: str
    query_path: str
    combo_id: str
    prompt_strategy: str
    reasoning_strategy: str
    model: str
    prompt_text: str
    stage1_prompt_text: Optional[str]
    selected_query: Optional[str]
    ranked_candidates: tuple[RankedCandidate, ...]
    validation_report: dict[str, Any]
    benchmark_report: dict[str, Any]
    analysis_report: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class ExperimentComboSummary:
    phase_name: str
    combo_id: str
    prompt_strategy: str
    reasoning_strategy: str
    model: str
    query_count: int
    selected_count: int
    valid_selected_count: int
    benchmark_success_count: int
    median_speedup: Optional[float]
    median_memory_score: Optional[float]
    average_score: Optional[float]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class ExperimentPhaseResult:
    phase_config: ExperimentPhaseConfig
    sampled_query_paths: tuple[str, ...]
    combo_summaries: tuple[ExperimentComboSummary, ...]
    query_traces: tuple[ExperimentQueryTrace, ...]
    selected_best_combo: Optional[ExperimentCombo] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
