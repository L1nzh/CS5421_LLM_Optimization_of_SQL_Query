from __future__ import annotations

from typing import Protocol

from models import ValidationReport
from pipeline.models import (
    AnalysisReport,
    BenchmarkReport,
    GeneratedCandidate,
    NormalizedCandidate,
    PipelineRequest,
    PromptPackage,
    QueryOptimizationResult,
    RankedCandidate,
    WorkloadItem,
)


class WorkloadPreparationLayer(Protocol):
    def prepare(self, request: PipelineRequest) -> list[WorkloadItem]:
        ...


class PromptBuilderLayer(Protocol):
    def build(self, workload_item: WorkloadItem, request: PipelineRequest) -> PromptPackage:
        ...


class CandidateGenerationLayer(Protocol):
    def generate(self, prompt_package: PromptPackage) -> list[GeneratedCandidate]:
        ...


class CandidateNormalizationLayer(Protocol):
    def normalize(self, generated_candidates: list[GeneratedCandidate]) -> list[NormalizedCandidate]:
        ...


class ValidationGateLayer(Protocol):
    def validate(self, raw_query: str, candidates: list[NormalizedCandidate]) -> ValidationReport:
        ...


class BenchmarkLayer(Protocol):
    def benchmark(self, raw_query: str, candidates: list[NormalizedCandidate]) -> BenchmarkReport:
        ...


class RankingLayer(Protocol):
    def rank(
        self,
        normalized_candidates: list[NormalizedCandidate],
        validation_report: ValidationReport,
        benchmark_report: BenchmarkReport,
    ) -> list[RankedCandidate]:
        ...


class AnalysisLayer(Protocol):
    def analyze(self, result: QueryOptimizationResult) -> AnalysisReport:
        ...
