from __future__ import annotations

from models import CandidateValidationResult, ValidationReport
from pipeline.models import (
    AnalysisReport,
    BenchmarkReport,
    CandidateBenchmarkResult,
    GeneratedCandidate,
    NormalizedCandidate,
    PipelineRequest,
    PromptPackage,
    RankedCandidate,
    WorkloadItem,
)
from pipeline.sql_optimization_pipeline import SQLRewriteResearchPipeline


class StubWorkloadLayer:
    def prepare(self, request: PipelineRequest) -> list[WorkloadItem]:
        return [WorkloadItem(query_id="query_1", raw_query="SELECT 1", engine="postgresql")]


class StubPromptLayer:
    def build(self, workload_item: WorkloadItem, request: PipelineRequest) -> PromptPackage:
        return PromptPackage(
            query_id=workload_item.query_id,
            raw_query=workload_item.raw_query,
            model="test-model",
            candidate_count=2,
            prompt_strategy="P1_ENGINE",
            reasoning_mode="DIRECT",
            prompt_text="prompt",
        )


class StubGenerationLayer:
    def generate(self, prompt_package: PromptPackage) -> list[GeneratedCandidate]:
        return [
            GeneratedCandidate(candidate_id="c1", raw_text="SELECT 1", model="test-model"),
            GeneratedCandidate(candidate_id="c2", raw_text="SELECT 2", model="test-model"),
        ]


class StubNormalizationLayer:
    def normalize(self, generated_candidates: list[GeneratedCandidate]) -> list[NormalizedCandidate]:
        return [
            NormalizedCandidate(candidate_id="c1", raw_text="SELECT 1", sql="SELECT 1", model="test-model"),
            NormalizedCandidate(candidate_id="c2", raw_text="SELECT 2", sql="SELECT 2", model="test-model"),
        ]


class StubValidationLayer:
    def validate(self, raw_query: str, candidates: list[NormalizedCandidate]) -> ValidationReport:
        return ValidationReport(
            raw_query=raw_query,
            baseline_execution_time_ms=1.0,
            baseline_row_count=1,
            baseline_columns=["?column?"],
            results=[
                CandidateValidationResult(query="SELECT 1", is_valid=True, reason="Equivalent", execution_time_ms=1.0),
                CandidateValidationResult(query="SELECT 2", is_valid=False, reason="Row mismatch", execution_time_ms=1.0),
            ],
        )


class StubBenchmarkLayer:
    def benchmark(self, raw_query: str, candidates: list[NormalizedCandidate]) -> BenchmarkReport:
        return BenchmarkReport(
            baseline_query=raw_query,
            baseline_execution_time_ms=10.0,
            baseline_planning_time_ms=1.0,
            candidate_results=(
                CandidateBenchmarkResult(
                    candidate_id="c1",
                    query="SELECT 1",
                    success=True,
                    execution_time_ms=5.0,
                    planning_time_ms=0.5,
                    speedup=2.0,
                ),
                CandidateBenchmarkResult(
                    candidate_id="c2",
                    query="SELECT 2",
                    success=True,
                    execution_time_ms=20.0,
                    planning_time_ms=0.5,
                    speedup=0.5,
                ),
            ),
        )


class StubRankingLayer:
    def rank(self, normalized_candidates, validation_report, benchmark_report):
        return [
            RankedCandidate(
                candidate_id="c1",
                query="SELECT 1",
                raw_text="SELECT 1",
                model="test-model",
                rank=1,
                score=2.0,
                is_valid=True,
                validation_reason="Equivalent",
            )
        ]


class StubAnalysisLayer:
    def analyze(self, result):
        return AnalysisReport(summary=f"selected={result.selected_query is not None}")


def test_pipeline_selects_top_ranked_query() -> None:
    pipeline = SQLRewriteResearchPipeline(
        workload_layer=StubWorkloadLayer(),
        prompt_layer=StubPromptLayer(),
        generation_layer=StubGenerationLayer(),
        normalization_layer=StubNormalizationLayer(),
        validation_layer=StubValidationLayer(),
        benchmark_layer=StubBenchmarkLayer(),
        ranking_layer=StubRankingLayer(),
        analysis_layer=StubAnalysisLayer(),
    )

    result = pipeline.run(PipelineRequest(raw_queries=("SELECT 1",)))

    assert len(result.results) == 1
    assert result.results[0].selected_query == "SELECT 1"
    assert result.results[0].analysis_report.summary == "selected=True"


def test_pipeline_can_return_no_selected_query() -> None:
    class EmptyRankingLayer(StubRankingLayer):
        def rank(self, normalized_candidates, validation_report, benchmark_report):
            return []

    pipeline = SQLRewriteResearchPipeline(
        workload_layer=StubWorkloadLayer(),
        prompt_layer=StubPromptLayer(),
        generation_layer=StubGenerationLayer(),
        normalization_layer=StubNormalizationLayer(),
        validation_layer=StubValidationLayer(),
        benchmark_layer=StubBenchmarkLayer(),
        ranking_layer=EmptyRankingLayer(),
        analysis_layer=StubAnalysisLayer(),
    )

    result = pipeline.run(PipelineRequest(raw_queries=("SELECT 1",)))

    assert result.results[0].selected_query is None
    assert result.results[0].ranked_candidates == ()
