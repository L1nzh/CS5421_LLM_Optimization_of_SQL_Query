from __future__ import annotations

from dataclasses import replace

from config.settings import ValidationSettings
from db.postgres_adapter import PostgresAdapter
from execution.query_executor import QueryExecutor
from layer1.workload_preparation import FileOrStringWorkloadPreparationLayer
from layer2.prompt_builder import DefaultPromptBuilderLayer
from layer3.generation_layer import DefaultCandidateGenerationLayer
from layer4.candidate_normalizer import DefaultCandidateNormalizationLayer
from layer5.validation_gate import ValidatorValidationGateLayer
from layer6.benchmark import PlaceholderBenchmarkLayer, PostgresExplainBenchmarkLayer
from layer7.ranking import SpeedupRankingLayer
from layer8.analysis import PlaceholderAnalysisLayer
from pipeline.layers import (
    AnalysisLayer,
    BenchmarkLayer,
    CandidateGenerationLayer,
    CandidateNormalizationLayer,
    PromptBuilderLayer,
    RankingLayer,
    ValidationGateLayer,
    WorkloadPreparationLayer,
)
from pipeline.models import AnalysisReport, PipelineRequest, PipelineRunResult, QueryOptimizationResult


class SQLRewriteResearchPipeline:
    """End-to-end orchestrator that composes independently testable layers."""

    def __init__(
        self,
        workload_layer: WorkloadPreparationLayer,
        prompt_layer: PromptBuilderLayer,
        generation_layer: CandidateGenerationLayer,
        normalization_layer: CandidateNormalizationLayer,
        validation_layer: ValidationGateLayer,
        benchmark_layer: BenchmarkLayer,
        ranking_layer: RankingLayer,
        analysis_layer: AnalysisLayer,
    ):
        self._workload_layer = workload_layer
        self._prompt_layer = prompt_layer
        self._generation_layer = generation_layer
        self._normalization_layer = normalization_layer
        self._validation_layer = validation_layer
        self._benchmark_layer = benchmark_layer
        self._ranking_layer = ranking_layer
        self._analysis_layer = analysis_layer

    def run(self, request: PipelineRequest) -> PipelineRunResult:
        workload_items = self._workload_layer.prepare(request)
        results: list[QueryOptimizationResult] = []

        for workload_item in workload_items:
            prompt_package = self._prompt_layer.build(workload_item, request)
            generated_candidates = self._generation_layer.generate(prompt_package)
            normalized_candidates = self._normalization_layer.normalize(generated_candidates)
            validation_report = self._validation_layer.validate(workload_item.raw_query, normalized_candidates)
            benchmark_report = self._benchmark_layer.benchmark(workload_item.raw_query, normalized_candidates)
            ranked_candidates = self._ranking_layer.rank(
                normalized_candidates=normalized_candidates,
                validation_report=validation_report,
                benchmark_report=benchmark_report,
            )
            selected_query = next((candidate.query for candidate in ranked_candidates if candidate.rank == 1), None)
            partial_result = QueryOptimizationResult(
                query_id=workload_item.query_id,
                raw_query=workload_item.raw_query,
                selected_query=selected_query,
                ranked_candidates=tuple(ranked_candidates),
                analysis_report=AnalysisReport(summary=""),
            )
            analysis_report = self._analysis_layer.analyze(partial_result)
            results.append(replace(partial_result, analysis_report=analysis_report))

        return PipelineRunResult(request=request, results=tuple(results))


def build_default_pipeline(
    *,
    dsn: str | None = None,
    validation_settings: ValidationSettings | None = None,
    benchmark_repeats: int = 1,
    statement_timeout_ms: int | None = None,
) -> SQLRewriteResearchPipeline:
    settings = validation_settings or ValidationSettings()
    validation_layer = ValidatorValidationGateLayer(
        executor=QueryExecutor(PostgresAdapter(dsn)) if dsn else None,
        settings=settings,
    )
    benchmark_layer: BenchmarkLayer
    if dsn:
        benchmark_layer = PostgresExplainBenchmarkLayer(
            dsn=dsn,
            repeats=benchmark_repeats,
            statement_timeout_ms=statement_timeout_ms,
        )
    else:
        benchmark_layer = PlaceholderBenchmarkLayer()

    return SQLRewriteResearchPipeline(
        workload_layer=FileOrStringWorkloadPreparationLayer(),
        prompt_layer=DefaultPromptBuilderLayer(),
        generation_layer=DefaultCandidateGenerationLayer(),
        normalization_layer=DefaultCandidateNormalizationLayer(),
        validation_layer=validation_layer,
        benchmark_layer=benchmark_layer,
        ranking_layer=SpeedupRankingLayer(),
        analysis_layer=PlaceholderAnalysisLayer(),
    )
