from __future__ import annotations

import csv
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from random import Random
from statistics import mean, median
from typing import Any, Iterable, Optional

from config.settings import ValidationSettings
from db.postgres_adapter import PostgresAdapter
from execution.query_executor import QueryExecutor
from experiments.benchmark import CachedPostgresBenchmarkLayer
from experiments.models import (
    ExperimentCombo,
    ExperimentComboSummary,
    ExperimentPhaseConfig,
    ExperimentPhaseResult,
    ExperimentQueryTrace,
)
from experiments.prompt_builder import ExperimentPromptBuilderLayer
from experiments.validation import CachedValidationGateLayer
from layer1.workload_preparation import FileOrStringWorkloadPreparationLayer
from layer3.generation_layer import DefaultCandidateGenerationLayer
from layer4.candidate_normalizer import DefaultCandidateNormalizationLayer
from layer7.ranking import SpeedupRankingLayer
from layer8.analysis import PlaceholderAnalysisLayer
from pipeline.models import AnalysisReport, PipelineRequest, QueryOptimizationResult


class QueryExperimentRunner:
    def __init__(
        self,
        *,
        dsn: str,
        schema_file: str,
        artifacts_root: str,
        validation_settings: ValidationSettings,
        statement_timeout_ms: int | None = None,
    ):
        self._dsn = dsn
        self._schema_file = schema_file
        self._artifacts_root = Path(artifacts_root)
        self._validation_settings = validation_settings
        self._statement_timeout_ms = statement_timeout_ms

    def run_phase(
        self,
        *,
        phase_config: ExperimentPhaseConfig,
        query_files: list[str],
        combos: list[ExperimentCombo],
        destination_subdir: str,
    ) -> ExperimentPhaseResult:
        sampled_query_files = self._sample_queries(query_files, phase_config.sample_size, phase_config.random_seed)
        run_dir = self._create_run_dir(destination_subdir, phase_config.phase_name)
        self._write_json(
            run_dir / "run_config.json",
            {
                "phase_name": phase_config.phase_name,
                "sample_size": phase_config.sample_size,
                "benchmark_repeats": phase_config.benchmark_repeats,
                "random_seed": phase_config.random_seed,
                "dsn": self._dsn,
                "schema_file": self._schema_file,
                "sampled_query_files": sampled_query_files,
                "combos": [asdict(combo) for combo in combos],
            },
        )

        workload_items = FileOrStringWorkloadPreparationLayer().prepare(
            PipelineRequest(
                query_files=tuple(sampled_query_files),
                engine="postgresql",
                schema_file=self._schema_file,
            )
        )

        adapter = PostgresAdapter(self._dsn)
        executor = QueryExecutor(adapter)
        prompt_layer = ExperimentPromptBuilderLayer(dsn=self._dsn, schema_file=self._schema_file)
        generation_layer = DefaultCandidateGenerationLayer()
        normalization_layer = DefaultCandidateNormalizationLayer()
        validation_layer = CachedValidationGateLayer(executor=executor, settings=self._validation_settings)
        benchmark_layer = CachedPostgresBenchmarkLayer(
            dsn=self._dsn,
            repeats=phase_config.benchmark_repeats,
            statement_timeout_ms=self._statement_timeout_ms,
        )
        ranking_layer = SpeedupRankingLayer()
        analysis_layer = PlaceholderAnalysisLayer()

        try:
            traces: list[ExperimentQueryTrace] = []
            for combo in combos:
                for workload_item in workload_items:
                    request = PipelineRequest(
                        engine="postgresql",
                        prompt_strategy=combo.prompt_strategy,
                        reasoning_mode=combo.reasoning_strategy,
                        model=combo.model,
                        candidate_count=3,
                        benchmark_repeats=phase_config.benchmark_repeats,
                        statement_timeout_ms=self._statement_timeout_ms,
                        validation_settings=self._validation_settings,
                    )
                    prompt_package = prompt_layer.build(workload_item, request)
                    generated_candidates = generation_layer.generate(prompt_package)
                    normalized_candidates = normalization_layer.normalize(generated_candidates)
                    validation_report = validation_layer.validate(workload_item.raw_query, normalized_candidates)
                    benchmark_report = benchmark_layer.benchmark(workload_item.raw_query, normalized_candidates)
                    ranked_candidates = ranking_layer.rank(normalized_candidates, validation_report, benchmark_report)
                    selected_query = next((candidate.query for candidate in ranked_candidates if candidate.rank == 1), None)
                    analysis_report = analysis_layer.analyze(
                        QueryOptimizationResult(
                            query_id=workload_item.query_id,
                            raw_query=workload_item.raw_query,
                            selected_query=selected_query,
                            ranked_candidates=tuple(ranked_candidates),
                            analysis_report=AnalysisReport(summary=""),
                        )
                    )
                    query_trace = ExperimentQueryTrace(
                        phase_name=phase_config.phase_name,
                        query_id=workload_item.query_id,
                        query_path=workload_item.source_path or "",
                        combo_id=combo.combo_id,
                        prompt_strategy=combo.prompt_strategy,
                        reasoning_strategy=combo.reasoning_strategy,
                        model=combo.model,
                        prompt_text=prompt_package.prompt_text,
                        stage1_prompt_text=prompt_package.stage1_prompt_text,
                        selected_query=selected_query,
                        ranked_candidates=tuple(ranked_candidates),
                        validation_report=validation_report.to_dict(),
                        benchmark_report=asdict(benchmark_report),
                        analysis_report=asdict(analysis_report),
                    )
                    traces.append(query_trace)
                    self._persist_query_artifacts(run_dir, combo, query_trace, generated_candidates, normalized_candidates)

            summaries = self._summarize_combos(phase_config.phase_name, combos, traces)
            summaries = sorted(
                summaries,
                key=lambda item: (
                    -(item.valid_selected_count / item.query_count if item.query_count else 0.0),
                    -(item.benchmark_success_count / item.query_count if item.query_count else 0.0),
                    -(item.median_speedup or float("-inf")),
                    -(item.median_memory_score or float("-inf")),
                    -(item.average_score or float("-inf")),
                    item.combo_id,
                ),
            )
            best_combo = next(
                (
                    ExperimentCombo(
                        prompt_strategy=summary.prompt_strategy,
                        reasoning_strategy=summary.reasoning_strategy,
                        model=summary.model,
                    )
                    for summary in summaries
                ),
                None,
            )

            phase_result = ExperimentPhaseResult(
                phase_config=phase_config,
                sampled_query_paths=tuple(sampled_query_files),
                combo_summaries=tuple(summaries),
                query_traces=tuple(traces),
                selected_best_combo=best_combo,
            )
            self._write_json(run_dir / "summary.json", phase_result.to_dict())
            self._write_jsonl(run_dir / "per_query_results.jsonl", [trace.to_dict() for trace in traces])
            self._write_combo_summary_csv(run_dir / "summary.csv", summaries)
            if best_combo is not None:
                self._write_json(run_dir / "selected_best_combo.json", asdict(best_combo))
            return phase_result
        finally:
            adapter.close()

    @staticmethod
    def default_phase1_config() -> ExperimentPhaseConfig:
        return ExperimentPhaseConfig(phase_name="phase1_candidate_combo", sample_size=1, benchmark_repeats=1, random_seed=20260407)

    @staticmethod
    def default_phase2_config() -> ExperimentPhaseConfig:
        return ExperimentPhaseConfig(phase_name="phase2_fullset_combo", sample_size=30, benchmark_repeats=5, random_seed=20260408)

    @staticmethod
    def default_gpt5_combos() -> list[ExperimentCombo]:
        # prompt_strategies = ["P0", "P1", "P2", "P3", "P4"]
        prompt_strategies = ["P0"]
        # reasoning_strategies = ["R0", "R1", "R2"]
        reasoning_strategies = ["R0"]
        # models = ["gpt-5", "gpt-5-mini", "gpt-5-nano"]
        models = ["gpt-5-mini"]
        return [
            ExperimentCombo(prompt_strategy=
                            prompt, reasoning_strategy=reasoning, model=model)
            for prompt in prompt_strategies
            for reasoning in reasoning_strategies
            for model in models
        ]

    @staticmethod
    def default_gpt54_combos() -> list[ExperimentCombo]:
        prompt_strategies = ["P0", "P1", "P2", "P3", "P4"]
        reasoning_strategies = ["R0", "R1", "R2"]
        models = ["gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano"]
        return [
            ExperimentCombo(prompt_strategy=prompt, reasoning_strategy=reasoning, model=model)
            for prompt in prompt_strategies
            for reasoning in reasoning_strategies
            for model in models
        ]

    @staticmethod
    def _sample_queries(query_files: list[str], sample_size: int, seed: int) -> list[str]:
        if sample_size > len(query_files):
            raise ValueError("sample_size cannot exceed the number of available query files")
        rng = Random(seed)
        return sorted(rng.sample(query_files, sample_size))

    @staticmethod
    def _summarize_combos(
        phase_name: str,
        combos: list[ExperimentCombo],
        traces: list[ExperimentQueryTrace],
    ) -> list[ExperimentComboSummary]:
        traces_by_combo: dict[str, list[ExperimentQueryTrace]] = {combo.combo_id: [] for combo in combos}
        for trace in traces:
            traces_by_combo.setdefault(trace.combo_id, []).append(trace)

        summaries: list[ExperimentComboSummary] = []
        for combo in combos:
            combo_traces = traces_by_combo.get(combo.combo_id, [])
            selected_candidates = [next((candidate for candidate in trace.ranked_candidates if candidate.rank == 1), None) for trace in combo_traces]
            valid_selected = [candidate for candidate in selected_candidates if candidate and candidate.is_valid]
            benchmark_success = [candidate for candidate in selected_candidates if candidate and candidate.benchmark_error is None and candidate.execution_time_ms is not None]
            speedups = [candidate.speedup for candidate in selected_candidates if candidate and candidate.speedup is not None]
            memory_scores = [candidate.memory_score for candidate in selected_candidates if candidate and candidate.memory_score is not None]
            scores = [candidate.score for candidate in selected_candidates if candidate and candidate.score is not None]

            summaries.append(
                ExperimentComboSummary(
                    phase_name=phase_name,
                    combo_id=combo.combo_id,
                    prompt_strategy=combo.prompt_strategy,
                    reasoning_strategy=combo.reasoning_strategy,
                    model=combo.model,
                    query_count=len(combo_traces),
                    selected_count=sum(1 for candidate in selected_candidates if candidate is not None),
                    valid_selected_count=len(valid_selected),
                    benchmark_success_count=len(benchmark_success),
                    median_speedup=median(speedups) if speedups else None,
                    median_memory_score=median(memory_scores) if memory_scores else None,
                    average_score=mean(scores) if scores else None,
                )
            )
        return summaries

    @staticmethod
    def _write_combo_summary_csv(path: Path, summaries: list[ExperimentComboSummary]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "phase_name",
                    "combo_id",
                    "prompt_strategy",
                    "reasoning_strategy",
                    "model",
                    "query_count",
                    "selected_count",
                    "valid_selected_count",
                    "benchmark_success_count",
                    "median_speedup",
                    "median_memory_score",
                    "average_score",
                ],
            )
            writer.writeheader()
            for summary in summaries:
                writer.writerow(summary.to_dict())

    def _persist_query_artifacts(
        self,
        run_dir: Path,
        combo: ExperimentCombo,
        trace: ExperimentQueryTrace,
        generated_candidates: list[Any],
        normalized_candidates: list[Any],
    ) -> None:
        artifact_dir = run_dir / "artifacts" / combo.combo_id / trace.query_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "prompt.txt").write_text(trace.prompt_text, encoding="utf-8")
        if trace.stage1_prompt_text:
            (artifact_dir / "stage1_prompt.txt").write_text(trace.stage1_prompt_text, encoding="utf-8")
        for generated in generated_candidates:
            (artifact_dir / f"{generated.candidate_id}_raw.txt").write_text(generated.raw_text, encoding="utf-8")
        for normalized in normalized_candidates:
            if normalized.sql:
                (artifact_dir / f"{normalized.candidate_id}.sql").write_text(normalized.sql, encoding="utf-8")
            if normalized.normalization_error:
                (artifact_dir / f"{normalized.candidate_id}_normalization_error.txt").write_text(normalized.normalization_error, encoding="utf-8")
        if trace.selected_query:
            (artifact_dir / "selected_query.sql").write_text(trace.selected_query, encoding="utf-8")
        self._write_json(artifact_dir / "validation.json", trace.validation_report)
        self._write_json(artifact_dir / "benchmark.json", trace.benchmark_report)
        self._write_json(artifact_dir / "ranked_candidates.json", [asdict(candidate) for candidate in trace.ranked_candidates])

    def _create_run_dir(self, destination_subdir: str, phase_name: str) -> Path:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = self._artifacts_root / destination_subdir / f"{phase_name}_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    @staticmethod
    def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
