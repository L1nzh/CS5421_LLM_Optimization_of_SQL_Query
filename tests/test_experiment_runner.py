from __future__ import annotations

from experiments.models import ExperimentCombo
from experiments.runner import QueryExperimentRunner
from pipeline.models import RankedCandidate


def test_sample_queries_is_deterministic() -> None:
    query_files = [f"query_{idx}.sql" for idx in range(1, 100)]

    first = QueryExperimentRunner._sample_queries(query_files, 10, 1234)
    second = QueryExperimentRunner._sample_queries(query_files, 10, 1234)

    assert first == second
    assert len(first) == 10


def test_default_gpt5_combos_cover_all_prompt_reasoning_model_combinations() -> None:
    combos = QueryExperimentRunner.default_gpt5_combos()

    assert len(combos) == 36
    assert ExperimentCombo(prompt_strategy="P0", reasoning_strategy="R0", model="gpt-5") in combos
    assert ExperimentCombo(prompt_strategy="P3", reasoning_strategy="R2", model="gpt-5-nano") in combos


def test_combo_summary_prioritizes_valid_selected_candidates() -> None:
    combo = ExperimentCombo(prompt_strategy="P1", reasoning_strategy="R0", model="gpt-5")
    trace = type("Trace", (), {})()
    trace.combo_id = combo.combo_id
    trace.ranked_candidates = (
        RankedCandidate(
            candidate_id="c1",
            query="SELECT 1",
            raw_text="SELECT 1",
            model="gpt-5",
            rank=1,
            score=1.5,
            is_valid=True,
            validation_reason="Equivalent",
            speedup=1.5,
            memory_score=0.8,
        ),
    )
    summaries = QueryExperimentRunner._summarize_combos("phase1", [combo], [trace])

    assert len(summaries) == 1
    assert summaries[0].valid_selected_count == 1
    assert summaries[0].median_speedup == 1.5
    assert summaries[0].median_memory_score == 0.8
