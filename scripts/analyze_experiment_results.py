from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any, Iterable


SQL_KEYWORD_RE = re.compile(r"\b(select|with|join|where|group by|order by|having|union|intersect)\b", re.IGNORECASE)


@dataclass(slots=True, frozen=True)
class SelectedCandidateView:
    query_id: str
    query_path: str
    combo_id: str
    prompt_strategy: str
    reasoning_strategy: str
    model: str
    selected: bool
    selected_rank: int | None
    score: float | None
    speedup: float | None
    memory_score: float | None
    execution_time_ms: float | None
    planning_time_ms: float | None
    is_valid: bool
    validation_reason: str
    benchmark_error: str | None
    normalization_error: str | None
    raw_query: str
    selected_query: str | None
    baseline_validation_execution_ms: float | None
    baseline_row_count: int | None
    baseline_columns: tuple[str, ...]
    baseline_benchmark_execution_ms: float | None
    baseline_benchmark_memory_score: float | None
    structural_changes: tuple[str, ...]
    outcome_bucket: str


def _safe_read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    idx = (len(ordered) - 1) * p
    lower = math.floor(idx)
    upper = math.ceil(idx)
    if lower == upper:
        return ordered[lower]
    fraction = idx - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _extract_sql_features(sql: str | None) -> dict[str, int | bool]:
    if not sql:
        return {
            "has_cte": False,
            "cte_count": 0,
            "join_count": 0,
            "subquery_count": 0,
            "has_window": False,
            "has_materialized": False,
            "has_not_materialized": False,
            "has_distinct_on": False,
            "has_lateral": False,
            "has_exists": False,
            "has_not_exists": False,
            "has_union": False,
            "has_intersect": False,
            "has_rollup": False,
            "keyword_count": 0,
        }

    lowered = sql.lower()
    cte_count = len(re.findall(r"\bas\s*\(", lowered))
    return {
        "has_cte": lowered.lstrip().startswith("with "),
        "cte_count": cte_count,
        "join_count": len(re.findall(r"\bjoin\b", lowered)),
        "subquery_count": len(re.findall(r"\(\s*select\b", lowered)),
        "has_window": " over " in f" {lowered} ",
        "has_materialized": " materialized " in f" {lowered} ",
        "has_not_materialized": " not materialized " in f" {lowered} ",
        "has_distinct_on": "distinct on" in lowered,
        "has_lateral": " lateral " in f" {lowered} ",
        "has_exists": bool(re.search(r"\bexists\s*\(", lowered)),
        "has_not_exists": bool(re.search(r"\bnot\s+exists\s*\(", lowered)),
        "has_union": " union " in f" {lowered} ",
        "has_intersect": " intersect " in f" {lowered} ",
        "has_rollup": " rollup " in f" {lowered} ",
        "keyword_count": len(SQL_KEYWORD_RE.findall(lowered)),
    }


def _derive_structural_changes(raw_query: str, selected_query: str | None) -> tuple[str, ...]:
    baseline = _extract_sql_features(raw_query)
    selected = _extract_sql_features(selected_query)
    changes: list[str] = []

    if bool(selected["has_window"]) and not bool(baseline["has_window"]):
        changes.append("introduced_window_function")
    if bool(selected["has_materialized"]) and not bool(baseline["has_materialized"]):
        changes.append("introduced_materialized_cte")
    if bool(selected["has_not_materialized"]) and not bool(baseline["has_not_materialized"]):
        changes.append("introduced_not_materialized")
    if bool(selected["has_lateral"]) and not bool(baseline["has_lateral"]):
        changes.append("introduced_lateral")
    if bool(selected["has_distinct_on"]) and not bool(baseline["has_distinct_on"]):
        changes.append("introduced_distinct_on")
    if bool(selected["has_not_exists"]) and not bool(baseline["has_not_exists"]):
        changes.append("introduced_not_exists")
    if int(selected["join_count"]) < int(baseline["join_count"]):
        changes.append("reduced_join_count")
    if int(selected["subquery_count"]) < int(baseline["subquery_count"]):
        changes.append("reduced_subquery_count")
    if int(selected["cte_count"]) > int(baseline["cte_count"]):
        changes.append("more_cte_structure")
    if bool(selected["has_cte"]) and not bool(baseline["has_cte"]):
        changes.append("introduced_cte")
    if int(selected["keyword_count"]) < int(baseline["keyword_count"]):
        changes.append("overall_syntax_simplified")

    return tuple(changes)


def _outcome_bucket(selected: bool, speedup: float | None, is_valid: bool, benchmark_error: str | None) -> str:
    if not selected:
        return "no_selection"
    if not is_valid:
        return "invalid_selection"
    if benchmark_error:
        return "benchmark_error"
    if speedup is None:
        return "missing_speedup"
    if speedup >= 1.10:
        return "strong_improvement"
    if speedup > 1.00:
        return "mild_improvement"
    if speedup >= 0.95:
        return "near_parity"
    return "regression"


def _selected_candidate(trace: dict[str, Any]) -> dict[str, Any] | None:
    ranked = trace.get("ranked_candidates", [])
    selected = next((item for item in ranked if item.get("rank") == 1), None)
    if selected is not None:
        return selected
    if ranked:
        return ranked[0]
    return None


def _flatten_trace(trace: dict[str, Any]) -> SelectedCandidateView:
    selected = _selected_candidate(trace)
    raw_query = trace.get("validation_report", {}).get("raw_query") or ""
    selected_query = selected.get("query") if selected else None
    speedup = selected.get("speedup") if selected else None
    is_valid = bool(selected.get("is_valid")) if selected else False
    benchmark_error = selected.get("benchmark_error") if selected else None
    outcome = _outcome_bucket(selected is not None, speedup, is_valid, benchmark_error)
    return SelectedCandidateView(
        query_id=trace["query_id"],
        query_path=trace.get("query_path", ""),
        combo_id=trace["combo_id"],
        prompt_strategy=trace["prompt_strategy"],
        reasoning_strategy=trace["reasoning_strategy"],
        model=trace["model"],
        selected=selected is not None,
        selected_rank=selected.get("rank") if selected else None,
        score=selected.get("score") if selected else None,
        speedup=speedup,
        memory_score=selected.get("memory_score") if selected else None,
        execution_time_ms=selected.get("execution_time_ms") if selected else None,
        planning_time_ms=selected.get("planning_time_ms") if selected else None,
        is_valid=is_valid,
        validation_reason=selected.get("validation_reason") if selected else "No ranked candidate",
        benchmark_error=benchmark_error,
        normalization_error=selected.get("normalization_error") if selected else None,
        raw_query=raw_query,
        selected_query=selected_query,
        baseline_validation_execution_ms=trace.get("validation_report", {}).get("baseline_execution_time_ms"),
        baseline_row_count=trace.get("validation_report", {}).get("baseline_row_count"),
        baseline_columns=tuple(trace.get("validation_report", {}).get("baseline_columns", [])),
        baseline_benchmark_execution_ms=trace.get("benchmark_report", {}).get("baseline_execution_time_ms"),
        baseline_benchmark_memory_score=trace.get("benchmark_report", {}).get("baseline_memory_score"),
        structural_changes=_derive_structural_changes(raw_query, selected_query),
        outcome_bucket=outcome,
    )


def _group_mean(values: Iterable[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return mean(cleaned) if cleaned else None


def _group_median(values: Iterable[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return median(cleaned) if cleaned else None


def _combo_macro(rows: list[SelectedCandidateView]) -> list[dict[str, Any]]:
    grouped: dict[str, list[SelectedCandidateView]] = defaultdict(list)
    for row in rows:
        grouped[row.combo_id].append(row)

    output: list[dict[str, Any]] = []
    for combo_id, items in grouped.items():
        speedups = [item.speedup for item in items if item.speedup is not None]
        memories = [item.memory_score for item in items if item.memory_score is not None]
        output.append(
            {
                "combo_id": combo_id,
                "prompt_strategy": items[0].prompt_strategy,
                "reasoning_strategy": items[0].reasoning_strategy,
                "model": items[0].model,
                "query_count": len(items),
                "selection_rate": sum(1 for item in items if item.selected) / len(items),
                "valid_selection_rate": sum(1 for item in items if item.selected and item.is_valid) / len(items),
                "benchmark_error_rate": sum(1 for item in items if item.benchmark_error) / len(items),
                "missing_speedup_rate": sum(1 for item in items if item.selected and item.speedup is None and not item.benchmark_error) / len(items),
                "win_rate_gt_1_0": (sum(1 for item in items if item.speedup is not None and item.speedup > 1.0) / len(speedups)) if speedups else 0.0,
                "win_rate_ge_1_1": (sum(1 for item in items if item.speedup is not None and item.speedup >= 1.1) / len(speedups)) if speedups else 0.0,
                "median_speedup_selected": _group_median(speedups),
                "mean_speedup_selected": _group_mean(speedups),
                "median_memory_score_selected": _group_median(memories),
                "mean_memory_score_selected": _group_mean(memories),
                "median_score_selected": _group_median(item.score for item in items),
            }
        )
    return sorted(
        output,
        key=lambda item: (
            -item["valid_selection_rate"],
            -(item["median_speedup_selected"] if item["median_speedup_selected"] is not None else float("-inf")),
            -(item["mean_memory_score_selected"] if item["mean_memory_score_selected"] is not None else float("-inf")),
            item["combo_id"],
        ),
    )


def _axis_macro(rows: list[SelectedCandidateView], axis: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[SelectedCandidateView]] = defaultdict(list)
    for row in rows:
        grouped[getattr(row, axis)].append(row)

    output: list[dict[str, Any]] = []
    for key, items in grouped.items():
        speedups = [item.speedup for item in items if item.speedup is not None]
        output.append(
            {
                axis: key,
                "trace_count": len(items),
                "selection_rate": sum(1 for item in items if item.selected) / len(items),
                "valid_selection_rate": sum(1 for item in items if item.selected and item.is_valid) / len(items),
                "median_speedup_selected": _group_median(speedups),
                "mean_speedup_selected": _group_mean(speedups),
                "median_memory_score_selected": _group_median(item.memory_score for item in items),
                "missing_speedup_count": sum(1 for item in items if item.selected and item.speedup is None and not item.benchmark_error),
            }
        )
    return sorted(output, key=lambda item: str(item[axis]))


def _per_query_phase1(rows: list[SelectedCandidateView]) -> list[dict[str, Any]]:
    grouped: dict[str, list[SelectedCandidateView]] = defaultdict(list)
    for row in rows:
        grouped[row.query_id].append(row)

    output: list[dict[str, Any]] = []
    for query_id, items in grouped.items():
        valid_items = [item for item in items if item.selected and item.is_valid]
        speedups = [item.speedup for item in valid_items if item.speedup is not None]
        best_item = max(
            valid_items,
            key=lambda item: (item.speedup if item.speedup is not None else float("-inf"), item.combo_id),
            default=None,
        )
        output.append(
            {
                "query_id": query_id,
                "query_path": items[0].query_path,
                "combo_count": len(items),
                "valid_selection_count": len(valid_items),
                "best_combo_id": best_item.combo_id if best_item else None,
                "best_speedup": best_item.speedup if best_item else None,
                "median_speedup_across_valid": _group_median(speedups),
                "speedup_stddev_across_valid": pstdev(speedups) if len(speedups) > 1 else 0.0,
                "missing_speedup_count": sum(1 for item in valid_items if item.speedup is None),
                "benchmark_error_count": sum(1 for item in items if item.benchmark_error),
            }
        )
    return sorted(
        output,
        key=lambda item: (
            item["valid_selection_count"],
            -(item["best_speedup"] if item["best_speedup"] is not None else float("-inf")),
            item["query_id"],
        ),
    )


def _phase2_query_rows(rows: list[SelectedCandidateView]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "query_id": row.query_id,
                "query_path": row.query_path,
                "combo_id": row.combo_id,
                "selected": row.selected,
                "is_valid": row.is_valid,
                "outcome_bucket": row.outcome_bucket,
                "score": row.score,
                "speedup": row.speedup,
                "memory_score": row.memory_score,
                "execution_time_ms": row.execution_time_ms,
                "planning_time_ms": row.planning_time_ms,
                "baseline_validation_execution_ms": row.baseline_validation_execution_ms,
                "baseline_benchmark_execution_ms": row.baseline_benchmark_execution_ms,
                "baseline_row_count": row.baseline_row_count,
                "benchmark_error": row.benchmark_error,
                "validation_reason": row.validation_reason,
                "structural_changes": list(row.structural_changes),
            }
        )
    return sorted(
        output,
        key=lambda item: (
            {"strong_improvement": 0, "mild_improvement": 1, "near_parity": 2, "regression": 3, "missing_speedup": 4, "benchmark_error": 5, "no_selection": 6, "invalid_selection": 7}.get(item["outcome_bucket"], 99),
            -(item["speedup"] if item["speedup"] is not None else float("-inf")),
            item["query_id"],
        ),
    )


def _feature_outcome_summary(rows: list[SelectedCandidateView]) -> dict[str, Any]:
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        for change in row.structural_changes:
            grouped[row.outcome_bucket][change] += 1

    return {
        outcome: counter.most_common(10)
        for outcome, counter in sorted(grouped.items())
    }


def _data_quality_summary(rows: list[SelectedCandidateView]) -> dict[str, Any]:
    return {
        "trace_count": len(rows),
        "selected_count": sum(1 for row in rows if row.selected),
        "selected_with_missing_speedup": sum(1 for row in rows if row.selected and row.speedup is None and not row.benchmark_error),
        "selected_with_missing_baseline_benchmark_time": sum(1 for row in rows if row.selected and row.baseline_benchmark_execution_ms is None),
        "selected_with_benchmark_error": sum(1 for row in rows if row.selected and row.benchmark_error),
        "selected_with_missing_query_text": sum(1 for row in rows if row.selected and not row.selected_query),
    }


def _top_examples(rows: list[SelectedCandidateView], *, key_name: str, count: int, reverse: bool) -> list[dict[str, Any]]:
    filtered = [row for row in rows if row.speedup is not None]
    filtered.sort(key=lambda row: (row.speedup, row.query_id), reverse=reverse)
    output: list[dict[str, Any]] = []
    for row in filtered[:count]:
        output.append(
            {
                "query_id": row.query_id,
                "query_path": row.query_path,
                "combo_id": row.combo_id,
                "speedup": row.speedup,
                "memory_score": row.memory_score,
                "outcome_bucket": row.outcome_bucket,
                "structural_changes": list(row.structural_changes),
                key_name: row.speedup,
            }
        )
    return output


def _phase2_macro(rows: list[SelectedCandidateView]) -> dict[str, Any]:
    selected_rows = [row for row in rows if row.selected]
    valid_rows = [row for row in selected_rows if row.is_valid]
    speedups = [row.speedup for row in valid_rows if row.speedup is not None]
    memories = [row.memory_score for row in valid_rows if row.memory_score is not None]
    return {
        "query_count": len(rows),
        "selected_count": len(selected_rows),
        "valid_selected_count": len(valid_rows),
        "selection_rate": len(selected_rows) / len(rows) if rows else 0.0,
        "valid_selection_rate": len(valid_rows) / len(rows) if rows else 0.0,
        "strong_improvement_count": sum(1 for row in rows if row.outcome_bucket == "strong_improvement"),
        "mild_improvement_count": sum(1 for row in rows if row.outcome_bucket == "mild_improvement"),
        "near_parity_count": sum(1 for row in rows if row.outcome_bucket == "near_parity"),
        "regression_count": sum(1 for row in rows if row.outcome_bucket == "regression"),
        "missing_speedup_count": sum(1 for row in rows if row.outcome_bucket == "missing_speedup"),
        "benchmark_error_count": sum(1 for row in rows if row.outcome_bucket == "benchmark_error"),
        "median_speedup": _group_median(speedups),
        "mean_speedup": _group_mean(speedups),
        "p25_speedup": _percentile(speedups, 0.25),
        "p75_speedup": _percentile(speedups, 0.75),
        "median_memory_score": _group_median(memories),
        "mean_memory_score": _group_mean(memories),
    }


def _phase2_baseline_bucket_analysis(rows: list[SelectedCandidateView]) -> list[dict[str, Any]]:
    valid_rows = [row for row in rows if row.baseline_validation_execution_ms is not None and row.speedup is not None]
    if len(valid_rows) < 4:
        return []

    ordered = sorted(valid_rows, key=lambda row: row.baseline_validation_execution_ms or 0.0)
    chunk = max(1, len(ordered) // 4)
    buckets: list[list[SelectedCandidateView]] = [ordered[i : i + chunk] for i in range(0, len(ordered), chunk)]
    output: list[dict[str, Any]] = []
    for idx, bucket in enumerate(buckets, start=1):
        output.append(
            {
                "baseline_runtime_bucket": idx,
                "query_count": len(bucket),
                "baseline_validation_execution_ms_min": min(row.baseline_validation_execution_ms or 0.0 for row in bucket),
                "baseline_validation_execution_ms_max": max(row.baseline_validation_execution_ms or 0.0 for row in bucket),
                "mean_speedup": _group_mean(row.speedup for row in bucket),
                "median_speedup": _group_median(row.speedup for row in bucket),
            }
        )
    return output


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _render_markdown(
    phase1_combo_macro: list[dict[str, Any]],
    phase1_prompt_macro: list[dict[str, Any]],
    phase1_reason_macro: list[dict[str, Any]],
    phase1_query_micro: list[dict[str, Any]],
    phase2_macro: dict[str, Any],
    phase2_query_rows: list[dict[str, Any]],
    phase2_feature_summary: dict[str, Any],
    phase1_quality: dict[str, Any],
    phase2_quality: dict[str, Any],
) -> str:
    lines: list[str] = [
        "# Findings Analysis Summary",
        "",
        "This file is generated locally from experiment artifacts. It is intended to support the report `Findings` section with both macro and micro views.",
        "",
        "## Phase 1: Combo Comparison",
        "",
    ]

    if phase1_combo_macro:
        top_combo = phase1_combo_macro[0]
        lines.extend(
            [
                f"- Best combo by valid-selection-first ordering: `{top_combo['combo_id']}`",
                f"- Valid selection rate: `{top_combo['valid_selection_rate']:.2%}`",
                f"- Median selected speedup: `{(top_combo['median_speedup_selected'] or 0.0):.4f}`",
                f"- Mean selected memory score: `{(top_combo['mean_memory_score_selected'] or 0.0):.4f}`",
                "",
                "### Notable Phase 1 combo observations",
            ]
        )
        for item in phase1_combo_macro[:5]:
            lines.append(
                f"- `{item['combo_id']}`: valid rate `{item['valid_selection_rate']:.2%}`, "
                f"median speedup `{(item['median_speedup_selected'] or 0.0):.4f}`, "
                f"missing-speedup rate `{item['missing_speedup_rate']:.2%}`"
            )

    if phase1_prompt_macro:
        lines.extend(["", "### Prompt-level view"])
        for item in phase1_prompt_macro:
            lines.append(
                f"- `{item['prompt_strategy']}`: valid rate `{item['valid_selection_rate']:.2%}`, "
                f"median speedup `{(item['median_speedup_selected'] or 0.0):.4f}`"
            )

    if phase1_reason_macro:
        lines.extend(["", "### Reasoning-level view"])
        for item in phase1_reason_macro:
            lines.append(
                f"- `{item['reasoning_strategy']}`: valid rate `{item['valid_selection_rate']:.2%}`, "
                f"median speedup `{(item['median_speedup_selected'] or 0.0):.4f}`"
            )

    if phase1_query_micro:
        lines.extend(["", "### Hardest / most variable Phase 1 queries"])
        for item in phase1_query_micro[:5]:
            lines.append(
                f"- `{item['query_id']}`: valid selections `{item['valid_selection_count']}/{item['combo_count']}`, "
                f"best combo `{item['best_combo_id']}`, best speedup `{item['best_speedup']}`"
            )

    lines.extend(
        [
            "",
            "## Phase 2: Best Combo Evaluation",
            "",
            f"- Query count: `{phase2_macro['query_count']}`",
            f"- Valid selected count: `{phase2_macro['valid_selected_count']}`",
            f"- Median speedup: `{phase2_macro['median_speedup']}`",
            f"- Mean speedup: `{phase2_macro['mean_speedup']}`",
            f"- Median memory score: `{phase2_macro['median_memory_score']}`",
            f"- Strong improvements (`>= 1.10x`): `{phase2_macro['strong_improvement_count']}`",
            f"- Mild improvements (`1.00x - 1.10x`): `{phase2_macro['mild_improvement_count']}`",
            f"- Near parity (`0.95x - 1.00x`): `{phase2_macro['near_parity_count']}`",
            f"- Regressions (`< 0.95x`): `{phase2_macro['regression_count']}`",
            f"- Missing speedup despite selection: `{phase2_macro['missing_speedup_count']}`",
            "",
            "### Phase 2 outcome buckets",
        ]
    )
    bucket_counter = Counter(item["outcome_bucket"] for item in phase2_query_rows)
    for bucket, count in bucket_counter.items():
        lines.append(f"- `{bucket}`: `{count}`")

    lines.extend(["", "### Common structural changes by outcome bucket"])
    for bucket, pairs in phase2_feature_summary.items():
        if not pairs:
            continue
        summary = ", ".join(f"{name} ({count})" for name, count in pairs[:5])
        lines.append(f"- `{bucket}`: {summary}")

    lines.extend(
        [
            "",
            "## Data Quality Checks",
            "",
            f"- Phase 1 selected rows with missing speedup: `{phase1_quality['selected_with_missing_speedup']}`",
            f"- Phase 1 selected rows with missing baseline benchmark time: `{phase1_quality['selected_with_missing_baseline_benchmark_time']}`",
            f"- Phase 2 selected rows with missing speedup: `{phase2_quality['selected_with_missing_speedup']}`",
            f"- Phase 2 selected rows with missing baseline benchmark time: `{phase2_quality['selected_with_missing_baseline_benchmark_time']}`",
            "",
            "These checks are useful when writing the findings section because missing baseline benchmark time can suppress speedup computation even when a candidate was validated and benchmarked successfully.",
        ]
    )
    return "\n".join(lines) + "\n"


def analyze(phase1_dir: Path, phase2_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    phase1_traces = _read_jsonl(phase1_dir / "per_query_results.jsonl")
    phase2_traces = _read_jsonl(phase2_dir / "per_query_results.jsonl")

    phase1_rows = [_flatten_trace(trace) for trace in phase1_traces]
    phase2_rows = [_flatten_trace(trace) for trace in phase2_traces]

    phase1_combo_macro = _combo_macro(phase1_rows)
    phase1_prompt_macro = _axis_macro(phase1_rows, "prompt_strategy")
    phase1_reason_macro = _axis_macro(phase1_rows, "reasoning_strategy")
    phase1_model_macro = _axis_macro(phase1_rows, "model")
    phase1_query_micro = _per_query_phase1(phase1_rows)

    phase2_macro = _phase2_macro(phase2_rows)
    phase2_query_micro = _phase2_query_rows(phase2_rows)
    phase2_feature_summary = _feature_outcome_summary(phase2_rows)
    phase2_baseline_buckets = _phase2_baseline_bucket_analysis(phase2_rows)

    phase1_quality = _data_quality_summary(phase1_rows)
    phase2_quality = _data_quality_summary(phase2_rows)

    report = {
        "phase1": {
            "combo_macro": phase1_combo_macro,
            "prompt_macro": phase1_prompt_macro,
            "reasoning_macro": phase1_reason_macro,
            "model_macro": phase1_model_macro,
            "query_micro": phase1_query_micro,
            "data_quality": phase1_quality,
            "top_combo_examples": _top_examples(phase1_rows, key_name="best_speedup", count=10, reverse=True),
            "worst_combo_examples": _top_examples(phase1_rows, key_name="worst_speedup", count=10, reverse=False),
        },
        "phase2": {
            "macro": phase2_macro,
            "query_micro": phase2_query_micro,
            "feature_outcome_summary": phase2_feature_summary,
            "baseline_runtime_bucket_analysis": phase2_baseline_buckets,
            "data_quality": phase2_quality,
            "top_improvements": _top_examples(phase2_rows, key_name="speedup", count=10, reverse=True),
            "worst_regressions": _top_examples(phase2_rows, key_name="speedup", count=10, reverse=False),
        },
    }

    _write_json(output_dir / "analysis_report.json", report)
    _write_csv(output_dir / "phase1_combo_macro.csv", phase1_combo_macro)
    _write_csv(output_dir / "phase1_prompt_macro.csv", phase1_prompt_macro)
    _write_csv(output_dir / "phase1_reasoning_macro.csv", phase1_reason_macro)
    _write_csv(output_dir / "phase1_model_macro.csv", phase1_model_macro)
    _write_csv(output_dir / "phase1_query_micro.csv", phase1_query_micro)
    _write_csv(output_dir / "phase2_query_micro.csv", phase2_query_micro)
    _write_csv(output_dir / "phase2_baseline_runtime_buckets.csv", phase2_baseline_buckets)

    markdown = _render_markdown(
        phase1_combo_macro,
        phase1_prompt_macro,
        phase1_reason_macro,
        phase1_query_micro,
        phase2_macro,
        phase2_query_micro,
        phase2_feature_summary,
        phase1_quality,
        phase2_quality,
    )
    (output_dir / "findings_summary.md").write_text(markdown, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze phase 1 and phase 2 experiment artifacts for report findings.")
    parser.add_argument(
        "--phase1-dir",
        default="datasets/artifacts/candidate_combo/phase1_candidate_combo_20260408T092456Z",
        help="Path to the phase 1 artifact directory.",
    )
    parser.add_argument(
        "--phase2-dir",
        default="datasets/artifacts/fullset_combo/phase2_fullset_combo_20260408T202807Z",
        help="Path to the phase 2 artifact directory.",
    )
    parser.add_argument(
        "--output-dir",
        default="datasets/artifacts/findings_analysis",
        help="Directory where analysis outputs should be written.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    analyze(Path(args.phase1_dir), Path(args.phase2_dir), Path(args.output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
