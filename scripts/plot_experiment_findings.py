from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as exc:  # pragma: no cover - environment dependent
    raise SystemExit(
        "matplotlib is required for plotting. Install it with `./venv/bin/pip install matplotlib`."
    ) from exc


PROMPT_ORDER = ["P0", "P1", "P2", "P3"]
REASONING_ORDER = ["R0", "R1", "R2"]
OUTCOME_ORDER = [
    "strong_improvement",
    "mild_improvement",
    "near_parity",
    "regression",
    "missing_speedup",
    "invalid_selection",
    "benchmark_error",
    "no_selection",
]
OUTCOME_COLORS = {
    "strong_improvement": "#1b9e77",
    "mild_improvement": "#66a61e",
    "near_parity": "#7570b3",
    "regression": "#d95f02",
    "missing_speedup": "#e6ab02",
    "invalid_selection": "#e7298a",
    "benchmark_error": "#666666",
    "no_selection": "#bdbdbd",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _save(fig, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_phase1_combo_leaderboard(report: dict[str, Any], output_dir: Path) -> None:
    rows = report["phase1"]["combo_macro"]
    labels = [row["combo_id"] for row in rows]
    speedups = [row["median_speedup_selected"] or 0.0 for row in rows]
    valid_rates = [row["valid_selection_rate"] * 100.0 for row in rows]

    fig, ax1 = plt.subplots(figsize=(11, 6.5))
    y = list(range(len(labels)))
    bars = ax1.barh(y, speedups, color="#1f77b4", alpha=0.85)
    ax1.set_yticks(y)
    ax1.set_yticklabels(labels, fontsize=9)
    ax1.set_xlabel("Median Selected Speedup")
    ax1.set_title("Phase 1 Combo Leaderboard: Speedup and Valid Selection Rate")
    ax1.axvline(1.0, color="#444444", linestyle="--", linewidth=1)
    ax1.invert_yaxis()

    ax2 = ax1.twiny()
    ax2.plot(valid_rates, y, color="#d62728", marker="o", linewidth=1.8)
    ax2.set_xlabel("Valid Selection Rate (%)")
    ax2.set_xlim(0, 100)

    for bar, value in zip(bars, speedups):
        ax1.text(value + 0.01, bar.get_y() + bar.get_height() / 2, f"{value:.3f}", va="center", fontsize=8)

    _save(fig, output_dir / "phase1_combo_leaderboard.png")


def plot_phase1_prompt_reasoning_heatmap(report: dict[str, Any], output_dir: Path) -> None:
    combo_rows = report["phase1"]["combo_macro"]
    lookup = {(row["prompt_strategy"], row["reasoning_strategy"]): row["median_speedup_selected"] for row in combo_rows}
    data = [[lookup.get((prompt, reasoning), 0.0) or 0.0 for reasoning in REASONING_ORDER] for prompt in PROMPT_ORDER]

    fig, ax = plt.subplots(figsize=(7.5, 5.5))
    im = ax.imshow(data, cmap="YlGnBu", aspect="auto")
    ax.set_xticks(range(len(REASONING_ORDER)))
    ax.set_xticklabels(REASONING_ORDER)
    ax.set_yticks(range(len(PROMPT_ORDER)))
    ax.set_yticklabels(PROMPT_ORDER)
    ax.set_title("Phase 1 Median Speedup by Prompt and Reasoning Strategy")
    ax.set_xlabel("Reasoning Strategy")
    ax.set_ylabel("Prompt Strategy")

    for i, prompt in enumerate(PROMPT_ORDER):
        for j, reasoning in enumerate(REASONING_ORDER):
            ax.text(j, i, f"{data[i][j]:.3f}", ha="center", va="center", color="#111111", fontsize=9)

    fig.colorbar(im, ax=ax, shrink=0.9, label="Median Speedup")
    _save(fig, output_dir / "phase1_prompt_reasoning_heatmap.png")


def plot_phase2_outcome_distribution(report: dict[str, Any], output_dir: Path) -> None:
    rows = report["phase2"]["query_micro"]
    counts = {bucket: 0 for bucket in OUTCOME_ORDER}
    for row in rows:
        counts[row["outcome_bucket"]] = counts.get(row["outcome_bucket"], 0) + 1

    labels = [bucket for bucket in OUTCOME_ORDER if counts.get(bucket, 0) > 0]
    values = [counts[bucket] for bucket in labels]
    colors = [OUTCOME_COLORS.get(bucket, "#999999") for bucket in labels]

    fig, ax = plt.subplots(figsize=(9, 5.2))
    bars = ax.bar(labels, values, color=colors)
    ax.set_title("Phase 2 Outcome Distribution for the Best Combo")
    ax.set_ylabel("Query Count")
    ax.set_xlabel("Outcome Bucket")
    ax.tick_params(axis="x", rotation=25)

    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, value + 0.15, str(value), ha="center", va="bottom", fontsize=9)

    _save(fig, output_dir / "phase2_outcome_distribution.png")


def plot_phase2_query_speedup(report: dict[str, Any], output_dir: Path) -> None:
    rows = [row for row in report["phase2"]["query_micro"] if row["speedup"] is not None]
    rows.sort(key=lambda row: row["speedup"])
    labels = [row["query_id"] for row in rows]
    speedups = [row["speedup"] for row in rows]
    colors = [OUTCOME_COLORS.get(row["outcome_bucket"], "#999999") for row in rows]

    fig, ax = plt.subplots(figsize=(12, 5.8))
    ax.bar(range(len(rows)), speedups, color=colors)
    ax.axhline(1.0, color="#333333", linestyle="--", linewidth=1)
    ax.set_title("Phase 2 Per-Query Speedup for the Best Combo")
    ax.set_ylabel("Speedup")
    ax.set_xlabel("Query")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=75, ha="right", fontsize=8)
    _save(fig, output_dir / "phase2_query_speedup.png")


def plot_phase2_speedup_vs_memory(report: dict[str, Any], output_dir: Path) -> None:
    rows = [row for row in report["phase2"]["query_micro"] if row["speedup"] is not None and row["memory_score"] is not None]

    fig, ax = plt.subplots(figsize=(8.2, 6.2))
    for bucket in OUTCOME_ORDER:
        bucket_rows = [row for row in rows if row["outcome_bucket"] == bucket]
        if not bucket_rows:
            continue
        ax.scatter(
            [row["memory_score"] for row in bucket_rows],
            [row["speedup"] for row in bucket_rows],
            s=58,
            alpha=0.85,
            label=bucket.replace("_", " "),
            color=OUTCOME_COLORS.get(bucket, "#999999"),
        )

    interesting = sorted(rows, key=lambda row: row["speedup"], reverse=True)[:3] + sorted(rows, key=lambda row: row["speedup"])[:3]
    seen: set[str] = set()
    for row in interesting:
        if row["query_id"] in seen:
            continue
        seen.add(row["query_id"])
        ax.annotate(row["query_id"], (row["memory_score"], row["speedup"]), xytext=(5, 5), textcoords="offset points", fontsize=8)

    ax.axhline(1.0, color="#333333", linestyle="--", linewidth=1)
    ax.set_title("Phase 2 Speedup vs Memory Score")
    ax.set_xlabel("Memory Score")
    ax.set_ylabel("Speedup")
    ax.legend(fontsize=8, frameon=False, loc="best")
    _save(fig, output_dir / "phase2_speedup_vs_memory.png")


def plot_phase2_runtime_bucket(report: dict[str, Any], output_dir: Path) -> None:
    rows = report["phase2"]["baseline_runtime_bucket_analysis"]
    if not rows:
        return

    labels = [f"B{row['baseline_runtime_bucket']}" for row in rows]
    means = [row["mean_speedup"] or 0.0 for row in rows]
    medians = [row["median_speedup"] or 0.0 for row in rows]

    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    x = range(len(labels))
    ax.plot(x, means, marker="o", linewidth=2.0, color="#1f77b4", label="Mean speedup")
    ax.plot(x, medians, marker="s", linewidth=2.0, color="#ff7f0e", label="Median speedup")
    ax.axhline(1.0, color="#333333", linestyle="--", linewidth=1)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_title("Phase 2 Speedup by Baseline Runtime Bucket")
    ax.set_xlabel("Baseline Runtime Bucket (increasing runtime)")
    ax.set_ylabel("Speedup")
    ax.legend(frameon=False)
    _save(fig, output_dir / "phase2_speedup_by_runtime_bucket.png")


def plot_phase2_structural_change_comparison(report: dict[str, Any], output_dir: Path) -> None:
    summary = report["phase2"]["feature_outcome_summary"]
    strong = dict(summary.get("strong_improvement", []))
    regress = dict(summary.get("regression", []))
    keys = sorted(set(strong) | set(regress))
    if not keys:
        return

    keys = keys[:8]
    strong_values = [strong.get(key, 0) for key in keys]
    regress_values = [regress.get(key, 0) for key in keys]

    fig, ax = plt.subplots(figsize=(10, 5.8))
    x = list(range(len(keys)))
    width = 0.38
    ax.bar([i - width / 2 for i in x], strong_values, width=width, color=OUTCOME_COLORS["strong_improvement"], label="Strong improvement")
    ax.bar([i + width / 2 for i in x], regress_values, width=width, color=OUTCOME_COLORS["regression"], label="Regression")
    ax.set_xticks(x)
    ax.set_xticklabels(keys, rotation=30, ha="right")
    ax.set_ylabel("Count")
    ax.set_title("Structural Change Patterns: Strong Improvements vs Regressions")
    ax.legend(frameon=False)
    _save(fig, output_dir / "phase2_structural_changes.png")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate report-friendly figures from experiment analysis outputs.")
    parser.add_argument(
        "--analysis-json",
        default="datasets/artifacts/findings_analysis/analysis_report.json",
        help="Path to the analysis_report.json generated by analyze_experiment_results.py",
    )
    parser.add_argument(
        "--output-dir",
        default="datasets/artifacts/findings_figures",
        help="Directory where the generated figure PNGs should be written.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = _read_json(Path(args.analysis_json))
    output_dir = Path(args.output_dir)
    _ensure_dir(output_dir)

    plot_phase1_combo_leaderboard(report, output_dir)
    plot_phase1_prompt_reasoning_heatmap(report, output_dir)
    plot_phase2_outcome_distribution(report, output_dir)
    plot_phase2_query_speedup(report, output_dir)
    plot_phase2_speedup_vs_memory(report, output_dir)
    plot_phase2_runtime_bucket(report, output_dir)
    plot_phase2_structural_change_comparison(report, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
