from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Optional


@dataclass(frozen=True)
class ModelSummary:
    model: str
    expected_queries: int
    succeeded: int
    speedups: list[float]

    @property
    def success_rate(self) -> Optional[float]:
        if self.expected_queries <= 0:
            return None
        return self.succeeded / self.expected_queries

    @property
    def median_speedup(self) -> Optional[float]:
        if not self.speedups:
            return None
        return float(median(self.speedups))

    @property
    def better_than_baseline_ratio(self) -> Optional[float]:
        if not self.speedups:
            return None
        better = sum(1 for s in self.speedups if s > 1.0)
        return better / len(self.speedups)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected JSON root (expected object): {path}")
    return payload


def _parse_baseline_medians(payload: dict[str, Any]) -> dict[str, Optional[float]]:
    results = payload.get("results", [])
    if not isinstance(results, list):
        raise ValueError("Unexpected baseline JSON: missing 'results' list")
    out: dict[str, Optional[float]] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        qid = r.get("query_id")
        if not isinstance(qid, str):
            continue
        med = r.get("median_execution_time_ms")
        out[qid] = float(med) if isinstance(med, (int, float)) else None
    return out


def _extract_models(compare_payload: dict[str, Any]) -> list[str]:
    models = compare_payload.get("models", [])
    if isinstance(models, list) and all(isinstance(m, str) for m in models):
        return list(models)
    results = compare_payload.get("results", [])
    if not isinstance(results, list):
        return []
    seen: list[str] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        m = r.get("model")
        if isinstance(m, str) and m not in seen:
            seen.append(m)
    return seen


def _summarize_by_model(
    baseline_medians: dict[str, Optional[float]],
    compare_payload: dict[str, Any],
) -> tuple[list[ModelSummary], list[str]]:
    results = compare_payload.get("results", [])
    if not isinstance(results, list):
        raise ValueError("Unexpected compare JSON: missing 'results' list")

    expected_qids = sorted(baseline_medians.keys()) if baseline_medians else []
    if not expected_qids:
        seen_qids: set[str] = set()
        for r in results:
            if isinstance(r, dict) and isinstance(r.get("query_id"), str):
                seen_qids.add(r["query_id"])
        expected_qids = sorted(seen_qids)

    per_model: dict[str, dict[str, dict[str, Any]]] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        model = r.get("model")
        qid = r.get("query_id")
        if not isinstance(model, str) or not isinstance(qid, str):
            continue
        per_model.setdefault(model, {})[qid] = r

    summaries: list[ModelSummary] = []
    for model in _extract_models(compare_payload) or sorted(per_model.keys()):
        qmap = per_model.get(model, {})
        succeeded = 0
        speedups: list[float] = []
        for qid in expected_qids:
            r = qmap.get(qid)
            if not isinstance(r, dict):
                continue
            if r.get("success") is True:
                succeeded += 1
                s = r.get("speedup")
                if isinstance(s, (int, float)):
                    speedups.append(float(s))

        summaries.append(
            ModelSummary(
                model=model,
                expected_queries=len(expected_qids),
                succeeded=succeeded,
                speedups=speedups,
            )
        )

    return summaries, expected_qids


def _fmt_ratio(num: int, den: int) -> str:
    if den <= 0:
        return "N/A"
    return f"{num}/{den} ({(num / den) * 100:.1f}%)"


def _fmt_float(value: Optional[float], digits: int = 3) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


def _build_markdown(
    baseline_path: Path,
    compare_path: Path,
    baseline_payload: dict[str, Any],
    compare_payload: dict[str, Any],
    summaries: list[ModelSummary],
    expected_qids: list[str],
) -> str:
    baseline_repeat = baseline_payload.get("repeat")
    compare_repeat = compare_payload.get("repeat")
    statement_timeout_ms = compare_payload.get("statement_timeout_ms")
    workload_dir = compare_payload.get("workload_dir") or baseline_payload.get("workload_dir")
    dsn = compare_payload.get("dsn")
    generated_at = compare_payload.get("generated_at") or datetime.now().isoformat()

    lines: list[str] = []
    lines.append("# PostgreSQL TPC-DS(SF=1) — Baseline vs Layer3 汇总报告")
    lines.append("")
    baseline_dry_run = baseline_payload.get("dry_run") is True
    compare_dry_run = compare_payload.get("dry_run") is True
    if baseline_dry_run or compare_dry_run:
        parts: list[str] = []
        if baseline_dry_run:
            parts.append("baseline.dry_run=true")
        if compare_dry_run:
            parts.append("compare.dry_run=true")
        lines.append(f"注意：检测到 {'、'.join(parts)}，本报告指标仅用于联调，不代表真实性能。")
        lines.append("")
    lines.append("## 1. 汇总指标（按模型）")
    lines.append("")
    lines.append(
        "| model | success | median speedup | 优于 baseline 比例 | speedup 样本数 |"
    )
    lines.append("| --- | ---: | ---: | ---: | ---: |")
    for s in summaries:
        better = sum(1 for v in s.speedups if v > 1.0)
        lines.append(
            "| "
            + " | ".join(
                [
                    s.model,
                    _fmt_ratio(s.succeeded, s.expected_queries),
                    _fmt_float(s.median_speedup),
                    _fmt_ratio(better, len(s.speedups)),
                    str(len(s.speedups)),
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("指标口径：")
    lines.append("")
    lines.append("- success：compare JSON 中 `success == true` 的 query 数 / workload query 数")
    lines.append("- speedup：`baseline_median_execution_time_ms / median_execution_time_ms`（仅统计有数值的条目）")
    lines.append("- median speedup：每模型所有 speedup 样本的中位数")
    lines.append("- 优于 baseline 比例：speedup > 1.0 的占比（分母为 speedup 样本数）")
    lines.append("")
    lines.append("## 2. 实验输入与运行参数")
    lines.append("")
    lines.append(f"- baseline: `{baseline_path.as_posix()}`")
    lines.append(f"- layer3 compare: `{compare_path.as_posix()}`")
    lines.append(f"- generated_at: `{generated_at}`")
    lines.append(f"- workload_dir: `{workload_dir}`")
    lines.append(f"- queries: `{len(expected_qids)}` ({', '.join(expected_qids)})")
    lines.append(f"- baseline repeat: `{baseline_repeat}`")
    lines.append(f"- compare repeat: `{compare_repeat}`")
    lines.append(f"- statement_timeout_ms: `{statement_timeout_ms}`")
    lines.append(f"- dsn: `{dsn}`")
    lines.append("")
    lines.append("## 3. 可复现实验说明（从零开始）")
    lines.append("")
    lines.append("### 3.1 安装与启动 PostgreSQL（macOS Homebrew）")
    lines.append("")
    lines.append("按文档操作：`docs/postgres_macos_homebrew.md`。最小目标是能用 DSN 连接：")
    lines.append("")
    lines.append("```bash")
    lines.append('psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -c "SELECT 1;"')
    lines.append("```")
    lines.append("")
    lines.append("### 3.2 导入 TPC-DS SF=1 数据（schema + load + validate）")
    lines.append("")
    lines.append("准备数据文件：把 TPC-DS SF=1 的 `.dat` 文件放到：`datasets/tpcds/sf1/*.dat`。")
    lines.append("")
    lines.append("在仓库根目录执行：")
    lines.append("")
    lines.append("```bash")
    lines.append('psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/schema.sql')
    lines.append('psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/load_sf1.sql')
    lines.append('psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/validate_sf1.sql')
    lines.append("```")
    lines.append("")
    lines.append("### 3.3 跑 baseline（q1~q10）")
    lines.append("")
    lines.append("```bash")
    lines.append("python3 benchmark/postgres/baseline_benchmark.py \\")
    lines.append('  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \\')
    lines.append("  --repeat 3 \\")
    lines.append("  --output-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \\")
    lines.append("  --output-csv benchmark/results/postgres_tpcds_sf1_queries10_baseline.csv")
    lines.append("```")
    lines.append("")
    lines.append("### 3.4 跑 layer3 对比（3 模型）")
    lines.append("")
    lines.append("先配置 LLM key：")
    lines.append("")
    lines.append("```bash")
    lines.append('export ARK_API_KEY="your_api_key_here"')
    lines.append("```")
    lines.append("")
    lines.append("执行：")
    lines.append("")
    lines.append("```bash")
    lines.append("python3 benchmark/postgres/layer3_benchmark.py \\")
    lines.append('  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \\')
    lines.append("  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \\")
    lines.append("  --repeat 3 \\")
    lines.append("  --output-json benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json \\")
    lines.append("  --output-csv benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.csv \\")
    lines.append("  --artifacts-dir benchmark/results/artifacts_layer3_sf1_q10")
    lines.append("```")
    lines.append("")
    lines.append("### 3.5 生成汇总报告（本文件）")
    lines.append("")
    lines.append("```bash")
    lines.append("python3 benchmark/postgres/generate_report.py \\")
    lines.append("  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \\")
    lines.append("  --compare-json benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json \\")
    lines.append("  --output-md benchmark/postgres/report.md")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Markdown report from baseline JSON and layer3 compare JSON."
    )
    parser.add_argument(
        "--baseline-json",
        default="benchmark/results/postgres_tpcds_sf1_queries10_baseline.json",
        help="Baseline JSON generated by benchmark/postgres/baseline_benchmark.py",
    )
    parser.add_argument(
        "--compare-json",
        default="benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json",
        help="Compare JSON generated by benchmark/postgres/layer3_benchmark.py",
    )
    parser.add_argument(
        "--output-md",
        default="benchmark/postgres/report.md",
        help="Output Markdown path.",
    )
    args = parser.parse_args()

    baseline_path = Path(args.baseline_json)
    compare_path = Path(args.compare_json)
    output_path = Path(args.output_md)

    if not baseline_path.exists():
        raise SystemExit(f"baseline JSON not found: {baseline_path}")
    if not compare_path.exists():
        raise SystemExit(f"compare JSON not found: {compare_path}")

    baseline_payload = _read_json(baseline_path)
    compare_payload = _read_json(compare_path)

    baseline_medians = _parse_baseline_medians(baseline_payload)
    summaries, expected_qids = _summarize_by_model(baseline_medians, compare_payload)
    md = _build_markdown(
        baseline_path=baseline_path,
        compare_path=compare_path,
        baseline_payload=baseline_payload,
        compare_payload=compare_payload,
        summaries=summaries,
        expected_qids=expected_qids,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(md + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
