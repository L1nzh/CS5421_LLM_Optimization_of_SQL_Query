from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import median
from typing import Any, Optional


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _median(values: list[float]) -> Optional[float]:
    return median(values) if values else None


def _group_by_variant(results: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in results:
        exp = r.get("experiment")
        vid = r.get("variant_id")
        if not isinstance(exp, str) or not isinstance(vid, str):
            continue
        out.setdefault((exp, vid), []).append(r)
    return out


def _variant_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    success_rows = [r for r in rows if r.get("success") is True and isinstance(r.get("speedup"), (int, float))]
    speedups = [float(r["speedup"]) for r in success_rows]
    better = [s for s in speedups if s > 1.0]
    return {
        "total": total,
        "success": sum(1 for r in rows if r.get("success") is True),
        "speedup_n": len(speedups),
        "median_speedup": _median(speedups),
        "better_ratio": f"{len(better)}/{len(speedups)}" if speedups else "0/0",
    }


def _pick_prompt_example(rows: list[dict[str, Any]], query_id: str = "q1") -> Optional[str]:
    for r in rows:
        if r.get("query_id") == query_id and isinstance(r.get("prompt"), str):
            return r["prompt"]
    for r in rows:
        if isinstance(r.get("prompt"), str):
            return r["prompt"]
    return None


def _pick_failures(results: list[dict[str, Any]], limit: int = 12) -> list[tuple[str, str, str]]:
    fails = []
    for r in results:
        if r.get("success") is True:
            continue
        qid = r.get("query_id")
        exp = r.get("experiment")
        vid = r.get("variant_id")
        msg = r.get("error_message")
        if not (isinstance(qid, str) and isinstance(exp, str) and isinstance(vid, str)):
            continue
        if not isinstance(msg, str):
            msg = ""
        fails.append((qid, f"{exp}/{vid}", msg.replace("\n", " ")[:200]))
    return fails[:limit]


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    line1 = "| " + " | ".join(headers) + " |"
    line2 = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows)
    return "\n".join([line1, line2, body]).strip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate markdown report for prompt/reasoning ablation experiments.")
    parser.add_argument("--ablations-json", default="benchmark/results/postgres_tpcds_sf1_q9_pro_ablations.json")
    parser.add_argument("--output-md", default="benchmark/postgres/prompt_reasoning_report.md")
    args = parser.parse_args()

    ablations_path = Path(args.ablations_json)
    if not ablations_path.exists():
        raise SystemExit(f"Ablations JSON not found: {ablations_path}")

    payload = _load_json(ablations_path)
    results = payload.get("results", [])
    if not isinstance(results, list):
        raise SystemExit("Invalid ablations JSON: missing results list")

    groups = _group_by_variant([r for r in results if isinstance(r, dict)])

    prompt_rows: list[list[str]] = []
    reasoning_rows: list[list[str]] = []
    prompt_examples: list[tuple[str, str]] = []
    reasoning_examples: list[tuple[str, str]] = []

    for (exp, vid), rows in sorted(groups.items()):
        s = _variant_summary(rows)
        row = [
            vid,
            f"{s['success']}/{s['total']}",
            f"{s['median_speedup']:.3f}" if isinstance(s["median_speedup"], float) else "NA",
            s["better_ratio"],
            str(s["speedup_n"]),
        ]
        if exp == "prompt":
            prompt_rows.append(row)
            ex = _pick_prompt_example(rows, "q1")
            if ex:
                prompt_examples.append((vid, ex))
        else:
            reasoning_rows.append(row)
            ex = _pick_prompt_example(rows, "q1")
            if ex:
                reasoning_examples.append((vid, ex))

    failures = _pick_failures([r for r in results if isinstance(r, dict)], limit=20)

    lines: list[str] = []
    lines.append("# Prompt & Reasoning Engineering 实验报告（Pro 模型）")
    lines.append("")
    lines.append("## 1. 实验设置")
    lines.append("")
    lines.append(f"- engine: `{payload.get('engine')}`")
    lines.append(f"- dsn: `{payload.get('dsn')}`")
    lines.append(f"- model: `{payload.get('model')}`")
    lines.append(f"- workload_dir: `{payload.get('workload_dir')}`")
    lines.append(f"- queries: `{len(payload.get('query_ids', []))}` ({', '.join(f'q{i}' for i in payload.get('query_ids', []))})")
    lines.append(f"- repeat: `{payload.get('repeat')}`")
    lines.append(f"- statement_timeout_ms: `{payload.get('statement_timeout_ms')}`")
    lines.append(f"- generated_at: `{payload.get('generated_at')}`")
    lines.append("")
    lines.append("指标口径：")
    lines.append("")
    lines.append("- 使用 `EXPLAIN (ANALYZE, FORMAT JSON)` 的 `Execution Time` 作为执行时间（ms）")
    lines.append("- speedup = baseline_median_execution_time_ms / variant_median_execution_time_ms（仅统计 success 的样本）")
    lines.append("")

    if prompt_rows:
        lines.append("## 2. Prompt Engineering 结果汇总")
        lines.append("")
        lines.append(
            _md_table(
                ["variant", "success", "median speedup", "优于 baseline 比例", "speedup 样本数"],
                prompt_rows,
            ).rstrip()
        )
        lines.append("")

    if reasoning_rows:
        lines.append("## 3. Reasoning Engineering 结果汇总")
        lines.append("")
        lines.append(
            _md_table(
                ["variant", "success", "median speedup", "优于 baseline 比例", "speedup 样本数"],
                reasoning_rows,
            ).rstrip()
        )
        lines.append("")

    if prompt_examples:
        lines.append("## 4. Prompt 具体内容（示例：q1）")
        lines.append("")
        for vid, ex in prompt_examples:
            lines.append(f"### {vid}")
            lines.append("")
            lines.append("```text")
            lines.append(ex.rstrip())
            lines.append("```")
            lines.append("")

    if reasoning_examples:
        lines.append("## 5. Reasoning Prompt 具体内容（示例：q1）")
        lines.append("")
        for vid, ex in reasoning_examples:
            lines.append(f"### {vid}")
            lines.append("")
            lines.append("```text")
            lines.append(ex.rstrip())
            lines.append("```")
            lines.append("")

    if failures:
        lines.append("## 6. 失败样例（截断）")
        lines.append("")
        lines.append(_md_table(["query", "variant", "error (truncated)"], [[a, b, c] for a, b, c in failures]).rstrip())
        lines.append("")

    out_path = Path(args.output_md)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

