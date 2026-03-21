from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Optional, Tuple

from psycopg import connect

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from layer3 import DOUBAO_MODELS, generate_text


@dataclass(frozen=True)
class QueryRunResult:
    query_id: str
    query_path: str
    model: str
    prompt: str
    raw_output_text: str
    rewritten_sql: Optional[str]
    executed: bool
    success: bool
    run_execution_time_ms: list[float]
    median_execution_time_ms: Optional[float]
    run_planning_time_ms: list[float]
    median_planning_time_ms: Optional[float]
    baseline_median_execution_time_ms: Optional[float]
    speedup: Optional[float]
    error_message: Optional[str]
    rewritten_sql_path: Optional[str]


def _parse_query_id(path: Path) -> int:
    stem = path.stem.lower()
    if not stem.startswith("q"):
        raise ValueError(f"Unexpected query filename: {path.name}")
    return int(stem[1:])


def _parse_query_ids(value: str) -> set[int]:
    parts = [v.strip() for v in value.split(",") if v.strip()]
    try:
        return {int(v) for v in parts}
    except ValueError as exc:
        raise SystemExit(f"Invalid --query-ids: {value}") from exc


def _load_sql(path: Path) -> str:
    sql = path.read_text(encoding="utf-8").strip()
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()
    if not sql:
        raise ValueError(f"Empty SQL file: {path}")
    return sql


def _ensure_parent_dir(file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)


def _to_json_obj(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, (bytes, bytearray)):
        return json.loads(value.decode("utf-8"))
    return value


def _extract_explain_times_ms(explain_json: Any) -> Tuple[float, Optional[float]]:
    explain_json = _to_json_obj(explain_json)
    if not isinstance(explain_json, list) or not explain_json:
        raise ValueError("Unexpected EXPLAIN JSON: expected a non-empty list")
    top = explain_json[0]
    if not isinstance(top, dict) or "Execution Time" not in top:
        raise ValueError("Unexpected EXPLAIN JSON: missing 'Execution Time'")
    execution_ms = float(top["Execution Time"])
    planning_ms = float(top["Planning Time"]) if "Planning Time" in top else None
    return execution_ms, planning_ms


def _benchmark_one_query(
    dsn: str,
    sql: str,
    repeats: int,
    statement_timeout_ms: Optional[int],
) -> Tuple[list[float], list[float], Optional[str]]:
    run_execution_ms: list[float] = []
    run_planning_ms: list[float] = []

    try:
        with connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                if statement_timeout_ms is not None:
                    cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)};")

                explain_sql = f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}"
                for _ in range(repeats):
                    cur.execute(explain_sql)
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("EXPLAIN returned no rows")
                    execution_ms, planning_ms = _extract_explain_times_ms(row[0])
                    run_execution_ms.append(execution_ms)
                    if planning_ms is not None:
                        run_planning_ms.append(planning_ms)
        return run_execution_ms, run_planning_ms, None
    except Exception as exc:
        return run_execution_ms, run_planning_ms, str(exc)


def _build_prompt(sql: str) -> str:
    return "\n".join(
        [
            "You are a PostgreSQL SQL optimizer.",
            "Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.",
            "Constraints:",
            "- Return only ONE SQL query.",
            "- Do NOT output explanations or markdown.",
            "- Preserve the result set exactly (columns, ordering, LIMIT).",
            "- Use only standard PostgreSQL syntax (no hints).",
            "",
            "SQL:",
            sql,
        ]
    )


_FENCE_RE = re.compile(r"```(?:sql)?\s*([\s\S]*?)\s*```", re.IGNORECASE)
_EXPLAIN_PREFIX_RE = re.compile(r"^\s*EXPLAIN\s*(?:\([^)]*\))?\s*", re.IGNORECASE)


def _extract_first_statement(sql: str) -> str:
    parts = sql.split(";")
    first = parts[0].strip()
    while first.endswith(";"):
        first = first[:-1].rstrip()
    return first


def _extract_sql_from_text(text: str) -> str:
    t = text.strip()
    m = _FENCE_RE.search(t)
    if m:
        t = m.group(1).strip()

    t = t.strip().strip("`").strip()
    t = _EXPLAIN_PREFIX_RE.sub("", t).strip()

    lowered = t.lower()
    starts: list[Tuple[int, str]] = []
    for token in ("with", "select"):
        idx = lowered.find(token)
        if idx != -1:
            starts.append((idx, token))
    if starts:
        start_idx = min(starts)[0]
        t = t[start_idx:].strip()

    t = _extract_first_statement(t)
    if not t:
        raise ValueError("Empty SQL extracted from model output")
    return t


def _model_alias(model: str) -> str:
    m = model.lower()
    if "pro" in m:
        return "pro"
    if "lite" in m:
        return "lite"
    if "mini" in m:
        return "mini"
    return re.sub(r"[^a-z0-9]+", "_", m).strip("_") or "model"


def _load_baseline_medians(path: Path) -> dict[str, Optional[float]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = payload.get("results", [])
    if not isinstance(results, list):
        raise ValueError("Unexpected baseline JSON: missing 'results' list")
    medians: dict[str, Optional[float]] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        qid = r.get("query_id")
        if not isinstance(qid, str):
            continue
        med = r.get("median_execution_time_ms")
        medians[qid] = float(med) if isinstance(med, (int, float)) else None
    return medians


def _write_json(path: Path, payload: Any) -> None:
    _ensure_parent_dir(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, results: list[QueryRunResult]) -> None:
    _ensure_parent_dir(path)
    fieldnames = [
        "query_id",
        "query_path",
        "model",
        "baseline_median_execution_time_ms",
        "median_execution_time_ms",
        "speedup",
        "executed",
        "success",
        "error_message",
        "rewritten_sql_path",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "query_id": r.query_id,
                    "query_path": r.query_path,
                    "model": r.model,
                    "baseline_median_execution_time_ms": r.baseline_median_execution_time_ms,
                    "median_execution_time_ms": r.median_execution_time_ms,
                    "speedup": r.speedup,
                    "executed": r.executed,
                    "success": r.success,
                    "error_message": r.error_message,
                    "rewritten_sql_path": r.rewritten_sql_path,
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate optimized SQL via Layer 3 (pro/lite/mini), run EXPLAIN ANALYZE (FORMAT JSON) on PostgreSQL, and compare with baseline."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("PG_DSN", "postgresql://bench:bench@localhost:5432/tpcds_sf1"),
        help="PostgreSQL DSN. Defaults to $PG_DSN or postgresql://bench:bench@localhost:5432/tpcds_sf1",
    )
    parser.add_argument(
        "--workload-dir",
        default="workloads/tpcds/sf1/queries_10",
        help="Directory containing q1.sql~q10.sql",
    )
    parser.add_argument(
        "--query-ids",
        default="1,2,3,5,6,7,8,9,10,12",
        help="Comma-separated query ids to run (default excludes q4 due to very slow runtime on local SF=1).",
    )
    parser.add_argument(
        "--baseline-json",
        default="benchmark/results/postgres_tpcds_sf1_queries10_baseline.json",
        help="Baseline JSON generated by baseline_benchmark.py",
    )
    parser.add_argument("--repeat", type=int, default=3, help="Repeat count per query per model (median is reported).")
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=None,
        help="Optional statement_timeout in ms for each run.",
    )
    parser.add_argument(
        "--output-json",
        default="benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json",
        help="Output JSON path (inside repo).",
    )
    parser.add_argument(
        "--output-csv",
        default="benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.csv",
        help="Output CSV path (inside repo).",
    )
    parser.add_argument(
        "--artifacts-dir",
        default=None,
        help="Optional directory to save model raw outputs and cleaned SQL for reproducibility.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only validate inputs and exit (no LLM, no DB).")
    parser.add_argument("--skip-llm", action="store_true", help="Skip LLM and benchmark the original SQL as rewrite.")
    args = parser.parse_args()

    workload_dir = Path(args.workload_dir)
    if not workload_dir.exists() or not workload_dir.is_dir():
        raise SystemExit(f"Workload dir not found: {workload_dir}")

    baseline_json = Path(args.baseline_json)
    if not baseline_json.exists():
        raise SystemExit(f"Baseline JSON not found: {baseline_json}")

    sql_files = sorted(workload_dir.glob("q*.sql"), key=_parse_query_id)
    if not sql_files:
        raise SystemExit(f"No q*.sql files found under: {workload_dir}")

    selected_ids = _parse_query_ids(args.query_ids)
    sql_files = [p for p in sql_files if _parse_query_id(p) in selected_ids]
    if not sql_files:
        raise SystemExit("No matching q*.sql after applying --query-ids")

    if args.repeat <= 0:
        raise SystemExit("--repeat must be >= 1")

    baseline_medians = _load_baseline_medians(baseline_json)

    artifacts_dir: Optional[Path] = Path(args.artifacts_dir) if args.artifacts_dir else None
    if artifacts_dir is not None:
        _ensure_parent_dir(artifacts_dir / "placeholder.txt")

    results: list[QueryRunResult] = []

    for sql_path in sql_files:
        query_id = sql_path.stem
        original_sql = _load_sql(sql_path)
        prompt = _build_prompt(original_sql)
        baseline_median_ms = baseline_medians.get(query_id)

        for model in DOUBAO_MODELS:
            raw_output_text = ""
            rewritten_sql: Optional[str] = None
            rewritten_sql_path: Optional[str] = None
            executed = False
            success = False
            run_execution_ms: list[float] = []
            run_planning_ms: list[float] = []
            err: Optional[str] = None

            try:
                if args.dry_run:
                    raise RuntimeError("dry_run")

                if args.skip_llm:
                    raw_output_text = original_sql
                    rewritten_sql = original_sql
                else:
                    raw_output_text = generate_text(prompt, model)
                    rewritten_sql = _extract_sql_from_text(raw_output_text)

                if artifacts_dir is not None:
                    model_tag = _model_alias(model)
                    raw_path = artifacts_dir / f"{query_id}_{model_tag}.raw.txt"
                    sql_out_path = artifacts_dir / f"{query_id}_{model_tag}.sql"
                    raw_path.write_text(raw_output_text, encoding="utf-8")
                    sql_out_path.write_text((rewritten_sql or "") + "\n", encoding="utf-8")
                    rewritten_sql_path = str(sql_out_path)

                run_execution_ms, run_planning_ms, err = _benchmark_one_query(
                    dsn=args.dsn,
                    sql=rewritten_sql,
                    repeats=args.repeat,
                    statement_timeout_ms=args.statement_timeout_ms,
                )
                executed = True
                success = err is None and len(run_execution_ms) == args.repeat
            except Exception as exc:
                if str(exc) == "dry_run":
                    executed = False
                    success = False
                    err = "dry_run"
                else:
                    executed = executed or False
                    success = False
                    err = str(exc)

            median_exec_ms = median(run_execution_ms) if run_execution_ms else None
            median_plan_ms = median(run_planning_ms) if run_planning_ms else None
            speedup = (
                (baseline_median_ms / median_exec_ms)
                if baseline_median_ms is not None and median_exec_ms is not None and median_exec_ms > 0
                else None
            )

            results.append(
                QueryRunResult(
                    query_id=query_id,
                    query_path=str(sql_path),
                    model=model,
                    prompt=prompt,
                    raw_output_text=raw_output_text,
                    rewritten_sql=rewritten_sql,
                    executed=executed,
                    success=success,
                    run_execution_time_ms=run_execution_ms,
                    median_execution_time_ms=median_exec_ms,
                    run_planning_time_ms=run_planning_ms,
                    median_planning_time_ms=median_plan_ms,
                    baseline_median_execution_time_ms=baseline_median_ms,
                    speedup=speedup,
                    error_message=err,
                    rewritten_sql_path=rewritten_sql_path,
                )
            )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "postgresql",
        "dsn": args.dsn,
        "workload_dir": str(workload_dir),
        "baseline_json": str(baseline_json),
        "repeat": int(args.repeat),
        "statement_timeout_ms": args.statement_timeout_ms,
        "models": list(DOUBAO_MODELS),
        "dry_run": bool(args.dry_run),
        "skip_llm": bool(args.skip_llm),
        "results": [asdict(r) for r in results],
    }
    _write_json(Path(args.output_json), payload)
    _write_csv(Path(args.output_csv), results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
