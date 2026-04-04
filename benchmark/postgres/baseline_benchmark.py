from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Optional, Tuple

from psycopg import connect


@dataclass(frozen=True)
class BufferStats:
    """Aggregated buffer statistics from EXPLAIN (ANALYZE, BUFFERS)."""
    shared_hit_blocks: int = 0
    shared_read_blocks: int = 0
    shared_dirtied_blocks: int = 0
    shared_written_blocks: int = 0
    temp_read_blocks: int = 0
    temp_written_blocks: int = 0


@dataclass(frozen=True)
class QueryBaselineResult:
    query_id: str
    query_path: str
    executed: bool
    success: bool
    run_execution_time_ms: list[float]
    median_execution_time_ms: Optional[float]
    run_planning_time_ms: list[float]
    median_planning_time_ms: Optional[float]
    error_message: Optional[str]
    # --- NEW: memory metrics ---
    buffer_stats: Optional[dict[str, int]] = None
    memory_score: Optional[float] = None


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
    sql = path.read_text(encoding="utf-8")
    sql = sql.strip()
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


def _aggregate_buffers(node: dict) -> BufferStats:
    """Recursively walk the plan tree and sum up buffer counters."""
    shared_hit = node.get("Shared Hit Blocks", 0)
    shared_read = node.get("Shared Read Blocks", 0)
    shared_dirtied = node.get("Shared Dirtied Blocks", 0)
    shared_written = node.get("Shared Written Blocks", 0)
    temp_read = node.get("Temp Read Blocks", 0)
    temp_written = node.get("Temp Written Blocks", 0)

    for child in node.get("Plans", []):
        child_buf = _aggregate_buffers(child)
        shared_hit += child_buf.shared_hit_blocks
        shared_read += child_buf.shared_read_blocks
        shared_dirtied += child_buf.shared_dirtied_blocks
        shared_written += child_buf.shared_written_blocks
        temp_read += child_buf.temp_read_blocks
        temp_written += child_buf.temp_written_blocks

    return BufferStats(
        shared_hit_blocks=shared_hit,
        shared_read_blocks=shared_read,
        shared_dirtied_blocks=shared_dirtied,
        shared_written_blocks=shared_written,
        temp_read_blocks=temp_read,
        temp_written_blocks=temp_written,
    )


def _compute_memory_score(buf: BufferStats) -> float:
    """Compute memory efficiency score (0~1, higher is better)."""
    total_shared = buf.shared_hit_blocks + buf.shared_read_blocks
    total_temp = buf.temp_read_blocks + buf.temp_written_blocks

    if total_shared == 0 and total_temp == 0:
        return 1.0

    hit_ratio = buf.shared_hit_blocks / total_shared if total_shared > 0 else 0.0
    total_all = total_shared + total_temp
    temp_ratio = total_temp / total_all if total_all > 0 else 0.0

    return round(0.7 * hit_ratio + 0.3 * (1.0 - temp_ratio), 4)


def _extract_explain_data(explain_json: Any) -> Tuple[float, Optional[float], BufferStats]:
    """Extract timing and buffer stats from EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)."""
    explain_json = _to_json_obj(explain_json)
    if not isinstance(explain_json, list) or not explain_json:
        raise ValueError("Unexpected EXPLAIN JSON: expected a non-empty list")
    top = explain_json[0]
    if not isinstance(top, dict) or "Execution Time" not in top:
        raise ValueError("Unexpected EXPLAIN JSON: missing 'Execution Time'")
    execution_ms = float(top["Execution Time"])
    planning_ms = float(top["Planning Time"]) if "Planning Time" in top else None
    plan = top.get("Plan", {})
    buffers = _aggregate_buffers(plan)
    return execution_ms, planning_ms, buffers


def _benchmark_one_query(
    dsn: str,
    sql: str,
    repeats: int,
    statement_timeout_ms: Optional[int],
) -> Tuple[list[float], list[float], Optional[BufferStats], Optional[str]]:
    run_execution_ms: list[float] = []
    run_planning_ms: list[float] = []
    last_buffer_stats: Optional[BufferStats] = None

    try:
        with connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                if statement_timeout_ms is not None:
                    cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)};")

                # KEY CHANGE: added BUFFERS
                explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
                for _ in range(repeats):
                    cur.execute(explain_sql)
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("EXPLAIN returned no rows")
                    execution_ms, planning_ms, buffers = _extract_explain_data(row[0])
                    run_execution_ms.append(execution_ms)
                    if planning_ms is not None:
                        run_planning_ms.append(planning_ms)
                    last_buffer_stats = buffers
        return run_execution_ms, run_planning_ms, last_buffer_stats, None
    except Exception as exc:
        return run_execution_ms, run_planning_ms, last_buffer_stats, str(exc)


def _write_json(path: Path, payload: Any) -> None:
    _ensure_parent_dir(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, results: list[QueryBaselineResult]) -> None:
    _ensure_parent_dir(path)
    max_runs = max((len(r.run_execution_time_ms) for r in results), default=0)
    fieldnames = [
        "query_id",
        "query_path",
        "executed",
        "success",
        "median_execution_time_ms",
        "median_planning_time_ms",
        "memory_score",
        "error_message",
        *[f"run_{i}_execution_time_ms" for i in range(1, max_runs + 1)],
        *[f"run_{i}_planning_time_ms" for i in range(1, max_runs + 1)],
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row: dict[str, Any] = {
                "query_id": r.query_id,
                "query_path": r.query_path,
                "executed": r.executed,
                "success": r.success,
                "median_execution_time_ms": r.median_execution_time_ms,
                "median_planning_time_ms": r.median_planning_time_ms,
                "memory_score": r.memory_score,
                "error_message": r.error_message,
            }
            for i in range(max_runs):
                row[f"run_{i+1}_execution_time_ms"] = r.run_execution_time_ms[i] if i < len(r.run_execution_time_ms) else None
                row[f"run_{i+1}_planning_time_ms"] = r.run_planning_time_ms[i] if i < len(r.run_planning_time_ms) else None
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run TPC-DS q1~q10 baseline on PostgreSQL and save results.")
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
    parser.add_argument("--repeat", type=int, default=3, help="Repeat count per query (median is reported).")
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=None,
        help="Optional statement_timeout in ms for each run.",
    )
    parser.add_argument(
        "--output-json",
        default="benchmark/results/postgres_tpcds_sf1_queries10_baseline.json",
        help="Output JSON path (inside repo).",
    )
    parser.add_argument(
        "--output-csv",
        default="benchmark/results/postgres_tpcds_sf1_queries10_baseline.csv",
        help="Output CSV path (inside repo).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only validate workload files and exit.")
    args = parser.parse_args()

    workload_dir = Path(args.workload_dir)
    if not workload_dir.exists() or not workload_dir.is_dir():
        raise SystemExit(f"Workload dir not found: {workload_dir}")

    sql_files = sorted(workload_dir.glob("q*.sql"), key=_parse_query_id)
    if not sql_files:
        raise SystemExit(f"No q*.sql files found under: {workload_dir}")

    selected_ids = _parse_query_ids(args.query_ids)
    sql_files = [p for p in sql_files if _parse_query_id(p) in selected_ids]
    if not sql_files:
        raise SystemExit("No matching q*.sql after applying --query-ids")

    output_json = Path(args.output_json)
    output_csv = Path(args.output_csv)

    if args.repeat <= 0:
        raise SystemExit("--repeat must be >= 1")

    results: list[QueryBaselineResult] = []
    for sql_path in sql_files:
        query_id = sql_path.stem
        sql = _load_sql(sql_path)
        if args.dry_run:
            results.append(
                QueryBaselineResult(
                    query_id=query_id,
                    query_path=str(sql_path),
                    executed=False,
                    success=False,
                    run_execution_time_ms=[],
                    median_execution_time_ms=None,
                    run_planning_time_ms=[],
                    median_planning_time_ms=None,
                    error_message="dry_run",
                    buffer_stats=None,
                    memory_score=None,
                )
            )
            continue

        run_execution_ms, run_planning_ms, last_buffer_stats, err = _benchmark_one_query(
            dsn=args.dsn,
            sql=sql,
            repeats=args.repeat,
            statement_timeout_ms=args.statement_timeout_ms,
        )
        success = err is None and len(run_execution_ms) == args.repeat

        buf_dict: Optional[dict[str, int]] = None
        mem_score: Optional[float] = None
        if last_buffer_stats is not None:
            buf_dict = asdict(last_buffer_stats)
            mem_score = _compute_memory_score(last_buffer_stats)

        results.append(
            QueryBaselineResult(
                query_id=query_id,
                query_path=str(sql_path),
                executed=True,
                success=success,
                run_execution_time_ms=run_execution_ms,
                median_execution_time_ms=median(run_execution_ms) if run_execution_ms else None,
                run_planning_time_ms=run_planning_ms,
                median_planning_time_ms=median(run_planning_ms) if run_planning_ms else None,
                error_message=err,
                buffer_stats=buf_dict,
                memory_score=mem_score,
            )
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "postgresql",
        "workload_dir": str(workload_dir),
        "repeat": int(args.repeat),
        "dry_run": bool(args.dry_run),
        "results": [asdict(r) for r in results],
    }
    _write_json(output_json, payload)
    _write_csv(output_csv, results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
