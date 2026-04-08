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
from typing import Any, Callable, Optional, Tuple

from psycopg import connect

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from layer3 import generate_text

DEFAULT_MODEL = "doubao-seed-2-0-pro-260215"
DEFAULT_QUERY_IDS = "1,2,3,6,7,8,9,10,12"
DEFAULT_WORKLOAD_DIR = "workloads/tpcds/sf1/queries_10"
DEFAULT_BASELINE_JSON = "benchmark/results/postgres_tpcds_sf1_queries10_baseline.json"


@dataclass(frozen=True)
class RunRecord:
    query_id: str
    query_path: str
    experiment: str
    variant_id: str
    model: str
    prompt: str
    stage1_prompt: Optional[str]
    stage1_raw_output_text: Optional[str]
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
    artifact_dir: Optional[str]


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


_FENCE_RE = re.compile(r"```(?:sql)?\s*([\s\S]*?)\s*```", re.IGNORECASE)
_EXPLAIN_PREFIX_RE = re.compile(r"^\s*EXPLAIN\s*(?:\([^)]*\))?\s*", re.IGNORECASE)
_SQL_TAG_RE = re.compile(r"<SQL>\s*([\s\S]*?)\s*</SQL>", re.IGNORECASE)


def _extract_first_statement(sql: str) -> str:
    parts = sql.split(";")
    first = parts[0].strip()
    while first.endswith(";"):
        first = first[:-1].rstrip()
    return first


def _extract_sql_from_text(text: str) -> str:
    t = text.strip()
    m = _SQL_TAG_RE.search(t)
    if m:
        t = m.group(1).strip()

    m = _FENCE_RE.search(t)
    if m:
        t = m.group(1).strip()

    t = t.strip().strip("`").strip()
    t = _EXPLAIN_PREFIX_RE.sub("", t).strip()

    lowered = t.lower()
    starts: list[int] = []
    for token in ("with", "select"):
        idx = lowered.find(token)
        if idx != -1:
            starts.append(idx)
    if starts:
        t = t[min(starts) :].strip()

    t = _extract_first_statement(t)
    if not t:
        raise ValueError("Empty SQL extracted from model output")
    return t


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


def _read_schema_create_table_map(schema_path: Path) -> dict[str, str]:
    text = schema_path.read_text(encoding="utf-8")
    blocks = re.findall(r"CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\);", text, flags=re.I | re.S)
    mapping: dict[str, str] = {}
    for name, body in blocks:
        mapping[name.lower()] = f"CREATE TABLE {name} ({body});"
    return mapping


def _extract_tables_from_sql(sql: str, known_tables: set[str]) -> list[str]:
    lowered = re.sub(r"\\s+", " ", sql).strip().lower()
    tables: list[str] = []
    for t in known_tables:
        if re.search(rf"\b{re.escape(t)}\b", lowered):
            tables.append(t)
    return sorted(set(tables))


def _check_db_connection(dsn: str) -> None:
    with connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")


def _fetch_table_row_estimates(dsn: str, tables: list[str]) -> dict[str, int]:
    if not tables:
        return {}
    sql = """
SELECT relname, COALESCE(reltuples, 0)::bigint AS est_rows
FROM pg_class
WHERE relkind = 'r' AND relname = ANY(%s);
"""
    with connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, (tables,))
            rows = cur.fetchall()
    out: dict[str, int] = {}
    for relname, est_rows in rows:
        if isinstance(relname, str) and isinstance(est_rows, int):
            out[relname] = est_rows
    return out


def _render_schema_min(tables: list[str], create_table_map: dict[str, str]) -> str:
    lines: list[str] = []
    for t in tables:
        ddl = create_table_map.get(t.lower())
        if not ddl:
            continue
        col_lines = []
        for raw in ddl.splitlines():
            raw = raw.strip().rstrip(",")
            if not raw or raw.lower().startswith("create table"):
                continue
            if raw.startswith(")"):
                continue
            col = raw.split()[0].strip().strip('"')
            if col:
                col_lines.append(col)
        if col_lines:
            cols = ", ".join(col_lines[:60])
            suffix = "..." if len(col_lines) > 60 else ""
            lines.append(f"- {t}({cols}{suffix})")
        else:
            lines.append(f"- {t}")
    return "\n".join(lines)


def _render_schema_full_ddl(tables: list[str], create_table_map: dict[str, str]) -> dict[str, dict]:
    """
    Parse CREATE TABLE DDL into structured column + constraint info.
    Returns a dict keyed by table name:
      {
        "columns": [{"name": str, "type": str, "nullable": bool}],
        "primary_key": [col, ...],
        "unique": [[col, ...], ...],
        "foreign_keys": [{"cols": [...], "ref_table": str, "ref_cols": [...]}],
      }
    """
    result = {}
    fk_re = re.compile(
        r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]+)\)",
        re.IGNORECASE,
    )
    pk_re = re.compile(r"PRIMARY\s+KEY\s*\(([^)]+)\)", re.IGNORECASE)
    uq_re = re.compile(r"UNIQUE\s*\(([^)]+)\)", re.IGNORECASE)

    for table in tables:
        ddl = create_table_map.get(table.lower())
        if not ddl:
            continue

        columns = []
        primary_key = []
        unique_keys = []
        foreign_keys = []

        body_match = re.search(r"CREATE\s+TABLE\s+\S+\s*\((.*)\)\s*;?$", ddl, re.IGNORECASE | re.DOTALL)
        if not body_match:
            result[table] = {"columns": [], "primary_key": [], "unique": [], "foreign_keys": []}
            continue
        body = body_match.group(1)

        depth = 0
        current = []
        clauses = []
        for ch in body:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                clauses.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            clauses.append("".join(current).strip())

        for clause in clauses:
            clause_stripped = clause.strip()
            upper = clause_stripped.upper()

            if pk_re.search(clause_stripped):
                m = pk_re.search(clause_stripped)
                primary_key = [c.strip().strip('"') for c in m.group(1).split(",")]

            elif fk_re.search(clause_stripped):
                m = fk_re.search(clause_stripped)
                foreign_keys.append({
                    "cols": [c.strip().strip('"') for c in m.group(1).split(",")],
                    "ref_table": m.group(2).strip().lower(),
                    "ref_cols": [c.strip().strip('"') for c in m.group(3).split(",")],
                })

            elif uq_re.search(clause_stripped):
                m = uq_re.search(clause_stripped)
                unique_keys.append([c.strip().strip('"') for c in m.group(1).split(",")])

            elif upper.startswith("PRIMARY") or upper.startswith("CONSTRAINT"):
                pass

            else:
                parts = clause_stripped.split()
                if len(parts) >= 2:
                    col_name = parts[0].strip('"')
                    col_type = parts[1].rstrip(",").upper()
                    nullable = "NOT NULL" not in upper
                    if "PRIMARY KEY" in upper:
                        primary_key = [col_name]
                    columns.append({"name": col_name, "type": col_type, "nullable": nullable})

        result[table] = {
            "columns": columns,
            "primary_key": primary_key,
            "unique": unique_keys,
            "foreign_keys": foreign_keys,
        }
    return result


def _fetch_indexes(dsn: str, tables: list[str]) -> dict[str, list[dict]]:
    """
    Returns index info per table from pg_catalog.
    Each entry: {"name": str, "columns": [str], "unique": bool, "partial": bool}
    """
    if not tables:
        return {}

    sql = """
SELECT
    t.relname                             AS table_name,
    i.relname                             AS index_name,
    ix.indisunique                        AS is_unique,
    ix.indisprimary                       AS is_primary,
    ix.indpred IS NOT NULL                AS is_partial,
    array_agg(a.attname ORDER BY k.pos)  AS columns
FROM pg_class      t
JOIN pg_index      ix ON ix.indrelid  = t.oid
JOIN pg_class      i  ON i.oid        = ix.indexrelid
JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, pos)
     ON true
JOIN pg_attribute  a  ON a.attrelid   = t.oid AND a.attnum = k.attnum
WHERE t.relkind = 'r'
  AND t.relname = ANY(%s)
  AND NOT ix.indisprimary          -- skip PK indexes (already shown via DDL)
GROUP BY t.relname, i.relname, ix.indisunique, ix.indisprimary, ix.indpred
ORDER BY t.relname, i.relname;
"""
    out: dict[str, list[dict]] = {t: [] for t in tables}
    try:
        with connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, (tables,))
                for table_name, index_name, is_unique, is_primary, is_partial, columns in cur.fetchall():
                    out.setdefault(table_name, []).append({
                        "name": index_name,
                        "columns": list(columns),
                        "unique": bool(is_unique),
                        "partial": bool(is_partial),
                    })
    except Exception:
        pass
    return out


def _render_schema_rich(
    tables: list[str],
    dsn: str,
    create_table_map: dict[str, str],
) -> str:
    """
    Render a rich schema block for P3, including:
      - Column names and types
      - Primary keys
      - Foreign keys (with referenced table)
      - Secondary indexes (unique, partial flags)
      - Approximate row counts
    """
    parsed   = _render_schema_full_ddl(tables, create_table_map)
    indexes  = _fetch_indexes(dsn, tables)
    row_est  = _fetch_table_row_estimates(dsn, tables)

    sorted_tables = sorted(tables, key=lambda t: row_est.get(t, 0), reverse=True)

    blocks = []
    for table in sorted_tables:
        info = parsed.get(table, {})
        lines = [f"TABLE {table}  (~{row_est.get(table, 0):,} rows)"]

        for col in info.get("columns", []):
            null_flag = "" if col["nullable"] else " NOT NULL"
            lines.append(f"  {col['name']}  {col['type']}{null_flag}")

        pk = info.get("primary_key", [])
        if pk:
            lines.append(f"  PK: ({', '.join(pk)})")

        for fk in info.get("foreign_keys", []):
            cols     = ", ".join(fk["cols"])
            ref_cols = ", ".join(fk["ref_cols"])
            lines.append(f"  FK: ({cols}) → {fk['ref_table']}({ref_cols})")

        for uq in info.get("unique", []):
            lines.append(f"  UNIQUE: ({', '.join(uq)})")

        for idx in indexes.get(table, []):
            flags = []
            if idx["unique"]:
                flags.append("UNIQUE")
            if idx["partial"]:
                flags.append("PARTIAL")
            flag_str = " [" + ", ".join(flags) + "]" if flags else ""
            lines.append(f"  INDEX {idx['name']}: ({', '.join(idx['columns'])}){flag_str}")

        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)


def _prompt_base(sql: str) -> str:
    return f"""You are a PostgreSQL SQL optimizer.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.

Rules:
- Output ONLY the rewritten SQL inside <SQL>...</SQL> tags.
- Preserve the result set exactly: same columns, same ordering, same LIMIT.
- Do NOT include explanations, markdown, or any text outside the <SQL> tags.
- Use only standard PostgreSQL syntax (no hints).

SQL:
{sql}
"""


def _prompt_engine(sql: str) -> str:
    return f"""You are a PostgreSQL 16 SQL optimizer.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.

You may use any PostgreSQL 16 feature, including:
- CTEs with MATERIALIZED / NOT MATERIALIZED
- LATERAL joins
- Window functions with FILTER
- Partial indexes (reference only; do not CREATE)
- DISTINCT ON

Rules:
- Output ONLY the rewritten SQL inside <SQL>...</SQL> tags.
- Preserve the result set exactly: same columns, same ordering, same LIMIT.
- Do NOT include explanations, markdown, or any text outside the <SQL> tags.
- Use only standard PostgreSQL syntax (no hints).

SQL:
{sql}
"""


def _prompt_engine_header() -> str:
    return f"""You are a PostgreSQL 16 SQL optimizer.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.

You may use any PostgreSQL 16 feature, including:
- CTEs with MATERIALIZED / NOT MATERIALIZED
- LATERAL joins
- Window functions with FILTER
- Partial indexes (reference only; do not CREATE)
- DISTINCT ON

Rules:
- Output ONLY the rewritten SQL inside <SQL>...</SQL> tags.
- Preserve the result set exactly: same columns, same ordering, same LIMIT.
- Do NOT include explanations, markdown, or any text outside the <SQL> tags.
- Use only standard PostgreSQL syntax (no hints, no proprietary keywords).
"""


def _prompt_with_schema(sql: str, schema_block: str, extra: str) -> str:
    return f"""
    {extra}

    Schema (subset):
    {schema_block or "(none)"}

    SQL:
    {sql}
    """


def _prompt_with_schema_rich(sql: str, schema_block: str, extra: str) -> str:
    return f"""
    {extra}

    The schema below includes column types, primary keys (PK), foreign keys (FK), indexes, and approximate row counts. Use this information to make better decisions on query optimization.

    Schema (subset):
    {schema_block or "(none)"}

    SQL:
    {sql}
    """

PROMPT_VARIANTS: dict[str, Callable[[str, str, list[str], str], str]] = {
    "P0_BASE": lambda sql, schema, tables, dsn: _prompt_base(sql),
    "P1_ENGINE": lambda sql, schema, tables, dsn: _prompt_engine(sql),
    "P2_SCHEMA_MIN": lambda sql, schema, tables, dsn: _prompt_with_schema(sql, schema, _prompt_engine_header()),
    "P3_SCHEMA_STATS": lambda sql, schema, tables, dsn: _prompt_with_schema_rich(sql, schema, _prompt_engine_header()),
}


def _build_prompt_variant(
    variant_id: str,
    sql: str,
    dsn: str,
    tables: list[str],
    create_table_map: dict[str, str],
) -> str:
    if variant_id == "P2_SCHEMA_MIN":
        schema_block = _render_schema_min(tables, create_table_map)
        return PROMPT_VARIANTS[variant_id](sql, schema_block, tables, dsn)
    if variant_id == "P3_SCHEMA_STATS":
        schema_block = _render_schema_rich(tables, dsn, create_table_map)
        return PROMPT_VARIANTS[variant_id](sql, schema_block, tables, dsn)
    return PROMPT_VARIANTS[variant_id](sql, "", tables, dsn)


def _build_reasoning_prompt_direct(sql: str, dsn: str, tables: list[str], create_table_map: dict[str, str]) -> str:
    schema_block = _render_schema_min(tables, create_table_map)
    return f"""You are a PostgreSQL 16 SQL optimizer.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.

Rules:
- Output ONLY the rewritten SQL inside <SQL>...</SQL> tags.
- Preserve the result set exactly: same columns, same ordering, same LIMIT.
- Do NOT include explanations, markdown, or any text outside the <SQL> tags.
- Use only standard PostgreSQL syntax (no hints).

Schema (subset):
{schema_block or "(none)"}

SQL:
{sql}
"""


def _build_reasoning_prompt_cot(sql: str, dsn: str, tables: list[str], create_table_map: dict[str, str]) -> str:
    schema_block = _render_schema_min(tables, create_table_map)
    return f"""You are a PostgreSQL 16 SQL optimizer.

Analyze the query using this exact structure:
<ANALYSIS>
1. Join structure: describe join types and estimated cardinalities.
2. Subquery patterns: identify correlated subqueries or repeated scans.
3. CTE usage: note if CTEs are materialized unnecessarily.
4. Predicate placement: identify predicates that could be pushed earlier.
5. Top bottleneck: state the single most expensive operation.
</ANALYSIS>

Then output the optimized SQL:
<SQL>
[rewritten query here]
</SQL>

Rules:
- Preserve result set exactly (columns, ordering, LIMIT).
- Use only standard PostgreSQL 16 syntax (no hints).

Schema (subset):
{schema_block or "(none)"}

SQL:
{sql}
"""


def _build_reasoning_prompt_plan(sql: str, dsn: str, tables: list[str], create_table_map: dict[str, str]) -> str:
    schema_block = _render_schema_min(tables, create_table_map)
    return f"""You are a PostgreSQL 16 SQL optimizer.
Analyze the SQL and produce a numbered optimization plan.
Each step must follow this format:
  N. [TECHNIQUE]: [one-line description of what to change and why]

Output ONLY the numbered plan. Do NOT output any SQL.

Schema (subset):
{schema_block or "(none)"}

SQL:
{sql}
"""


def _build_reasoning_prompt_apply_plan(sql: str, plan: str, dsn: str, tables: list[str], create_table_map: dict[str, str]) -> str:
    schema_block = _render_schema_min(tables, create_table_map)
    return f"""You are a PostgreSQL 16 SQL optimizer.
Apply each step of the optimization plan below to rewrite the SQL.

Rules:
- Output ONLY the rewritten SQL inside <SQL>...</SQL> tags.
- Preserve the result set exactly: same columns, same ordering, same LIMIT.
- Do NOT include explanations, markdown, or any text outside the <SQL> tags.
- Use only standard PostgreSQL syntax (no hints).

Optimization plan:
{plan.strip()}

Schema (subset):
{schema_block or "(none)"}

SQL:
{sql}
"""


def _artifact_write(dir_path: Path, name: str, content: str) -> None:
    _ensure_parent_dir(dir_path / "placeholder.txt")
    (dir_path / name).write_text(content, encoding="utf-8")


def _run_one(
    *,
    dsn: str,
    query_id: str,
    query_path: Path,
    original_sql: str,
    baseline_median_ms: Optional[float],
    experiment: str,
    variant_id: str,
    model: str,
    prompt: str,
    repeats: int,
    statement_timeout_ms: Optional[int],
    artifacts_dir: Optional[Path],
    stage1_prompt: Optional[str] = None,
    stage1_raw_output_text: Optional[str] = None,
) -> RunRecord:
    raw_output_text = ""
    rewritten_sql: Optional[str] = None
    artifact_dir_str: Optional[str] = None

    try:
        raw_output_text = generate_text(prompt, model)
        rewritten_sql = _extract_sql_from_text(raw_output_text)
    except Exception as exc:
        err = str(exc)
        if artifacts_dir is not None:
            artifact_dir = artifacts_dir / experiment / variant_id / query_id
            _artifact_write(artifact_dir, "prompt.txt", prompt)
            if stage1_prompt is not None:
                _artifact_write(artifact_dir, "stage1_prompt.txt", stage1_prompt)
            if stage1_raw_output_text is not None:
                _artifact_write(artifact_dir, "stage1_raw.txt", stage1_raw_output_text)
            _artifact_write(artifact_dir, "raw.txt", raw_output_text)
        return RunRecord(
            query_id=query_id,
            query_path=str(query_path),
            experiment=experiment,
            variant_id=variant_id,
            model=model,
            prompt=prompt,
            stage1_prompt=stage1_prompt,
            stage1_raw_output_text=stage1_raw_output_text,
            raw_output_text=raw_output_text,
            rewritten_sql=None,
            executed=False,
            success=False,
            run_execution_time_ms=[],
            median_execution_time_ms=None,
            run_planning_time_ms=[],
            median_planning_time_ms=None,
            baseline_median_execution_time_ms=baseline_median_ms,
            speedup=None,
            error_message=err,
            artifact_dir=str(artifacts_dir) if artifacts_dir is not None else None,
        )

    if artifacts_dir is not None:
        artifact_dir = artifacts_dir / experiment / variant_id / query_id
        _artifact_write(artifact_dir, "prompt.txt", prompt)
        if stage1_prompt is not None:
            _artifact_write(artifact_dir, "stage1_prompt.txt", stage1_prompt)
        if stage1_raw_output_text is not None:
            _artifact_write(artifact_dir, "stage1_raw.txt", stage1_raw_output_text)
        _artifact_write(artifact_dir, "raw.txt", raw_output_text)
        _artifact_write(artifact_dir, "sql.sql", rewritten_sql)
        artifact_dir_str = str(artifact_dir)

    run_exec, run_plan, err = _benchmark_one_query(
        dsn=dsn,
        sql=rewritten_sql,
        repeats=repeats,
        statement_timeout_ms=statement_timeout_ms,
    )
    ok = err is None and len(run_exec) == repeats
    med_exec = median(run_exec) if run_exec else None
    med_plan = median(run_plan) if run_plan else None
    speed = (baseline_median_ms / med_exec) if (baseline_median_ms and med_exec) else None
    return RunRecord(
        query_id=query_id,
        query_path=str(query_path),
        experiment=experiment,
        variant_id=variant_id,
        model=model,
        prompt=prompt,
        stage1_prompt=stage1_prompt,
        stage1_raw_output_text=stage1_raw_output_text,
        raw_output_text=raw_output_text,
        rewritten_sql=rewritten_sql,
        executed=True,
        success=ok,
        run_execution_time_ms=run_exec,
        median_execution_time_ms=med_exec,
        run_planning_time_ms=run_plan,
        median_planning_time_ms=med_plan,
        baseline_median_execution_time_ms=baseline_median_ms,
        speedup=speed,
        error_message=err,
        artifact_dir=artifact_dir_str,
    )


def _write_json(path: Path, payload: Any) -> None:
    _ensure_parent_dir(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, results: list[RunRecord]) -> None:
    _ensure_parent_dir(path)
    fieldnames = [
        "experiment",
        "variant_id",
        "query_id",
        "query_path",
        "model",
        "baseline_median_execution_time_ms",
        "median_execution_time_ms",
        "speedup",
        "executed",
        "success",
        "error_message",
        "artifact_dir",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "experiment": r.experiment,
                    "variant_id": r.variant_id,
                    "query_id": r.query_id,
                    "query_path": r.query_path,
                    "model": r.model,
                    "baseline_median_execution_time_ms": r.baseline_median_execution_time_ms,
                    "median_execution_time_ms": r.median_execution_time_ms,
                    "speedup": r.speedup,
                    "executed": r.executed,
                    "success": r.success,
                    "error_message": r.error_message,
                    "artifact_dir": r.artifact_dir,
                }
            )


def _read_optional_text(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8", errors="replace")


def _execute_existing_artifact(
    *,
    dsn: str,
    query_id: str,
    query_path: Path,
    baseline_median_ms: Optional[float],
    experiment: str,
    variant_id: str,
    model: str,
    repeats: int,
    statement_timeout_ms: Optional[int],
    artifact_dir: Path,
) -> RunRecord:
    prompt_path = artifact_dir / "prompt.txt"
    raw_path = artifact_dir / "raw.txt"
    sql_path = artifact_dir / "sql.sql"
    stage1_prompt_path = artifact_dir / "stage1_prompt.txt"
    stage1_raw_path = artifact_dir / "stage1_raw.txt"

    prompt = _read_optional_text(prompt_path) or ""
    raw_output_text = _read_optional_text(raw_path) or ""
    rewritten_sql = _read_optional_text(sql_path)
    stage1_prompt = _read_optional_text(stage1_prompt_path)
    stage1_raw_output_text = _read_optional_text(stage1_raw_path)

    if rewritten_sql is None or not rewritten_sql.strip():
        return RunRecord(
            query_id=query_id,
            query_path=str(query_path),
            experiment=experiment,
            variant_id=variant_id,
            model=model,
            prompt=prompt,
            stage1_prompt=stage1_prompt,
            stage1_raw_output_text=stage1_raw_output_text,
            raw_output_text=raw_output_text,
            rewritten_sql=None,
            executed=False,
            success=False,
            run_execution_time_ms=[],
            median_execution_time_ms=None,
            run_planning_time_ms=[],
            median_planning_time_ms=None,
            baseline_median_execution_time_ms=baseline_median_ms,
            speedup=None,
            error_message="missing sql.sql under artifacts",
            artifact_dir=str(artifact_dir),
        )

    rewritten_sql = _extract_first_statement(rewritten_sql.strip())
    run_exec, run_plan, err = _benchmark_one_query(
        dsn=dsn,
        sql=rewritten_sql,
        repeats=repeats,
        statement_timeout_ms=statement_timeout_ms,
    )
    ok = err is None and len(run_exec) == repeats
    med_exec = median(run_exec) if run_exec else None
    med_plan = median(run_plan) if run_plan else None
    speed = (baseline_median_ms / med_exec) if (baseline_median_ms and med_exec) else None
    return RunRecord(
        query_id=query_id,
        query_path=str(query_path),
        experiment=experiment,
        variant_id=variant_id,
        model=model,
        prompt=prompt,
        stage1_prompt=stage1_prompt,
        stage1_raw_output_text=stage1_raw_output_text,
        raw_output_text=raw_output_text,
        rewritten_sql=rewritten_sql,
        executed=True,
        success=ok,
        run_execution_time_ms=run_exec,
        median_execution_time_ms=med_exec,
        run_planning_time_ms=run_plan,
        median_planning_time_ms=med_plan,
        baseline_median_execution_time_ms=baseline_median_ms,
        speedup=speed,
        error_message=err,
        artifact_dir=str(artifact_dir),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run prompt/reasoning ablations on TPC-DS queries using pro model.")
    parser.add_argument(
        "--dsn",
        default=os.environ.get("PG_DSN", "postgresql://bench:bench@localhost:5432/tpcds_sf1"),
        help="PostgreSQL DSN. Defaults to $PG_DSN or postgresql://bench:bench@localhost:5432/tpcds_sf1",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model name (fixed to pro by default).")
    parser.add_argument("--baseline-json", default=DEFAULT_BASELINE_JSON, help="Baseline JSON path.")
    parser.add_argument("--workload-dir", default=DEFAULT_WORKLOAD_DIR, help="Directory containing q*.sql.")
    parser.add_argument("--query-ids", default=DEFAULT_QUERY_IDS, help=f"Comma-separated query ids. Default: {DEFAULT_QUERY_IDS}")
    parser.add_argument("--repeat", type=int, default=1, help="Repeat count per query per variant (median is reported).")
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000, help="statement_timeout in ms for each run.")
    parser.add_argument("--schema-sql", default="benchmark/postgres/tpcds/schema.sql", help="Schema DDL path for schema extraction.")
    parser.add_argument("--mode", choices=["prompt", "reasoning", "all"], default="all", help="Which experiment group to run.")
    parser.add_argument("--prompt-variants", default="P0_BASE,P1_ENGINE,P2_SCHEMA_MIN,P3_SCHEMA_STATS", help="Comma-separated prompt variant ids.")
    parser.add_argument("--reasoning-variants", default="R0_DIRECT,R1_COT_DELIM,R2_TWO_PASS", help="Comma-separated reasoning variant ids.")
    parser.add_argument("--artifacts-dir", default="benchmark/results/ablation_artifacts", help="Artifacts directory.")
    parser.add_argument("--output-json", default="benchmark/results/postgres_tpcds_sf1_q9_pro_ablations.json", help="Output JSON path.")
    parser.add_argument("--output-csv", default="benchmark/results/postgres_tpcds_sf1_q9_pro_ablations.csv", help="Output CSV path.")
    parser.add_argument("--execute-only", action="store_true", help="Skip LLM calls and execute EXPLAIN using existing artifacts.")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and exit (no LLM, no DB).")
    args = parser.parse_args()
    model = args.model.strip() or DEFAULT_MODEL

    workload_dir = Path(args.workload_dir)
    if not workload_dir.exists() or not workload_dir.is_dir():
        raise SystemExit(f"Workload dir not found: {workload_dir}")

    baseline_path = Path(args.baseline_json)
    if not baseline_path.exists():
        raise SystemExit(f"Baseline JSON not found: {baseline_path}")

    schema_path = Path(args.schema_sql)
    if not schema_path.exists():
        raise SystemExit(f"Schema SQL not found: {schema_path}")

    query_ids = _parse_query_ids(args.query_ids)
    sql_files = sorted(workload_dir.glob("q*.sql"), key=_parse_query_id)
    sql_files = [p for p in sql_files if _parse_query_id(p) in query_ids]
    if not sql_files:
        raise SystemExit("No matching q*.sql after applying --query-ids")

    if args.repeat <= 0:
        raise SystemExit("--repeat must be >= 1")

    prompt_variants = [v.strip() for v in args.prompt_variants.split(",") if v.strip()]
    reasoning_variants = [v.strip() for v in args.reasoning_variants.split(",") if v.strip()]
    for v in prompt_variants:
        if v not in PROMPT_VARIANTS:
            raise SystemExit(f"Unknown prompt variant: {v}")
    for v in reasoning_variants:
        if v not in ("R0_DIRECT", "R1_COT_DELIM", "R2_TWO_PASS"):
            raise SystemExit(f"Unknown reasoning variant: {v}")

    baseline_medians = _load_baseline_medians(baseline_path)
    create_table_map = _read_schema_create_table_map(schema_path)
    known_tables = set(create_table_map.keys())

    artifacts_dir = Path(args.artifacts_dir) if args.artifacts_dir else None
    if artifacts_dir is not None:
        _ensure_parent_dir(artifacts_dir / "placeholder.txt")

    results: list[RunRecord] = []

    if args.dry_run:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "engine": "postgresql",
            "dsn": args.dsn,
            "model": model,
            "workload_dir": str(workload_dir),
            "query_ids": sorted(query_ids),
            "repeat": int(args.repeat),
            "statement_timeout_ms": int(args.statement_timeout_ms),
            "mode": args.mode,
            "prompt_variants": prompt_variants,
            "reasoning_variants": reasoning_variants,
            "dry_run": True,
            "results": [],
        }
        _write_json(Path(args.output_json), payload)
        _write_csv(Path(args.output_csv), results)
        return 0

    _check_db_connection(args.dsn)

    if args.execute_only:
        for sql_path in sql_files:
            query_id = sql_path.stem
            baseline_median_ms = baseline_medians.get(query_id)

            if artifacts_dir is None:
                raise SystemExit("Missing --artifacts-dir")

            if args.mode in ("prompt", "all"):
                for vid in prompt_variants:
                    artifact_subdir = artifacts_dir / "prompt" / vid / query_id
                    results.append(
                        _execute_existing_artifact(
                            dsn=args.dsn,
                            query_id=query_id,
                            query_path=sql_path,
                            baseline_median_ms=baseline_median_ms,
                            experiment="prompt",
                            variant_id=vid,
                            model=model,
                            repeats=args.repeat,
                            statement_timeout_ms=args.statement_timeout_ms,
                            artifact_dir=artifact_subdir,
                        )
                    )

            if args.mode in ("reasoning", "all"):
                for vid in reasoning_variants:
                    artifact_subdir = artifacts_dir / "reasoning" / vid / query_id
                    results.append(
                        _execute_existing_artifact(
                            dsn=args.dsn,
                            query_id=query_id,
                            query_path=sql_path,
                            baseline_median_ms=baseline_median_ms,
                            experiment="reasoning",
                            variant_id=vid,
                            model=model,
                            repeats=args.repeat,
                            statement_timeout_ms=args.statement_timeout_ms,
                            artifact_dir=artifact_subdir,
                        )
                    )

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "engine": "postgresql",
            "dsn": args.dsn,
            "model": model,
            "workload_dir": str(workload_dir),
            "query_ids": sorted(query_ids),
            "repeat": int(args.repeat),
            "statement_timeout_ms": int(args.statement_timeout_ms),
            "mode": args.mode,
            "prompt_variants": prompt_variants,
            "reasoning_variants": reasoning_variants,
            "dry_run": False,
            "execute_only": True,
            "results": [asdict(r) for r in results],
        }
        _write_json(Path(args.output_json), payload)
        _write_csv(Path(args.output_csv), results)
        return 0

    for sql_path in sql_files:
        query_id = sql_path.stem
        original_sql = _load_sql(sql_path)
        tables = _extract_tables_from_sql(original_sql, known_tables)
        baseline_median_ms = baseline_medians.get(query_id)

        if args.mode in ("prompt", "all"):
            for vid in prompt_variants:
                prompt = _build_prompt_variant(
                    variant_id=vid,
                    sql=original_sql,
                    dsn=args.dsn,
                    tables=tables,
                    create_table_map=create_table_map,
                )
                results.append(
                    _run_one(
                        dsn=args.dsn,
                        query_id=query_id,
                        query_path=sql_path,
                        original_sql=original_sql,
                        baseline_median_ms=baseline_median_ms,
                        experiment="prompt",
                        variant_id=vid,
                        model=model,
                        prompt=prompt,
                        repeats=args.repeat,
                        statement_timeout_ms=args.statement_timeout_ms,
                        artifacts_dir=artifacts_dir,
                    )
                )

        if args.mode in ("reasoning", "all"):
            for vid in reasoning_variants:
                if vid == "R0_DIRECT":
                    prompt = _build_reasoning_prompt_direct(original_sql, args.dsn, tables, create_table_map)
                    results.append(
                        _run_one(
                            dsn=args.dsn,
                            query_id=query_id,
                            query_path=sql_path,
                            original_sql=original_sql,
                            baseline_median_ms=baseline_median_ms,
                            experiment="reasoning",
                            variant_id=vid,
                            model=model,
                            prompt=prompt,
                            repeats=args.repeat,
                            statement_timeout_ms=args.statement_timeout_ms,
                            artifacts_dir=artifacts_dir,
                        )
                    )
                    continue

                if vid == "R1_COT_DELIM":
                    prompt = _build_reasoning_prompt_cot(original_sql, args.dsn, tables, create_table_map)
                    results.append(
                        _run_one(
                            dsn=args.dsn,
                            query_id=query_id,
                            query_path=sql_path,
                            original_sql=original_sql,
                            baseline_median_ms=baseline_median_ms,
                            experiment="reasoning",
                            variant_id=vid,
                            model=model,
                            prompt=prompt,
                            repeats=args.repeat,
                            statement_timeout_ms=args.statement_timeout_ms,
                            artifacts_dir=artifacts_dir,
                        )
                    )
                    continue

                stage1_prompt = _build_reasoning_prompt_plan(original_sql, args.dsn, tables, create_table_map)
                stage1_raw = generate_text(stage1_prompt, model)
                prompt = _build_reasoning_prompt_apply_plan(original_sql, stage1_raw, args.dsn, tables, create_table_map)
                results.append(
                    _run_one(
                        dsn=args.dsn,
                        query_id=query_id,
                        query_path=sql_path,
                        original_sql=original_sql,
                        baseline_median_ms=baseline_median_ms,
                        experiment="reasoning",
                        variant_id=vid,
                        model=model,
                        prompt=prompt,
                        repeats=args.repeat,
                        statement_timeout_ms=args.statement_timeout_ms,
                        artifacts_dir=artifacts_dir,
                        stage1_prompt=stage1_prompt,
                        stage1_raw_output_text=stage1_raw,
                    )
                )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "postgresql",
        "dsn": args.dsn,
        "model": model,
        "workload_dir": str(workload_dir),
        "query_ids": sorted(query_ids),
        "repeat": int(args.repeat),
        "statement_timeout_ms": int(args.statement_timeout_ms),
        "mode": args.mode,
        "prompt_variants": prompt_variants,
        "reasoning_variants": reasoning_variants,
        "dry_run": False,
        "results": [asdict(r) for r in results],
    }
    _write_json(Path(args.output_json), payload)
    _write_csv(Path(args.output_csv), results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
