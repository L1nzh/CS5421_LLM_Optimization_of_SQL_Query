from __future__ import annotations

import re
from typing import Optional

from psycopg import connect

from pipeline.models import PipelineRequest, PromptPackage, WorkloadItem


# ---------------------------------------------------------------------------
# Schema utilities (ported from benchmark/postgres/ablation_experiments.py)
# ---------------------------------------------------------------------------

def _read_schema_create_table_map(schema_text: str) -> dict[str, str]:
    """Parse raw DDL text and return a mapping of table_name -> CREATE TABLE DDL."""
    blocks = re.findall(
        r"CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\);",
        schema_text,
        flags=re.I | re.S,
    )
    mapping: dict[str, str] = {}
    for name, body in blocks:
        mapping[name.lower()] = f"CREATE TABLE {name} ({body});"
    return mapping


def _extract_tables_from_sql(sql: str, known_tables: set[str]) -> list[str]:
    """Return sorted list of *known* table names referenced in *sql*."""
    lowered = re.sub(r"\s+", " ", sql).strip().lower()
    tables: list[str] = []
    for t in known_tables:
        if re.search(rf"\b{re.escape(t)}\b", lowered):
            tables.append(t)
    return sorted(set(tables))


def _render_schema_min(tables: list[str], create_table_map: dict[str, str]) -> str:
    """Render a compact ``- table(col1, col2, …)`` schema block."""
    lines: list[str] = []
    for t in tables:
        ddl = create_table_map.get(t.lower())
        if not ddl:
            continue
        col_lines: list[str] = []
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


def _fetch_table_row_estimates(dsn: str, tables: list[str]) -> dict[str, int]:
    """Fetch approximate row counts from pg_class."""
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
    Render a rich schema block including column types, primary keys (PK),
    foreign keys (FK), secondary indexes, and approximate row counts.
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


# ---------------------------------------------------------------------------


class DefaultPromptBuilderLayer:
    """Layer 2: constructs prompt packages without calling the model."""

    def build(self, workload_item: WorkloadItem, request: PipelineRequest) -> PromptPackage:
        prompt_strategy = request.prompt_strategy.upper()
        reasoning_mode = request.reasoning_mode.upper()
        dsn: Optional[str] = request.extra_metadata.get("dsn")
        context_block = self._build_context_block(
            workload_item,
            prompt_strategy=prompt_strategy,
            raw_query=workload_item.raw_query,
            dsn=dsn,
        )

        if reasoning_mode == "TWO_PASS":
            stage1_prompt = self._build_plan_prompt(workload_item.raw_query, context_block, workload_item.engine)
            stage2_template = self._build_apply_plan_template(workload_item.raw_query, context_block, workload_item.engine)
            return PromptPackage(
                query_id=workload_item.query_id,
                raw_query=workload_item.raw_query,
                model=request.model,
                candidate_count=request.candidate_count,
                prompt_strategy=prompt_strategy,
                reasoning_mode=reasoning_mode,
                prompt_text=stage2_template,
                stage1_prompt_text=stage1_prompt,
                stage2_prompt_template=stage2_template,
                metadata={"context_block": context_block},
            )

        prompt_text = self._build_single_prompt(
            raw_query=workload_item.raw_query,
            engine=workload_item.engine,
            prompt_strategy=prompt_strategy,
            reasoning_mode=reasoning_mode,
            context_block=context_block,
        )
        return PromptPackage(
            query_id=workload_item.query_id,
            raw_query=workload_item.raw_query,
            model=request.model,
            candidate_count=request.candidate_count,
            prompt_strategy=prompt_strategy,
            reasoning_mode=reasoning_mode,
            prompt_text=prompt_text,
            metadata={"context_block": context_block},
        )

    def _build_single_prompt(
        self,
        *,
        raw_query: str,
        engine: str,
        prompt_strategy: str,
        reasoning_mode: str,
        context_block: str,
    ) -> str:
        lines = self._strategy_header(prompt_strategy, engine)
        if reasoning_mode == "COT_DELIM":
            lines.extend(
                [
                    "Rules:",
                    "1. The result set MUST remain exactly the same (identical columns, ordering, and LIMIT).",
                    "2. Use only standard PostgreSQL syntax. Do not use query hints (e.g., pg_hint_plan).",
                    "3. Output the optimized query strictly inside <SQL>...</SQL> tags.",
                    "4. First, analyze the query and schema inside <THINKING>...</THINKING> tags. Briefly identify the bottleneck and state your rewrite strategy.",
                    "5. Finally, output the optimized query strictly inside <SQL>...</SQL> tags.",
                ]
            )
        else:
            lines.extend(
                [
                    "Rules:",
                    "1. The result set MUST remain exactly the same (identical columns, ordering, and LIMIT).",
                    "2. Use only standard PostgreSQL syntax. Do not use query hints (e.g., pg_hint_plan).",
                    "3. Output the optimized query strictly inside <SQL>...</SQL> tags.",
                ]
            )

        if context_block:
            lines.extend(["", context_block])

        lines.extend(["", "SQL:", raw_query])
        return "\n".join(lines)

    @staticmethod
    def _strategy_header(prompt_strategy: str, engine: str) -> list[str]:
        header = [
            f"You are an expert {engine} SQL optimizer.",
            f"Target engine: {engine}.",
            f"Rewrite the SQL to be semantically equivalent but potentially faster on {engine}."
        ]
        engine_features = [
            f"You may use any {engine} feature, including:",
            "- CTEs (Use MATERIALIZED strictly as an optimization fence for heavy aggregations referenced multiple times).",
            "- LATERAL joins for set-returning functions or complex correlated subqueries.",
            "- Window functions (including aggregate FILTER clauses).",
            "- DISTINCT ON (to replace expensive GROUP BY + Window function combos where safe)."
        ]
        rules = [
            "Rules:",
            "1. The result set MUST remain exactly the same (identical columns, ordering, and LIMIT).",
            "2. Use only standard PostgreSQL syntax. Do not use query hints (e.g., pg_hint_plan).",
            "3. Output the optimized query strictly inside <SQL>...</SQL> tags.",
        ]
        rewrite_strategies = [
            "Apply the following rewrite strategies where applicable:",
            "1. Predicate Pushdown: Push WHERE conditions as deep into subqueries or CTEs as logically possible to reduce the intermediate working set early.",
            "2. Join Simplification: Eliminate redundant joins if the joined table's columns are not selected AND the join is guaranteed to be 1:1 based on the provided Primary/Foreign Keys.",
            "3. Subquery Decorrelation: Convert correlated subqueries to JOINs or LATERAL joins to enable Hash or Merge joins instead of Nested Loops.",
            "4. Anti-Join Optimization: Convert `NOT IN` subqueries to `NOT EXISTS` to avoid NULL-handling performance traps and enable Postgres Hash Anti Joins.",
            "5. Aggregate Consolidation: Convert scalar aggregate subqueries in SELECT lists to Window Functions to avoid multiple passes over the same data."
        ]
        if prompt_strategy == "P0_BASE":
            return header + rules
        if prompt_strategy == "P1_ENGINE":
            return header + engine_features + rules
        if prompt_strategy == "P2_SCHEMA_MIN":
            return header + engine_features + rules
        if prompt_strategy == "P3_SCHEMA_STATS":
            return header + engine_features + rules
        if prompt_strategy == "P4_RULES":
            return header + engine_features + rewrite_strategies + rules
        return header + [
            "Use only syntax supported by the target engine.",
        ]

    @staticmethod
    def _build_context_block(
        workload_item: WorkloadItem,
        *,
        prompt_strategy: str = "",
        raw_query: str = "",
        dsn: Optional[str] = None,
    ) -> str:
        """Build the context block injected into the prompt.

        For strategies that need a rendered schema (P2_SCHEMA_MIN, P4_RULES),
        the raw DDL in ``workload_item.schema_text`` is parsed and rendered in
        compact ``- table(col1, col2, …)`` form, filtered to only the tables
        referenced by the query.

        For P3_SCHEMA_STATS, when a *dsn* is provided, the rich schema
        renderer is used (column types, PK/FK, indexes, row counts).
        Without a DSN it falls back to ``_render_schema_min``.
        """
        lines: list[str] = []
        schema_text: Optional[str] = workload_item.schema_text

        if schema_text and prompt_strategy in ("P2_SCHEMA_MIN", "P3_SCHEMA_STATS", "P4_RULES"):
            create_table_map = _read_schema_create_table_map(schema_text)
            if create_table_map:
                tables = _extract_tables_from_sql(raw_query, set(create_table_map.keys()))
                if prompt_strategy == "P3_SCHEMA_STATS" and dsn:
                    rendered = _render_schema_rich(tables, dsn, create_table_map)
                else:
                    rendered = _render_schema_min(tables, create_table_map)
                if rendered:
                    schema_text = rendered

        if schema_text:
            lines.extend(["Schema (subset):", schema_text])

        if workload_item.index_text:
            if lines:
                lines.append("")
            lines.extend(["Index context:", workload_item.index_text])
        return "\n".join(lines)

    @staticmethod
    def _build_plan_prompt(raw_query: str, context_block: str, engine: str) -> str:
        lines = [
            f"You are an expert {engine} SQL optimizer.",
            "Analyze the SQL and produce a numbered optimization plan.",
            "Do not output SQL in this step.",
        ]
        if context_block:
            lines.extend(["", context_block])
        lines.extend(["", "SQL:", raw_query])
        return "\n".join(lines)

    @staticmethod
    def _build_apply_plan_template(raw_query: str, context_block: str, engine: str) -> str:
        lines = [
            f"You are an expert {engine} SQL optimizer.",
            "Apply the optimization plan below to rewrite the SQL.",
            "Return only one optimized SQL query.",
            "Do not output explanations or markdown.",
            "Preserve the result set exactly.",
            "",
            "Optimization plan:",
            "{plan}",
        ]
        if context_block:
            lines.extend(["", context_block])
        lines.extend(["", "SQL:", raw_query])
        return "\n".join(lines)
