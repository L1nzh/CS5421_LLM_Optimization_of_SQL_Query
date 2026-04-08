from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from psycopg import connect

from pipeline.models import PipelineRequest, PromptPackage, WorkloadItem


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
    """Return sorted list of known table names referenced in sql."""
    lowered = re.sub(r"\s+", " ", sql).strip().lower()
    tables: list[str] = []
    for table in known_tables:
        if re.search(rf"\b{re.escape(table)}\b", lowered):
            tables.append(table)
    return sorted(set(tables))


def _render_schema_min(tables: list[str], create_table_map: dict[str, str]) -> str:
    """Render a compact ``- table(col1, col2, …)`` schema block."""
    lines: list[str] = []
    for table in tables:
        ddl = create_table_map.get(table.lower())
        if not ddl:
            continue
        column_names: list[str] = []
        for raw in ddl.splitlines():
            raw = raw.strip().rstrip(",")
            if not raw or raw.lower().startswith("create table"):
                continue
            if raw.startswith(")"):
                continue
            column = raw.split()[0].strip().strip('"')
            if column:
                column_names.append(column)
        if column_names:
            suffix = "..." if len(column_names) > 60 else ""
            lines.append(f"- {table}({', '.join(column_names[:60])}{suffix})")
        else:
            lines.append(f"- {table}")
    return "\n".join(lines)


def _render_schema_full_ddl(tables: list[str], create_table_map: dict[str, str]) -> dict[str, dict]:
    """
    Parse CREATE TABLE DDL into structured column and constraint info.
    """
    result: dict[str, dict] = {}
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

        columns: list[dict[str, object]] = []
        primary_key: list[str] = []
        unique_keys: list[list[str]] = []
        foreign_keys: list[dict[str, object]] = []

        body_match = re.search(r"CREATE\s+TABLE\s+\S+\s*\((.*)\)\s*;?$", ddl, re.IGNORECASE | re.DOTALL)
        if not body_match:
            result[table] = {"columns": [], "primary_key": [], "unique": [], "foreign_keys": []}
            continue
        body = body_match.group(1)

        depth = 0
        current: list[str] = []
        clauses: list[str] = []
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
                match = pk_re.search(clause_stripped)
                if match:
                    primary_key = [col.strip().strip('"') for col in match.group(1).split(",")]
            elif fk_re.search(clause_stripped):
                match = fk_re.search(clause_stripped)
                if match:
                    foreign_keys.append(
                        {
                            "cols": [col.strip().strip('"') for col in match.group(1).split(",")],
                            "ref_table": match.group(2).strip().lower(),
                            "ref_cols": [col.strip().strip('"') for col in match.group(3).split(",")],
                        }
                    )
            elif uq_re.search(clause_stripped):
                match = uq_re.search(clause_stripped)
                if match:
                    unique_keys.append([col.strip().strip('"') for col in match.group(1).split(",")])
            elif upper.startswith("PRIMARY") or upper.startswith("CONSTRAINT"):
                continue
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

    output: dict[str, int] = {}
    for relname, est_rows in rows:
        if isinstance(relname, str) and isinstance(est_rows, int):
            output[relname] = est_rows
    return output


def _fetch_indexes(dsn: str, tables: list[str]) -> dict[str, list[dict[str, object]]]:
    """
    Return index info per table from pg_catalog.
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
    array_agg(a.attname ORDER BY k.pos)   AS columns
FROM pg_class      t
JOIN pg_index      ix ON ix.indrelid = t.oid
JOIN pg_class      i  ON i.oid = ix.indexrelid
JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, pos)
     ON true
JOIN pg_attribute  a  ON a.attrelid = t.oid AND a.attnum = k.attnum
WHERE t.relkind = 'r'
  AND t.relname = ANY(%s)
  AND NOT ix.indisprimary
GROUP BY t.relname, i.relname, ix.indisunique, ix.indisprimary, ix.indpred
ORDER BY t.relname, i.relname;
"""
    output: dict[str, list[dict[str, object]]] = {table: [] for table in tables}
    try:
        with connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, (tables,))
                for table_name, index_name, is_unique, _is_primary, is_partial, columns in cur.fetchall():
                    output.setdefault(table_name, []).append(
                        {
                            "name": index_name,
                            "columns": list(columns),
                            "unique": bool(is_unique),
                            "partial": bool(is_partial),
                        }
                    )
    except Exception:
        pass
    return output


def _render_schema_rich(tables: list[str], dsn: str, create_table_map: dict[str, str]) -> str:
    """
    Render a rich schema block with column types, PK/FK, indexes, and row counts.
    """
    parsed = _render_schema_full_ddl(tables, create_table_map)
    indexes = _fetch_indexes(dsn, tables)
    row_estimates = _fetch_table_row_estimates(dsn, tables)

    sorted_tables = sorted(tables, key=lambda table: row_estimates.get(table, 0), reverse=True)
    blocks: list[str] = []
    for table in sorted_tables:
        info = parsed.get(table, {})
        lines = [f"TABLE {table}  (~{row_estimates.get(table, 0):,} rows)"]

        for column in info.get("columns", []):
            null_flag = "" if column["nullable"] else " NOT NULL"
            lines.append(f"  {column['name']}  {column['type']}{null_flag}")

        primary_key = info.get("primary_key", [])
        if primary_key:
            lines.append(f"  PK: ({', '.join(primary_key)})")

        for foreign_key in info.get("foreign_keys", []):
            cols = ", ".join(foreign_key["cols"])
            ref_cols = ", ".join(foreign_key["ref_cols"])
            lines.append(f"  FK: ({cols}) -> {foreign_key['ref_table']}({ref_cols})")

        for unique_key in info.get("unique", []):
            lines.append(f"  UNIQUE: ({', '.join(unique_key)})")

        for index in indexes.get(table, []):
            flags: list[str] = []
            if index["unique"]:
                flags.append("UNIQUE")
            if index["partial"]:
                flags.append("PARTIAL")
            flag_suffix = f" [{', '.join(flags)}]" if flags else ""
            lines.append(f"  INDEX {index['name']}: ({', '.join(index['columns'])}){flag_suffix}")

        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)


class ExperimentPromptBuilderLayer:
    """Prompt builder used by the experiment runner for P0-P3 and R0-R2 combos."""

    def __init__(self, dsn: str, schema_file: str):
        self._dsn = dsn
        self._schema_file = Path(schema_file)
        self._default_schema_text = self._schema_file.read_text(encoding="utf-8")
        self._schema_map = _read_schema_create_table_map(self._default_schema_text)
        self._known_tables = set(self._schema_map.keys())

    def build(self, workload_item: WorkloadItem, request: PipelineRequest) -> PromptPackage:
        prompt_strategy = request.prompt_strategy.upper()
        reasoning_mode = self._normalize_reasoning_mode(request.reasoning_mode)
        context_block = self._build_context_block(workload_item, prompt_strategy=prompt_strategy, raw_query=workload_item.raw_query)
        tables = _extract_tables_from_sql(workload_item.raw_query, self._known_tables)

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
                metadata={"context_block": context_block, "tables": tables},
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
            metadata={"context_block": context_block, "tables": tables},
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

    def _build_context_block(
        self,
        workload_item: WorkloadItem,
        *,
        prompt_strategy: str,
        raw_query: str,
    ) -> str:
        lines: list[str] = []
        schema_text = workload_item.schema_text or self._default_schema_text

        if schema_text and prompt_strategy in {
            "P2",
            "P2_SCHEMA_MIN",
            "P3",
            "P3_SCHEMA_STATS",
        }:
            create_table_map = _read_schema_create_table_map(schema_text)
            if create_table_map:
                tables = _extract_tables_from_sql(raw_query, set(create_table_map.keys()))
                if prompt_strategy in {"P3", "P3_SCHEMA_STATS"} and self._dsn:
                    rendered = _render_schema_rich(tables, self._dsn, create_table_map)
                else:
                    rendered = _render_schema_min(tables, create_table_map)
                if rendered:
                    schema_text = rendered

        if schema_text and prompt_strategy not in {"P0", "P0_BASE", "P1", "P1_ENGINE"}:
            lines.extend(["Schema (subset):", schema_text])

        if workload_item.index_text:
            if lines:
                lines.append("")
            lines.extend(["Index context:", workload_item.index_text])

        return "\n".join(lines)

    @staticmethod
    def _normalize_reasoning_mode(reasoning_mode: str) -> str:
        normalized = reasoning_mode.upper()
        if normalized in {"R0", "DIRECT"}:
            return "DIRECT"
        if normalized in {"R1", "COT", "COT_DELIM"}:
            return "COT_DELIM"
        if normalized in {"R2", "TWO_PASS"}:
            return "TWO_PASS"
        return normalized

    @staticmethod
    def _strategy_header(prompt_strategy: str, engine: str) -> list[str]:
        header = [
            f"You are an expert {engine} SQL optimizer.",
            f"Target engine: {engine}.",
            f"Rewrite the SQL to be semantically equivalent but potentially faster on {engine}.",
        ]
        engine_features = [
            f"You may use any {engine} feature, including:",
            "- CTEs (Use MATERIALIZED strictly as an optimization fence for heavy aggregations referenced multiple times).",
            "- LATERAL joins for set-returning functions or complex correlated subqueries.",
            "- Window functions (including aggregate FILTER clauses).",
            "- DISTINCT ON (to replace expensive GROUP BY + Window function combos where safe).",
        ]
        rules = [
            "Rules:",
            "1. The result set MUST remain exactly the same (identical columns, ordering, and LIMIT).",
            "2. Use only standard PostgreSQL syntax. Do not use query hints (e.g., pg_hint_plan).",
            "3. Output the optimized query strictly inside <SQL>...</SQL> tags.",
        ]
        if prompt_strategy in {"P0", "P0_BASE"}:
            return header + rules
        if prompt_strategy in {"P1", "P1_ENGINE"}:
            return header + engine_features + rules
        if prompt_strategy in {"P2", "P2_SCHEMA_MIN"}:
            return header + engine_features + rules
        if prompt_strategy in {"P3", "P3_SCHEMA_STATS"}:
            return header + engine_features + rules
        return header + ["Use only syntax supported by the target engine."]

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
