from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from psycopg import connect

from pipeline.models import PipelineRequest, PromptPackage, WorkloadItem


class ExperimentPromptBuilderLayer:
    """Prompt builder used by the experiment runner for P0-P4 and R0-R2 combos."""

    def __init__(self, dsn: str, schema_file: str):
        self._dsn = dsn
        self._schema_file = Path(schema_file)
        self._schema_map = self._read_schema_create_table_map(self._schema_file)
        self._known_tables = set(self._schema_map.keys())

    def build(self, workload_item: WorkloadItem, request: PipelineRequest) -> PromptPackage:
        prompt_strategy = request.prompt_strategy.upper()
        reasoning_mode = self._normalize_reasoning_mode(request.reasoning_mode)
        tables = self._extract_tables_from_sql(workload_item.raw_query, self._known_tables)
        context_block = self._build_context_block(prompt_strategy, tables)

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

    def _build_context_block(self, prompt_strategy: str, tables: list[str]) -> str:
        if prompt_strategy in {"P0", "P0_BASE"}:
            return ""
        if prompt_strategy in {"P1", "P1_ENGINE"}:
            return ""
        if prompt_strategy in {"P2", "P2_SCHEMA_MIN", "P4", "P4_RULES"}:
            return self._render_schema_min(tables)
        if prompt_strategy in {"P3", "P3_SCHEMA_STATS"}:
            return self._render_schema_stats(tables)
        return ""

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
                    "Reason step by step about performance bottlenecks.",
                    "Then output the final SQL between <SQL> and </SQL>.",
                    "The final SQL must preserve exact semantics.",
                ]
            )
        else:
            lines.extend(
                [
                    "Return only one optimized SQL query.",
                    "Do not output explanations or markdown.",
                    "Preserve the result set exactly.",
                ]
            )

        if context_block:
            lines.extend(["", context_block])

        lines.extend(["", "SQL:", raw_query])
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
        ]
        if prompt_strategy in {"P0", "P0_BASE"}:
            return header
        if prompt_strategy in {"P4", "P4_RULES"}:
            return header + [
                "Rewrite the SQL to be semantically equivalent but potentially faster.",
                "Prefer semantically safe rewrites such as predicate pushdown, join simplification, and avoiding unnecessary SELECT *.",
                "Do not introduce hints or non-standard syntax.",
            ]
        return header + [
            "Rewrite the SQL to be semantically equivalent but potentially faster.",
            "Use only syntax supported by the target engine.",
        ]

    @staticmethod
    def _build_plan_prompt(raw_query: str, context_block: str, engine: str) -> str:
        lines = [
            f"You are an expert {engine} SQL optimizer.",
            "Produce a short optimization plan as numbered bullets.",
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

    @staticmethod
    def _read_schema_create_table_map(schema_path: Path) -> dict[str, str]:
        text = schema_path.read_text(encoding="utf-8")
        blocks = re.findall(r"CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\);", text, flags=re.I | re.S)
        mapping: dict[str, str] = {}
        for name, body in blocks:
            mapping[name.lower()] = f"CREATE TABLE {name} ({body});"
        return mapping

    @staticmethod
    def _extract_tables_from_sql(sql: str, known_tables: set[str]) -> list[str]:
        lowered = re.sub(r"\s+", " ", sql).strip().lower()
        tables: list[str] = []
        for table in known_tables:
            if re.search(rf"\b{re.escape(table)}\b", lowered):
                tables.append(table)
        return sorted(set(tables))

    def _render_schema_min(self, tables: list[str]) -> str:
        lines: list[str] = ["Schema (subset):"]
        for table in tables:
            ddl = self._schema_map.get(table.lower())
            if not ddl:
                continue
            column_names: list[str] = []
            for raw in ddl.splitlines():
                raw = raw.strip().rstrip(",")
                if not raw or raw.lower().startswith("create table") or raw.startswith(")"):
                    continue
                column = raw.split()[0].strip().strip('"')
                if column:
                    column_names.append(column)
            if column_names:
                lines.append(f"- {table}({', '.join(column_names[:60])}{'...' if len(column_names) > 60 else ''})")
        return "\n".join(lines) if len(lines) > 1 else ""

    def _render_schema_stats(self, tables: list[str]) -> str:
        schema_min = self._render_schema_min(tables)
        if not tables:
            return schema_min

        stats_lines = ["", "Schema stats:"]
        row_estimates = self._fetch_table_row_estimates(tables)
        index_map = self._fetch_table_indexes(tables)
        for table in tables:
            stats_lines.append(
                f"- {table}: approx_rows={row_estimates.get(table, 0)}, indexes={', '.join(index_map.get(table, [])) or '(none)'}"
            )
        return (schema_min + "\n" + "\n".join(stats_lines)).strip()

    def _fetch_table_row_estimates(self, tables: list[str]) -> dict[str, int]:
        sql = """
SELECT relname, COALESCE(reltuples, 0)::bigint AS est_rows
FROM pg_class
WHERE relkind = 'r' AND relname = ANY(%s);
"""
        with connect(self._dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, (tables,))
                rows = cur.fetchall()
        return {name: est_rows for name, est_rows in rows if isinstance(name, str) and isinstance(est_rows, int)}

    def _fetch_table_indexes(self, tables: list[str]) -> dict[str, list[str]]:
        sql = """
SELECT tablename, indexname
FROM pg_indexes
WHERE schemaname = current_schema() AND tablename = ANY(%s)
ORDER BY tablename, indexname;
"""
        with connect(self._dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, (tables,))
                rows = cur.fetchall()
        output: dict[str, list[str]] = {table: [] for table in tables}
        for table, index_name in rows:
            if isinstance(table, str) and isinstance(index_name, str):
                output.setdefault(table, []).append(index_name)
        return output
