from __future__ import annotations

from pathlib import Path

import experiments.prompt_builder as prompt_builder_module
from experiments.prompt_builder import ExperimentPromptBuilderLayer
from pipeline.models import PipelineRequest, WorkloadItem


def _write_schema(tmp_path: Path) -> Path:
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        """
CREATE TABLE employee (
    employee_id INTEGER,
    employee_name TEXT,
    department TEXT
);

CREATE TABLE payroll (
    employee_id INTEGER,
    salary NUMERIC
);
""".strip(),
        encoding="utf-8",
    )
    return schema_file


def test_p2_uses_subset_schema_context_and_sql_tags(tmp_path: Path) -> None:
    schema_file = _write_schema(tmp_path)
    builder = ExperimentPromptBuilderLayer(dsn="", schema_file=str(schema_file))
    workload_item = WorkloadItem(
        query_id="q1",
        raw_query="SELECT e.employee_name, p.salary FROM employee e JOIN payroll p ON p.employee_id = e.employee_id",
        engine="PostgreSQL",
    )

    prompt = builder.build(
        workload_item,
        PipelineRequest(prompt_strategy="P2", reasoning_mode="R0", model="gpt-5-mini"),
    )

    assert "Schema (subset):" in prompt.prompt_text
    assert "- employee(employee_id, employee_name, department)" in prompt.prompt_text
    assert "- payroll(employee_id, salary)" in prompt.prompt_text
    assert "<SQL>...</SQL>" in prompt.prompt_text


def test_p3_uses_rich_schema_renderer_when_available(tmp_path: Path, monkeypatch) -> None:
    schema_file = _write_schema(tmp_path)
    builder = ExperimentPromptBuilderLayer(dsn="postgresql://example", schema_file=str(schema_file))
    workload_item = WorkloadItem(query_id="q3", raw_query="SELECT * FROM employee", engine="PostgreSQL")

    monkeypatch.setattr(
        prompt_builder_module,
        "_render_schema_rich",
        lambda tables, dsn, create_table_map: "TABLE employee  (~10 rows)\n  employee_id  INTEGER",
    )

    prompt = builder.build(
        workload_item,
        PipelineRequest(prompt_strategy="P3", reasoning_mode="R1", model="gpt-5-mini"),
    )

    assert "TABLE employee  (~10 rows)" in prompt.prompt_text
    assert "<THINKING>...</THINKING>" in prompt.prompt_text
    assert "<SQL>...</SQL>" in prompt.prompt_text
