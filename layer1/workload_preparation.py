from __future__ import annotations

from pathlib import Path

from pipeline.models import PipelineRequest, WorkloadItem


class FileOrStringWorkloadPreparationLayer:
    """Layer 1: prepares raw query workload items from strings or files."""

    def prepare(self, request: PipelineRequest) -> list[WorkloadItem]:
        query_sources: list[tuple[str, str | None]] = []
        for raw_query in request.raw_queries:
            query_sources.append((raw_query.strip(), None))
        for query_file in request.query_files:
            path = Path(query_file)
            query_sources.append((path.read_text(encoding="utf-8").strip(), str(path)))

        if not query_sources:
            raise ValueError("At least one raw query or query file must be provided")

        schema_text = self._load_optional_text(request.schema_text, request.schema_file)
        index_text = self._load_optional_text(request.index_text, request.index_file)

        items: list[WorkloadItem] = []
        for idx, (raw_query, source_path) in enumerate(query_sources, start=1):
            query_id = Path(source_path).stem if source_path else f"query_{idx}"
            items.append(
                WorkloadItem(
                    query_id=query_id,
                    raw_query=self._strip_trailing_semicolon(raw_query),
                    engine=request.engine,
                    source_path=source_path,
                    schema_text=schema_text,
                    index_text=index_text,
                    metadata=dict(request.extra_metadata),
                )
            )
        return items

    @staticmethod
    def _load_optional_text(text: str | None, file_path: str | None) -> str | None:
        if text:
            return text.strip()
        if file_path:
            return Path(file_path).read_text(encoding="utf-8").strip()
        return None

    @staticmethod
    def _strip_trailing_semicolon(sql: str) -> str:
        stripped = sql.strip()
        while stripped.endswith(";"):
            stripped = stripped[:-1].rstrip()
        if not stripped:
            raise ValueError("Raw query cannot be empty")
        return stripped
