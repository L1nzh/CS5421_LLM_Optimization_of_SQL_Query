from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass(slots=True)
class QueryExecutionResult:
    query: str
    success: bool
    columns: list[str]
    rows: list[tuple[Any, ...]]
    execution_time_ms: float
    error_message: Optional[str] = None


@dataclass(slots=True, frozen=True)
class NormalizedResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


@dataclass(slots=True)
class CandidateValidationResult:
    query: str
    is_valid: bool
    reason: str
    execution_time_ms: float
    error_message: Optional[str] = None


@dataclass(slots=True)
class ValidationReport:
    raw_query: str
    baseline_execution_time_ms: float
    baseline_row_count: int
    baseline_columns: list[str]
    results: list[CandidateValidationResult] = field(default_factory=list)
    baseline_error_message: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
