from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Optional


@dataclass
class QueryExecutionResult:
    query: str
    success: bool
    columns: list[str]
    rows: list[tuple[Any, ...]]
    execution_time_ms: float
    error_message: Optional[str] = None


@dataclass
class QueryStreamResult:
    query: str
    success: bool
    columns: list[str]
    rows: Iterable[tuple[Any, ...]]
    error_message: Optional[str] = None
    close: Callable[[], None] = lambda: None


@dataclass(frozen=True)
class NormalizedResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


@dataclass(frozen=True)
class HashedResult:
    columns: tuple[str, ...]
    row_count: int
    digest: str


@dataclass
class CandidateValidationResult:
    query: str
    is_valid: bool
    reason: str
    execution_time_ms: float
    error_message: Optional[str] = None


@dataclass
class ValidationReport:
    raw_query: str
    baseline_execution_time_ms: float
    baseline_row_count: int
    baseline_columns: list[str]
    results: list[CandidateValidationResult] = field(default_factory=list)
    baseline_error_message: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
