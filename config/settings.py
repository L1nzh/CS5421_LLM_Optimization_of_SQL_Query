from __future__ import annotations

from dataclasses import dataclass
from math import ceil, log10
from typing import Optional

from validator.comparison_strategy import ComparisonStrategy


@dataclass(frozen=True)
class ValidationSettings:
    comparison_strategy: ComparisonStrategy = ComparisonStrategy.EXACT_UNORDERED
    float_tolerance: float = 1e-6
    preserve_row_order: bool = False
    stream_batch_size: int = 10_000
    trim_strings: bool = False
    normalize_column_names: bool = False
    datetime_format: str = "%Y-%m-%dT%H:%M:%S.%f%z"
    bytes_encoding: str = "utf-8"
    bytes_errors: str = "replace"
    float_precision: Optional[int] = None

    @property
    def effective_float_precision(self) -> int:
        if self.float_precision is not None:
            return self.float_precision
        tolerance = max(self.float_tolerance, 1e-15)
        return max(0, ceil(-log10(tolerance)))
