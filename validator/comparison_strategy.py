from __future__ import annotations

from enum import Enum


class ComparisonStrategy(str, Enum):
    EXACT_ORDERED = "exact_ordered"
    EXACT_UNORDERED = "exact_unordered"
    MULTISET = "multiset"
    HASH = "hash"
