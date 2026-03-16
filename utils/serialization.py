from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any


def to_canonical_json(value: Any) -> str:
    """Serialize mappings and sequences with stable ordering."""

    def normalize(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): normalize(val) for key, val in sorted(item.items(), key=lambda pair: str(pair[0]))}
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
            return [normalize(element) for element in item]
        return item

    return json.dumps(normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
