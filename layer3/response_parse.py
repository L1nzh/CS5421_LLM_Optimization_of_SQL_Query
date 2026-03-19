from __future__ import annotations

from typing import Any, Iterable, Optional


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _first_non_empty_str(values: Iterable[Any]) -> Optional[str]:
    for v in values:
        if isinstance(v, str) and v.strip():
            return v
    return None


def extract_output_text(response: Any) -> str:
    text = _first_non_empty_str(
        [
            _get(response, "output_text", None),
            _get(response, "text", None),
        ]
    )
    if text:
        return text

    output = _get(response, "output", None)
    if not isinstance(output, list):
        raise RuntimeError("Unexpected response format: missing output list")

    texts: list[str] = []
    for block in output:
        block_type = _get(block, "type", None)

        if block_type == "output_text":
            t = _get(block, "text", None)
            if isinstance(t, str) and t:
                texts.append(t)
            continue

        if block_type != "message":
            continue

        content = _get(block, "content", None)
        if not isinstance(content, list):
            continue

        for part in content:
            part_type = _get(part, "type", None)
            if part_type not in ("output_text", "text"):
                continue
            t = _get(part, "text", None)
            if isinstance(t, str) and t:
                texts.append(t)

    if texts:
        return "".join(texts).strip()

    raise RuntimeError("No output text found in response")
