from __future__ import annotations

import re

from pipeline.models import GeneratedCandidate, NormalizedCandidate

_FENCE_RE = re.compile(r"```(?:sql)?\s*([\s\S]*?)\s*```", re.IGNORECASE)
_SQL_TAG_RE = re.compile(r"<SQL>\s*([\s\S]*?)\s*</SQL>", re.IGNORECASE)
_THINK_TAG_RE = re.compile(r"<think>\s*[\s\S]*?\s*</think>", re.IGNORECASE)
_EXPLAIN_PREFIX_RE = re.compile(r"^\s*EXPLAIN\s*(?:\([^)]*\))?\s*", re.IGNORECASE)


class DefaultCandidateNormalizationLayer:
    """Layer 4: extracts executable SQL from raw LLM text responses."""

    def normalize(self, generated_candidates: list[GeneratedCandidate]) -> list[NormalizedCandidate]:
        normalized: list[NormalizedCandidate] = []
        for candidate in generated_candidates:
            sql: str | None = None
            error: str | None = None
            try:
                sql = self._extract_sql(candidate.raw_text)
            except Exception as exc:
                error = str(exc)
            normalized.append(
                NormalizedCandidate(
                    candidate_id=candidate.candidate_id,
                    raw_text=candidate.raw_text,
                    sql=sql,
                    model=candidate.model,
                    normalization_error=error,
                    stage1_text=candidate.stage1_text,
                )
            )
        return normalized

    def _extract_sql(self, text: str) -> str:
        candidate = text.strip()
        sql_tag_matches = [match.strip() for match in _SQL_TAG_RE.findall(candidate) if match.strip()]
        if sql_tag_matches:
            candidate = sql_tag_matches[-1]
        else:
            candidate = _THINK_TAG_RE.sub("", candidate).strip()

        fence_match = _FENCE_RE.search(candidate)
        if fence_match:
            candidate = fence_match.group(1).strip()

        candidate = candidate.strip().strip("`").strip()
        candidate = _EXPLAIN_PREFIX_RE.sub("", candidate).strip()

        lowered = candidate.lower()
        starts = [idx for idx in (lowered.find("with"), lowered.find("select")) if idx != -1]
        if starts:
            candidate = candidate[min(starts) :].strip()

        candidate = candidate.split(";")[0].strip()
        if not candidate:
            raise ValueError("No executable SQL could be extracted from model output")
        return candidate
