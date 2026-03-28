# Layer 4: Candidate Normalization

## Purpose

Layer 4 extracts executable SQL from raw LLM outputs.

This layer is where model text is converted into a normalized SQL candidate that later layers can validate and benchmark.

## Default Implementation

- Module: `layer4/candidate_normalizer.py`
- Class: `DefaultCandidateNormalizationLayer`

## Responsibilities

- Remove markdown fences
- Extract SQL from `<SQL>...</SQL>` tags when present
- Remove accidental `EXPLAIN` prefixes
- Trim non-SQL text before the first `WITH` or `SELECT`
- Keep only the first SQL statement

## Input

The layer consumes a list of `GeneratedCandidate`.

## Output

The layer emits a list of `NormalizedCandidate`.

Each normalized candidate contains:

- `candidate_id`
- `raw_text`
- `sql`
- `model`
- `normalization_error`
- `stage1_text`

## Current Behavior

- If SQL extraction succeeds, `sql` is populated
- If SQL extraction fails, `normalization_error` is populated instead
- Failed normalization does not crash the pipeline; it is handled later by Layer 5 and Layer 7

## Design Notes

- This layer is intentionally syntax-light
- It does not try to fully parse SQL
- It only prepares likely executable SQL for downstream processing

## TODO

- Add stricter SQL safety checks such as rejecting DDL/DML when the experiment is SELECT-only
- Add multi-statement detection and explicit rejection reasons
- Add dialect cleanup and canonical formatting
- Add parser-backed extraction instead of regex-only heuristics
