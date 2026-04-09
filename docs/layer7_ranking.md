# Layer 7: Ranking

## Purpose

Layer 7 ranks candidate rewrites and determines which validated candidate should be treated as the best optimized query.

This layer is separated from generation, validation, and benchmarking so the selection policy can evolve independently.

## Default Implementation

- Module: `layer7/ranking.py`
- Class: `SpeedupRankingLayer`

## Responsibilities

- merge normalized candidates with validation results
- merge benchmark results by candidate id
- compute a ranking score for each candidate
- produce a ranked list from best to worst
- preserve enough metadata for downstream reporting

## Input

- list of `NormalizedCandidate`
- `ValidationReport`
- `BenchmarkReport`

## Output

The layer emits a list of `RankedCandidate`.

Each ranked candidate currently includes:

- `candidate_id`
- `query`
- `raw_text`
- `model`
- `rank`
- `score`
- `is_valid`
- `validation_reason`
- `normalization_error`
- `execution_time_ms`
- `planning_time_ms`
- `speedup`
- `benchmark_error`
- `stage1_text`
- `buffer_stats`
- `memory_score`

## Current Ranking Rule

The default implementation is no longer speedup-only.

It uses a composite score that combines:

- `speedup`
- `memory_score`

The default weights are:

- `speedup_weight = 0.7`
- `memory_weight = 0.3`

Current score rule:

- invalid candidates receive `None`
- valid candidates with successful benchmark results receive:
  - `0.7 * speedup + 0.3 * memory_score`
- valid candidates with benchmark success but missing `memory_score` fall back to:
  - `speedup`
- valid candidates without usable benchmark results receive:
  - `0.0`

This keeps correctness as a hard gate while allowing the ranking layer to consider both latency and memory efficiency.

## Candidate Validity Handling

A candidate is considered rankable only if:

- validation marks it as semantically valid
- it does not have a normalization error

This means:

- malformed SQL is not allowed to compete
- semantically invalid rewrites are not allowed to compete
- benchmark results do not override correctness

## Sorting Behavior

Candidates are sorted by:

1. whether a score exists
2. descending score
3. candidate id as a deterministic tie-breaker

Rank assignment behavior:

- candidates with a real score receive rank `1, 2, 3, ...`
- candidates with `None` score are left unranked

This ensures deterministic ordering even when multiple candidates have similar outcomes.

## Design Notes

- Benchmark results are joined by `candidate_id`.
- Validation and normalization still act as hard filters before performance matters.
- Ranking preserves candidate-level execution and memory metadata for later reporting.
- The current implementation is intentionally simple enough to interpret in an experiment report.

## Why Memory Is Included

The current research direction evaluates not only whether a rewrite is faster, but also whether it is more resource-efficient.

Using `memory_score` in ranking helps avoid selecting a candidate that is only marginally faster but significantly worse in buffer usage or temporary spill behavior.

This is especially relevant for large analytical workloads such as TPC-DS.

## Limitations

- The current score uses a fixed linear weighting scheme.
- It does not yet use stability metrics across repeated runs.
- It does not explicitly incorporate planning-time penalties.
- It does not provide an explanatory rationale field describing why a candidate was selected beyond the numeric score.

## TODO

- make ranking weights configurable from experiment settings
- add stability-aware scoring across repeated benchmark runs
- add richer tie-breaking policies
- add explicit selection rationale for reporting
- consider separate ranking modes for latency-first vs resource-first evaluation
