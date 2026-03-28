# Layer 6: Benchmark

## Purpose

Layer 6 measures the performance of valid candidate queries relative to the baseline query.

This layer is intentionally separate from correctness validation so generation and selection are not conflated.

## Default Implementation

- Real PostgreSQL implementation: `layer6/benchmark.py`
  - `PostgresExplainBenchmarkLayer`
- Placeholder implementation:
  - `PlaceholderBenchmarkLayer`

## Responsibilities

- Benchmark baseline query performance
- Benchmark candidate query performance
- Compute median execution and planning time
- Compute candidate speedup relative to baseline

## Input

- `raw_query`
- list of `NormalizedCandidate`

## Output

The layer emits `BenchmarkReport`.

Important fields:

- `baseline_execution_time_ms`
- `baseline_planning_time_ms`
- `candidate_results`

Each `CandidateBenchmarkResult` includes:

- `candidate_id`
- `query`
- `success`
- `execution_time_ms`
- `planning_time_ms`
- `speedup`
- `error_message`

## Current Behavior

### PostgreSQL implementation

The default PostgreSQL benchmark layer uses:

- `EXPLAIN (ANALYZE, FORMAT JSON)`

It currently collects:

- execution time
- planning time
- derived speedup

### Placeholder implementation

If no DSN is provided in the top-level pipeline, the benchmark layer returns placeholder results and keeps the output structure stable.

## Design Notes

- This layer is not responsible for candidate selection
- It only produces performance signals for later ranking

## TODO

- Add repeated-run variance and stability metrics
- Add support for cold-cache and warm-cache modes
- Add richer PostgreSQL metrics such as buffers, temp reads/writes, and total cost
- Add support for additional engines beyond PostgreSQL
