# Layer 6: Benchmark

## Purpose

Layer 6 measures the physical execution behavior of semantically valid candidate queries relative to the baseline query.

This layer is intentionally separated from correctness validation so the system does not conflate:

- semantic equivalence
- execution performance
- final candidate ranking

## Default Implementation

- Real PostgreSQL implementation: `layer6/benchmark.py`
  - `PostgresExplainBenchmarkLayer`
- Placeholder implementation:
  - `PlaceholderBenchmarkLayer`

## Responsibilities

- benchmark baseline query performance
- benchmark candidate query performance
- repeat executions and aggregate timing signals
- collect buffer and temp-block statistics from PostgreSQL
- derive a memory-efficiency score from buffer usage
- return a structured benchmark report for downstream ranking

## Input

- `raw_query`
- list of `NormalizedCandidate`

The benchmark layer receives normalized candidates from Layer 4. It does not perform semantic validation itself.

## Output

The layer emits `BenchmarkReport`.

Important baseline fields:

- `baseline_query`
- `baseline_execution_time_ms`
- `baseline_planning_time_ms`
- `baseline_buffer_stats`
- `baseline_memory_score`

Each `CandidateBenchmarkResult` includes:

- `candidate_id`
- `query`
- `success`
- `execution_time_ms`
- `planning_time_ms`
- `speedup`
- `error_message`
- `buffer_stats`
- `memory_score`

## Current Behavior

### PostgreSQL implementation

The real PostgreSQL benchmark layer uses:

- `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`

For each benchmarked query it currently:

- executes the query `repeats` times
- collects execution time from each run
- collects planning time when PostgreSQL returns it
- uses the median execution time as the representative execution latency
- uses the median planning time as the representative planning latency
- extracts plan-tree buffer counters from the JSON plan
- aggregates buffer counters recursively across the full plan tree

### Buffer and memory metrics

The benchmark layer records these PostgreSQL counters:

- `shared_hit_blocks`
- `shared_read_blocks`
- `shared_dirtied_blocks`
- `shared_written_blocks`
- `temp_read_blocks`
- `temp_written_blocks`

These are wrapped in `BufferStats`, which also derives:

- `total_shared_blocks`
- `cache_hit_ratio`
- `total_temp_blocks`
- `memory_score`

### Memory score

The current memory score is a derived efficiency signal in the range `0.0` to `1.0` where higher is better.

Current logic:

- reward higher shared-buffer cache hit ratio
- penalize temp-block usage, which indicates disk spill
- combine both into a simple weighted score:
  - `70%` cache-hit behavior
  - `30%` temp-spill avoidance

This is intended as a practical research signal rather than a universal DBMS cost metric.

### Baseline vs candidate comparison

For each candidate:

- the baseline query is benchmarked first
- the candidate query is benchmarked separately
- candidate `speedup` is computed as:
  - `baseline_execution_time_ms / candidate_execution_time_ms`

Interpretation:

- `speedup > 1.0` means the candidate is faster than baseline
- `speedup < 1.0` means the candidate is slower than baseline

### Statement timeout

If configured, the PostgreSQL implementation sets:

- `SET statement_timeout = ...`

before benchmarking runs. This protects long experiment runs from hanging indefinitely on pathological queries.

### Placeholder implementation

If real benchmarking is unavailable, `PlaceholderBenchmarkLayer` returns a structurally valid `BenchmarkReport` with:

- no timing metrics
- no buffer metrics
- candidate results marked unsuccessful
- `error_message="Benchmark skipped (placeholder)"`

This allows the pipeline to remain runnable even without a live database benchmark backend.

## Design Notes

- This layer is not responsible for correctness validation.
- This layer is not responsible for final candidate selection.
- It only produces performance signals for Layer 7.
- The PostgreSQL implementation currently uses the last run's buffer statistics as the representative buffer profile for a query.
- Timing aggregation uses median rather than mean to reduce sensitivity to outliers across repeated runs.

## Limitations

- Benchmarking currently targets PostgreSQL only.
- Memory score is a heuristic signal derived from buffer counters, not an official PostgreSQL metric.
- The current implementation does not separately model cold-cache and warm-cache runs.
- The benchmark layer does not yet persist run-to-run variance directly as a first-class metric.

## TODO

- add explicit variance and stability metrics across repeated runs
- add cold-cache and warm-cache benchmarking modes
- add more PostgreSQL metrics when useful, such as WAL or plan cost fields
- support additional engines beyond PostgreSQL while preserving the same report interface
