# Layer 7: Ranking

## Purpose

Layer 7 ranks candidates and chooses the best optimized query based on validation and benchmark outputs.

This layer is separated from generation and benchmarking so the selection logic can be tested independently.

## Default Implementation

- Module: `layer7/ranking.py`
- Class: `SpeedupRankingLayer`

## Responsibilities

- Merge normalized candidates with validation outcomes
- Merge benchmark results with candidate ids
- Compute a score for each candidate
- Produce a ranked list from best to worst

## Current Scoring Rule

The default implementation uses a simple rule:

- invalid candidates receive no score
- valid candidates with benchmark speedup use `speedup` as score
- valid candidates without benchmark success receive score `0.0`

This makes the current ranking behavior easy to reason about and easy to replace later.

## Input

- list of `NormalizedCandidate`
- `ValidationReport`
- `BenchmarkReport`

## Output

The layer emits a list of `RankedCandidate`.

Each ranked candidate includes:

- `rank`
- `score`
- `is_valid`
- `validation_reason`
- `speedup`
- `execution_time_ms`
- `planning_time_ms`
- `benchmark_error`

## Design Notes

- Ranking is candidate-id aware for benchmark results
- Ranking follows validation result order to preserve candidate alignment
- This layer can be swapped without touching the upstream layers

## TODO

- Replace the simple speedup-only rule with a research scoring policy
- Add support for weighted scoring across correctness confidence, speedup, and stability
- Add tie-breaking policies
- Add explicit “selected candidate rationale” output
