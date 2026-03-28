# Layer 8: Analysis

## Purpose

Layer 8 summarizes pipeline outcomes for later research reporting.

In the full architecture, this layer is where results should be aggregated into tables, failure taxonomies, and paper-ready outputs.

## Default Implementation

- Module: `layer8/analysis.py`
- Class: `PlaceholderAnalysisLayer`

## Responsibilities

- Consume the per-query optimization result
- Produce a lightweight analysis summary
- Keep the final pipeline output shape stable even before full research analytics are implemented

## Input

- `QueryOptimizationResult`

## Output

The layer emits `AnalysisReport`.

Current fields:

- `summary`
- `metadata`

## Current Behavior

The default placeholder currently reports:

- total candidate count
- number of valid candidates
- whether a selected query exists

## Design Notes

- This layer is intentionally a placeholder for now
- The important architectural decision is that analysis remains separate from validation, benchmarking, and ranking

## TODO

- Add failure taxonomy aggregation
- Add prompt/model comparison summaries
- Add benchmark distribution analysis
- Add structured export for markdown tables, CSV, or visualization tooling
- Add experiment-level aggregation across many query runs
