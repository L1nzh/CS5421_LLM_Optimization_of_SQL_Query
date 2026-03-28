# Layer 1: Workload Preparation

## Purpose

Layer 1 prepares the workload items that will enter the research pipeline.

It is responsible for turning user-provided raw SQL strings or query files into a normalized internal workload representation without invoking any LLM, database validation, or benchmarking logic.

## Default Implementation

- Module: `layer1/workload_preparation.py`
- Class: `FileOrStringWorkloadPreparationLayer`

## Responsibilities

- Accept raw SQL passed directly by the user
- Accept raw SQL loaded from file paths
- Normalize and strip trailing semicolons
- Attach optional schema and index context
- Produce a stable `query_id` for each workload item

## Input

The default implementation consumes `PipelineRequest` from `pipeline/models.py`.

Important fields:

- `raw_queries`
- `query_files`
- `schema_text`
- `schema_file`
- `index_text`
- `index_file`
- `engine`

## Output

The layer emits a list of `WorkloadItem`.

Each `WorkloadItem` contains:

- `query_id`
- `raw_query`
- `engine`
- `source_path`
- `schema_text`
- `index_text`
- `metadata`

## Current Behavior

- If no query string or query file is provided, the layer raises an error
- If a query file is provided, the file is read immediately
- Schema and index context may come from either direct text or external files

## Design Notes

- This layer is intentionally simple and deterministic
- It does not inspect SQL structure or query semantics
- It only prepares inputs for later layers

## TODO

- Add workload directory support for batch benchmark runs such as TPC-DS query folders
- Add metadata extraction for benchmark query ids, workload families, and dataset scale factors
- Add optional schema/index autodiscovery from database metadata
- Add richer query package metadata, for example source benchmark name and query tags
