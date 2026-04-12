# LLM SQL Optimizer Research Pipeline

This repository contains a research-oriented pipeline for optimizing SQL queries with Large Language Models (LLMs) and evaluating the optimized queries in a controlled, reproducible way.

The project is built around two goals:

1. Generate candidate SQL rewrites from different prompt, reasoning, and model combinations.
2. Verify that the rewritten query is semantically correct and compare its performance against the baseline query.

The current workflow supports experiment execution, semantic validation, benchmarking, ranking, artifact persistence, and post-hoc analysis for report writing.

## Project Scope

At a high level, the project implements an 8-layer pipeline:

1. Workload preparation
2. Prompt construction
3. LLM candidate generation
4. Candidate normalization
5. Semantic validation
6. Benchmarking
7. Ranking
8. Analysis

This separation is intentional. Each layer is designed to stay independently testable and replaceable, which makes the system easier to debug, benchmark, and extend.

Typical use cases in this repository are:

- run prompt / reasoning / model experiments on a query workload
- validate optimized SQL against the baseline result set
- benchmark valid candidates on PostgreSQL
- rank candidates by performance signals
- generate analysis summaries and plots for the final report

## Core Packages

The main code is organized into the following packages:

- `layer1` to `layer8`: layer-specific implementations for the end-to-end optimization pipeline
- `pipeline`: orchestration contracts and pipeline-level models
- `experiments`: experiment runner, combo handling, persistence, and experiment-specific utilities
- `validator`: result normalization, comparison strategies, hashing, and validation pipeline
- `db`: database abstraction layer and PostgreSQL adapter
- `execution`: query execution helpers
- `cli`: command-line entrypoints
- `scripts`: local analysis and plotting utilities
- `tests`: unit and integration-style tests

## Installation

The project targets Python 3.10+.

Create a virtual environment and install the package:

```bash
python3 -m venv .venv
source .venv/bin/activate
./venv/bin/pip install -U pip
./venv/bin/pip install -e .
```

For test tooling:

```bash
./venv/bin/pip install -e .[dev]
```

Core runtime dependencies are defined in [pyproject.toml](/Users/dex/Documents/python-workspace/llm_query_optimizer/pyproject.toml).

## Model Provider Configuration

Layer 3 supports multiple providers behind a shared generation interface.

Supported families in the current codebase include:

- OpenAI GPT models
- Ark / Doubao models
- MiniMax models
- local OpenAI-compatible chat-completions models

Common environment variables:

```bash
export OPENAI_API_KEY="your_openai_key"
export ARK_API_KEY="your_ark_key"
export MINIMAX_API_KEY="your_minimax_key"
export LOCAL_LLM_BASE_URL="http://100.64.0.45:11434/v1"
export LOCAL_LLM_API_KEY="local"
```

You only need to set the variables required by the model family you plan to use.

## Running the Experiment

The main entrypoint for the research workflow is the experiment CLI:

```bash
./venv/bin/python -m cli.experiment_cli \
  --dsn "postgresql://postgres:abcd1234@localhost:5432/tpcds_sf1" \
  --query-dir "datasets/testing_query" \
  --schema-file "benchmark/postgres/tpcds/schema.sql" \
  --artifacts-root "datasets/artifacts" \
  --phase all \
  --comparison-strategy hash
```

Important arguments:

- `--dsn`: PostgreSQL DSN used for validation and benchmarking
- `--query-dir`: directory containing SQL workload files
- `--schema-file`: schema context injected into prompts
- `--artifacts-root`: root directory for persisted experiment outputs
- `--phase`: `phase1`, `phase2`, or `all`
- `--comparison-strategy`: validation strategy, including `hash` for large-result workloads
- `--include-gpt54`: optionally include the GPT-5.4 family in the Phase 1 combo search
- `--include-local`: optionally include a local OpenAI-compatible model in the Phase 1 combo search
- `--local-model`: local model id when `--include-local` is enabled

To inspect the full CLI surface:

```bash
./venv/bin/python -m cli.experiment_cli --help
```

### Experiment Phases

The experiment workflow is split into two phases:

- `phase1`: evaluate combinations of prompt strategy, reasoning strategy, and model on a sampled subset to identify the best-performing combo
- `phase2`: run the selected best combo on a larger sampled subset and persist the detailed outputs for deeper analysis

The exact setup and sampling rationale are documented in [docs/experimental_setup.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/experimental_setup.md).

## Semantic Validation

The system does not assume that a faster query is acceptable by default. Every generated candidate can be checked against the baseline query through the validation stack.

Supported validation strategies include:

- `exact_ordered`
- `exact_unordered`
- `multiset`
- `hash`

The `hash` strategy is especially useful for large workloads such as TPC-DS because it supports streaming comparison and avoids materializing large result sets fully in memory.

More details are in:

- [docs/validator_design.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/validator_design.md)
- [docs/validation_flow.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/validation_flow.md)

## Generating Analysis Data

After the experiment has finished, you can generate local analysis outputs from the saved artifacts:

```bash
./venv/bin/python scripts/analyze_experiment_results.py
```

This script produces structured analysis outputs such as:

- JSON summaries
- CSV summaries
- per-query micro-analysis tables
- macro-level combo comparisons

By default, the outputs are written to:

- `datasets/artifacts/findings_analysis`

## Generating Figures

To generate report-friendly figures from the analysis output:

```bash
./venv/bin/python scripts/plot_experiment_findings.py
```

By default, the figures are written to:

- `datasets/artifacts/findings_figures`

These figures are intended to support the report's methodology, performance, and findings sections.

## Data and Artifact Layout

The repository keeps workload inputs and generated outputs under `datasets/`.

Important paths:

- `datasets/testing_query`: workload SQL files used in the experiment
- `datasets/artifacts/candidate_combo`: Phase 1 outputs for combo exploration
- `datasets/artifacts/fullset_combo`: Phase 2 outputs for the best combo
- `datasets/artifacts/findings_analysis`: derived analysis outputs
- `datasets/artifacts/findings_figures`: generated plots and report figures

In practice, the artifact folders contain run-stamped subdirectories, persisted summaries, and per-query traces that can be reused without rerunning the expensive LLM calls.

## Documentation

Project documentation is stored under `docs/`.

Recommended starting points:

- [docs/llm_arch_proposal.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/llm_arch_proposal.md): overall architecture proposal
- [docs/layers_index.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/layers_index.md): entrypoint to layer-by-layer documentation
- [docs/layer1_workload_preparation.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/layer1_workload_preparation.md) to [docs/layer8_analysis.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/layer8_analysis.md): detailed layer design
- [docs/experimental_setup.md](/Users/dex/Documents/python-workspace/llm_query_optimizer/docs/experimental_setup.md): experiment methodology and run design

## Other Useful CLI Entry Points

Besides the experiment runner, the repository exposes:

```bash
llm-sql-validator
llm-sql-pipeline
llm-sql-experiment
```

These are defined in [pyproject.toml](/Users/dex/Documents/python-workspace/llm_query_optimizer/pyproject.toml) and map to:

- semantic validation CLI
- layered SQL optimization pipeline CLI
- experiment CLI

## Testing

Run the test suite with:

```bash
./venv/bin/python -m pytest
```

## Notes

- The project is currently benchmarked primarily on PostgreSQL, but the validation and execution design intentionally uses a database abstraction layer so the system can be extended to other engines later.
- The repository contains both implementation code and report-generation support code because the experiments are designed to feed directly into the project write-up.
