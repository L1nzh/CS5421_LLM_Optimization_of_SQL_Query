# Layer 2: Prompt Builder

## Purpose

Layer 2 constructs the prompt package used by the LLM generation layer.

It should remain independent from model execution so prompt strategy can be tested separately from model choice and decoding behavior.

## Default Implementation

- Module: `layer2/prompt_builder.py`
- Class: `DefaultPromptBuilderLayer`

## Responsibilities

- Build controlled prompt text from a raw SQL query
- Inject schema and index context when available
- Encode prompt strategy choices
- Encode reasoning mode choices
- Support both single-pass and two-pass prompting

## Supported Default Modes

### Prompt strategy

The default implementation currently accepts the following ids:

- `P0_BASE`
- `P1_ENGINE`
- `P4_RULES`

These are lightweight defaults aligned with the broader architecture.

### Reasoning mode

The default implementation currently supports:

- `DIRECT`
- `COT_DELIM`
- `TWO_PASS`

## Input

- `WorkloadItem`
- `PipelineRequest`

Important fields:

- `raw_query`
- `engine`
- `schema_text`
- `index_text`
- `prompt_strategy`
- `reasoning_mode`
- `model`
- `candidate_count`

## Output

The layer emits `PromptPackage`.

Important fields:

- `prompt_text`
- `stage1_prompt_text`
- `stage2_prompt_template`
- `prompt_strategy`
- `reasoning_mode`
- `candidate_count`

## Current Behavior

- `DIRECT` builds a one-shot prompt and expects only final SQL
- `COT_DELIM` asks the model to place final SQL inside `<SQL>...</SQL>`
- `TWO_PASS` builds:
  - a first-stage planning prompt
  - a second-stage apply-plan template

## Design Notes

- This layer does not call the model
- This layer is reusable for ablation and controlled prompt experiments
- Prompt structure is isolated so it can evolve without touching ranking, validation, or benchmarking

## TODO

- Fully unify this implementation with the prompt variants already present in `benchmark/postgres/ablation_experiments.py`
- Add explicit support for `P2_SCHEMA_MIN` and `P3_SCHEMA_STATS` as first-class reusable prompt builder paths
- Add richer engine-specific prompt templates
- Add prompt artifact serialization for reproducibility
