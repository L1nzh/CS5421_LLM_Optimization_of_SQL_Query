# Layer 5: Validation Gate

## Purpose

Layer 5 is the correctness gate of the architecture.

It determines whether a generated candidate preserves the semantics of the original query before any benchmark ranking is trusted.

## Default Implementation

- Layer adapter: `layer5/validation_gate.py`
- Existing validator core: `validator/validation_pipeline.py`

## Responsibilities

- Receive normalized candidate SQL
- Reject candidates that failed normalization
- Run baseline query execution
- Run candidate query execution
- Compare result sets using the configured strategy

## Validation Strategies

The current validator supports:

- `exact_ordered`
- `exact_unordered`
- `multiset`
- `hash`

The `hash` strategy supports streaming comparison for large outputs.

## Input

- `raw_query`
- list of `NormalizedCandidate`

## Output

The layer emits `ValidationReport`.

Important report fields:

- `baseline_execution_time_ms`
- `baseline_row_count`
- `baseline_columns`
- `results`
- `baseline_error_message`

## Current Behavior

- If no real database executor is configured, this layer falls back to placeholder validation behavior
- Candidates that fail normalization are marked invalid with their normalization error
- Executable candidates are passed to the existing validator pipeline

## Design Notes

- This layer is a thin adapter over the validator so the validator itself remains independently testable
- The adapter preserves candidate ordering so later ranking stays stable

## TODO

- Add syntax-only precheck before full semantic validation
- Add failure taxonomy fields beyond plain reason strings
- Add configurable safety rules such as rejecting unsupported hints or forbidden rewrites
- Add richer mapping between candidate ids and validation results as a first-class model
