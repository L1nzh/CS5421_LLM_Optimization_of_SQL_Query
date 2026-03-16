# LLM SQL Validator

This project provides a result-set validation subsystem for SQL optimization research. It executes a baseline query and one or more candidate optimized queries, normalizes their outputs, and determines whether each candidate is semantically equivalent to the baseline.

The implementation is designed for PostgreSQL testing today while keeping shared infrastructure such as `db`, `execution`, `models`, and `config` reusable across the wider project.

## Features

- Database adapter abstraction with a PostgreSQL `psycopg3` implementation
- Structured execution results with timing and error capture
- Deterministic normalization for floats, decimals, `NULL`, datetimes, bytes, JSON-like values, and strings
- Configurable comparison strategies:
  - `exact_ordered`
  - `exact_unordered`
  - `multiset`
  - `hash`
- Validation pipeline that never crashes on candidate query failures
- CLI that emits JSON reports
- Unit tests for normalization, comparison, and pipeline behavior

## Project Layout

```text
cli/
config/
db/
execution/
models/
utils/
validator/
tests/
```

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## CLI Usage

```bash
python -m cli.validator_cli \
  --dsn "postgres://user:pass@localhost/dbname" \
  --raw-query "SELECT id, name FROM users" \
  --candidate-query "SELECT id, name FROM users ORDER BY id" \
  --candidate-query "SELECT name, id FROM users"
```

Optional flags:

- `--comparison-strategy exact_ordered|exact_unordered|multiset|hash`
- `--ordered`
- `--float-tolerance 1e-6`
- `--stream-batch-size 10000`
- `--trim-strings`

## Example Report

```json
{
  "raw_query": "SELECT id, name FROM users",
  "baseline_execution_time_ms": 12.3,
  "baseline_row_count": 2,
  "baseline_columns": ["id", "name"],
  "results": [
    {
      "query": "SELECT id, name FROM users ORDER BY id",
      "is_valid": true,
      "reason": "Equivalent",
      "execution_time_ms": 8.4,
      "error_message": null
    }
  ],
  "baseline_error_message": null
}
```

## Testing

```bash
pytest
```

## Extending To Other Databases

Implement `db.adapter.DatabaseAdapter` for the target engine and pass it into `QueryExecutor`. The validator, normalizer, comparator, and pipeline are database-agnostic.

## Notes

- Column structure is validated before row comparison.
- `hash` streams rows in batches and computes the fingerprint incrementally, which avoids materializing the full result set in memory.
- When `preserve_row_order=False`, `hash` uses an order-insensitive multiset fingerprint over normalized rows. This is intended for large benchmark result sets such as TPC-DS, but like any hash-based equivalence check it is collision-resistant rather than mathematically collision-free.
- Candidate query failures return `is_valid = false` with reason `Candidate execution failed`.
- Baseline query failures return a report with `baseline_error_message` and mark all candidates invalid.
