# Validation Flow And Comparison Strategies

This document explains how the result-set validator works in the current codebase, with special focus on the `hash` comparison strategy for large query outputs such as TPC-DS.

## End-To-End Flow

The validator follows this pipeline:

```text
Raw Query
  -> execute baseline
  -> normalize baseline result
  -> for each candidate:
       execute candidate
       normalize candidate result
       compare against baseline
  -> emit ValidationReport
```

In code:

- Query execution starts in `execution/query_executor.py`
- Database access is abstracted in `db/adapter.py`
- PostgreSQL streaming support lives in `db/postgres_adapter.py`
- Normalization lives in `validator/result_normalizer.py`
- Comparison lives in `validator/result_comparator.py`
- Streaming hash logic lives in `validator/result_hasher.py`
- Orchestration lives in `validator/validation_pipeline.py`

## Normalization

Before any comparison, values are normalized into a deterministic representation.

The normalizer handles:

- `float`: rounded using the configured precision derived from `float_tolerance`
- `Decimal`: quantized to the same effective precision
- `NULL`: kept as `None`
- `datetime`, `date`, `time`: converted to ISO strings
- `bytes`: decoded using the configured encoding
- `dict` and JSON-like structures: serialized into canonical JSON
- `str`: newline-normalized, optionally trimmed
- nested sequences: normalized recursively

Column names are also normalized before comparison.

This matters because SQL-equivalent queries may still differ in raw driver output formatting.

## Comparison Strategies

### `exact_ordered`

Use this when row order is part of the expected semantics.

Behavior:

- Columns must match exactly
- Rows must match exactly
- Row order must be identical

Good fit:

- queries with meaningful `ORDER BY`
- window queries where order is intentionally part of the output contract

Tradeoff:

- strict but memory-heavy, because both full result sets are materialized

### `exact_unordered`

Use this when row order is not semantically important.

Behavior:

- Columns must match exactly
- Rows are normalized and then sorted into a deterministic order
- Sorted baseline rows are compared to sorted candidate rows

Good fit:

- ordinary `SELECT` queries where row order is unspecified

Tradeoff:

- exact comparison, but still materializes and sorts both full result sets in memory

### `multiset`

Use this when duplicate row frequency matters and order does not.

Behavior:

- Columns must match exactly
- Rows are counted using `collections.Counter`
- Row value and row frequency must both match

Good fit:

- queries that may return duplicates and where duplicate counts are semantically meaningful

Tradeoff:

- exact duplicate-sensitive comparison, but still memory-heavy for very large outputs

### `hash`

Use this when result sets are too large to compare comfortably in memory and you want a compact fingerprint instead.

Behavior:

- Columns are normalized first
- Rows are streamed from the database in batches
- Each row is normalized before hashing
- The validator computes a compact digest and compares digests instead of storing all rows

Good fit:

- TPC-DS-scale result sets
- large benchmark runs where memory pressure matters

Tradeoff:

- far more memory efficient
- collision-resistant, but not mathematically collision-free

## How Streaming Hash Comparison Works

### 1. Streaming rows from the adapter

For PostgreSQL, `db/postgres_adapter.py` uses `cursor.fetchmany(batch_size)` to avoid loading the entire result set at once.

Conceptually:

```text
execute query
open cursor
fetch batch of rows
yield rows one by one
repeat until exhausted
close cursor
```

The batch size is controlled by `stream_batch_size`.

### 2. Normalizing each row

Every streamed row is passed through the same `ResultNormalizer` logic used by the non-hash strategies.

That means the hash path still respects:

- float tolerance
- decimal precision
- string normalization
- datetime normalization
- bytes decoding
- JSON canonicalization

So the digest represents the normalized result, not the raw driver payload.

### 3. Building the digest

There are two hash modes, controlled by `preserve_row_order`.

#### Ordered hash mode

If `preserve_row_order=True`:

- normalize each row
- serialize it deterministically
- append it into a rolling SHA-256 digest
- include normalized columns and row count in the final payload

This behaves like an ordered fingerprint of the full result set.

Conceptually:

```text
digest = sha256()
for row in rows:
    digest.update(serialize(normalize(row)))
```

If the same rows appear in a different order, the digest changes.

#### Unordered hash mode

If `preserve_row_order=False`:

- normalize each row
- hash each normalized row individually
- combine the row hashes using order-insensitive aggregates

The current implementation uses:

- sum of row hashes
- xor of row hashes
- sum of squared row hashes
- row count

Those aggregates are then hashed again into the final digest.

Conceptually:

```text
for row in rows:
    h = sha256(serialize(normalize(row)))
    sum_hash += h
    xor_hash ^= h
    square_sum += h*h
```

This gives an order-insensitive multiset-style fingerprint:

- reordering rows does not change the digest
- changing row values does change the digest
- duplicate rows affect the aggregates, so frequency matters

### 4. Comparing baseline vs candidate

The hash comparator checks:

1. normalized columns
2. row count
3. final digest

If all three match, the candidate is marked equivalent.

## Why Hashing Helps For TPC-DS

TPC-DS queries can return very large result sets, and exact in-memory comparison becomes expensive because it requires:

- storing all baseline rows
- storing all candidate rows
- often sorting or counting them

The streaming hash strategy reduces memory usage because it only keeps:

- the current batch or row
- a few running aggregate values
- the final digest

So memory stays roughly constant even when row count grows very large.

## Important Limitation

Hash comparison is a strong fingerprint, not a proof of equivalence.

That means:

- it is deterministic
- it is highly collision-resistant
- but it is not impossible for two different results to produce the same digest

For most benchmark validation workflows, this is a practical tradeoff. If you later need exact unordered comparison at very large scale, the next step would be an external-sort or disk-backed comparison strategy.

## Strategy Selection Guidance

Use:

- `exact_ordered` when order matters
- `exact_unordered` when order does not matter and results are manageable in memory
- `multiset` when duplicate counts matter and results are manageable in memory
- `hash` when results are large and you need bounded memory
