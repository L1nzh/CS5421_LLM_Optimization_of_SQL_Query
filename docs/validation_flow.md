Given your **LLM SQL optimizer research project**, the **result set validation layer** is actually a **critical correctness gate** before performance evaluation (e.g., latency, cost, query plan comparison). In academic benchmarking systems (including those used with TPC-DS), this stage is often called **semantic equivalence verification**.

Your validator must answer one question:

> **Does the optimized SQL produce the same logical result as the original query?**

But there are several complexities:

* row ordering differences
* floating-point precision differences
* duplicates vs distinct
* NULL handling
* LIMIT / ORDER BY effects
* non-deterministic functions (e.g., `random()`, `now()`)

Therefore the validator must be **deterministic, database-agnostic, and extensible**.

Below is a **research-grade architecture** suitable for your project.

---

# 1. High-Level Validation Flow

The validation pipeline should look like this:

```
                Raw Query
                    │
                    ▼
           Execute Baseline Query
                    │
                    ▼
              Baseline Result
                    │
                    ▼
     ┌────────────────────────────────┐
     │  Optimized Query Candidates     │
     │   Q1, Q2, Q3, ... QN            │
     └────────────────────────────────┘
            │      │       │
            ▼      ▼       ▼

       Execute Query Candidates
            │      │       │
            ▼      ▼       ▼

        Resultset Comparator
              │
              ▼
       Semantic Equivalence Check
              │
              ▼
   ┌───────────────────────────────┐
   │ Valid Queries (Correct SQL)   │
   │ Invalid Queries (Rejected)    │
   └───────────────────────────────┘
              │
              ▼
       Pass to Evaluation Stage
       (Latency / Plan / Cost)
```

Key stages:

1. **Baseline execution**
2. **Candidate execution**
3. **Result normalization**
4. **Result comparison**
5. **Correctness classification**

---

# 2. Core Design Principles

For this module we must enforce:

### 1️⃣ Database abstraction

Your project may later test:

* PostgreSQL
* DuckDB
* SQLite
* MySQL

So **never couple logic to PostgreSQL driver**.

Use an **Adapter Pattern**.

---

### 2️⃣ Deterministic comparison

We normalize results before comparing.

Examples:

| Problem              | Solution        |
| -------------------- | --------------- |
| Row order difference | Sort rows       |
| Float precision      | Round           |
| Column name mismatch | Use index       |
| NULL vs None         | Normalize       |
| Duplicate rows       | Count frequency |

---

### 3️⃣ Memory safety

Large query results must not crash memory.

Use:

* streaming
* hashing
* chunk comparison

---

### 4️⃣ Multiple comparison strategies

Different queries require different comparison types:

| Query Type      | Strategy          |
| --------------- | ----------------- |
| SELECT          | result equality   |
| SELECT DISTINCT | set equality      |
| Aggregation     | tolerance compare |
| ORDER BY        | strict equality   |

---

# 3. Proposed Project Structure

Here is the **recommended project layout**.

```
llm_sql_optimizer/
│
├── config/
│   └── settings.py
│
├── db/
│   ├── database.py
│   ├── adapter.py
│   ├── postgres_adapter.py
│   └── connection_pool.py
│
├── execution/
│   ├── query_executor.py
│   └── execution_result.py
│
├── validator/
│   ├── result_validator.py
│   ├── result_normalizer.py
│   ├── result_comparator.py
│   └── comparison_strategies.py
│
├── models/
│   ├── query_candidate.py
│   └── validation_result.py
│
├── utils/
│   ├── dataframe_utils.py
│   └── hashing.py
│
├── pipeline/
│   └── validation_pipeline.py
│
└── main.py
```

---

# 4. Core Components

We divide the system into **5 subsystems**.

---

# 4.1 Database Abstraction Layer

Goal:

```
LLM Optimizer
     │
     ▼
 Query Executor
     │
     ▼
 Database Adapter
     │
     ▼
 PostgreSQL / other DB
```

### Interface

```python
class DatabaseAdapter(ABC):

    @abstractmethod
    def execute_query(self, query: str) -> QueryExecutionResult:
        pass

    @abstractmethod
    def get_schema(self):
        pass

    @abstractmethod
    def close(self):
        pass
```

Concrete:

```
PostgresAdapter(DatabaseAdapter)
```

Internally uses:

```
psycopg2 / asyncpg
```

---

# 4.2 Query Execution Layer

Responsible for:

* executing SQL
* capturing errors
* capturing runtime
* returning structured results

Output object:

```
QueryExecutionResult
```

Example:

```python
@dataclass
class QueryExecutionResult:

    columns: list[str]
    rows: list[tuple]

    execution_time: float

    success: bool
    error_message: Optional[str]
```

---

# 4.3 Result Normalization Layer

Before comparison, normalize result sets.

Normalization steps:

```
Raw result
   │
   ▼
Convert to dataframe
   │
   ▼
Normalize values
   │
   ▼
Sort rows
   │
   ▼
Standardized dataframe
```

Normalization rules:

| Rule                    | Example            |
| ----------------------- | ------------------ |
| convert Decimal → float | PostgreSQL numeric |
| round float             | 1e-6               |
| normalize NULL          | None               |
| trim strings            | optional           |
| sort rows               | deterministic      |

---

# 4.4 Result Comparator

The comparator determines **semantic equality**.

Three strategies:

### Strategy 1 — Exact equality

```
DataFrame.equals()
```

Used for:

```
SELECT
```

---

### Strategy 2 — Set equality

```
multiset comparison
```

Handles duplicates.

Example:

```
SELECT DISTINCT
```

---

### Strategy 3 — Floating tolerance

Use:

```
abs(a-b) < epsilon
```

Useful for:

```
AVG
SUM
FLOAT operations
```

---

# 4.5 Result Validator

This is the **main component**.

Inputs:

```
raw_query
optimized_queries[]
```

Output:

```
validation_result
```

Example output:

```
QueryCandidateResult:

query: ...
valid: True
reason: None
execution_time: 0.23
```

---

# 5. Validation Pipeline

Main orchestrator:

```
ValidationPipeline
```

Flow:

```
1 run baseline query
2 normalize baseline result

3 for each optimized query
      execute query
      normalize result
      compare with baseline

4 classify query
```

---

### Pipeline pseudocode

```python
baseline = executor.execute(raw_query)

baseline_df = normalizer.normalize(baseline)

for candidate in optimized_queries:

    result = executor.execute(candidate)

    if not result.success:
        mark_invalid()

    candidate_df = normalizer.normalize(result)

    equal = comparator.compare(
        baseline_df,
        candidate_df
    )

    if equal:
        mark_valid()
    else:
        mark_invalid()
```

---

# 6. Handling Edge Cases

Your validator must detect **dangerous SQL rewrites**.

Examples:

---

### Case 1 — Missing ORDER BY

Raw:

```
SELECT * FROM orders ORDER BY created_at
```

Optimized:

```
SELECT * FROM orders
```

Results same but order different.

Solution:

```
Ignore row order unless ORDER BY exists
```

---

### Case 2 — LIMIT pushdown

Raw:

```
SELECT * FROM table LIMIT 10
```

Optimized incorrectly modifies.

Solution:

```
Strict comparison
```

---

### Case 3 — Floating aggregation

```
AVG(price)
```

Use tolerance:

```
1e-6
```

---

### Case 4 — NULL comparison

SQL:

```
NULL != NULL
```

But validator must treat them as equal.

---

# 7. Result Validation Output

Final output format:

```python
@dataclass
class ValidationResult:

    raw_query: str

    baseline_execution_time: float

    candidates: list[CandidateValidationResult]
```

Candidate result:

```python
@dataclass
class CandidateValidationResult:

    query: str

    is_valid: bool

    reason: Optional[str]

    execution_time: float
```

---

# 8. Example Output

Example JSON:

```json
{
  "raw_query": "...",
  "baseline_time": 0.42,
  "candidates": [
    {
      "query": "SELECT ...",
      "valid": true,
      "execution_time": 0.21
    },
    {
      "query": "SELECT ...",
      "valid": false,
      "reason": "Result mismatch"
    }
  ]
}
```

---

# 9. Performance Optimization (Important)

When queries return **millions of rows**, comparison must avoid full memory load.

Techniques:

### Row hashing

```
hash(row)
```

Compare hash frequency.

---

### Streaming compare

Compare chunks:

```
fetchmany(1000)
```

---

### Aggregated checksum

```
SUM(hash(row))
COUNT(*)
```

---

This is exactly how large benchmark validators work.

---

# 10. Suggested Python Libraries

| Library               | Purpose           |
| --------------------- | ----------------- |
| psycopg2 / asyncpg    | PostgreSQL driver |
| pandas                | result comparison |
| dataclasses           | structured models |
| sqlalchemy (optional) | abstraction       |

---

# 11. Integration with Your LLM Optimizer

Your full optimizer pipeline should look like:

```
User Query
    │
    ▼
Prompt Engineering
    │
    ▼
LLM Generates N Queries
    │
    ▼
Result Validation  ← (this module)
    │
    ▼
Valid Queries Only
    │
    ▼
Performance Evaluation
    │
    ▼
Best Query Selection
```

---

# 12. Next Step

If you want, I can now provide the **full Python implementation**, including:

* Database adapter abstraction
* PostgreSQL adapter
* Query executor
* Result normalizer
* Result comparator
* Validation pipeline
* Large result set hashing strategy
* Complete runnable code (~600–800 lines)

This will be **research-grade and ready for your project.**
