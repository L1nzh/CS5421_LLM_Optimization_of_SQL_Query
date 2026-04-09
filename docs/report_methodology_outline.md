# Report Methodology Outline

This document is intended as a drafting aid for the report.

It does not try to provide polished prose. Instead, it lists the content that should be covered in the report's `Methodology` section and the supporting details that help readers understand the design, implementation, and evaluation protocol.

## 1. Methodology Goals

The methodology section should help the reader understand:

- what problem the system solves
- what the system takes as input and produces as output
- how the end-to-end pipeline is organized
- how semantic correctness is enforced
- how performance is evaluated
- what benchmark workload is used
- how experiments are structured for fair comparison
- how the implementation supports reproducibility

## 2. Problem Definition

Suggested content:

- The system studies `LLM-based SQL query optimization`.
- Input:
  - a raw SQL query
  - optional schema and index context
  - a configured prompt strategy, reasoning strategy, and model
- Output:
  - one or more candidate optimized SQL queries
  - validation outcome for semantic equivalence
  - benchmark metrics for valid candidates
  - a ranked final recommendation, or no recommendation if no valid candidate exists

Points to mention:

- The goal is not text generation quality alone.
- The goal is to produce SQL rewrites that are:
  - semantically equivalent to the original query
  - executable on the target engine
  - potentially faster and/or more memory efficient
- This is framed as a `research evaluation pipeline`, not just a query rewriting tool.

## 3. System Overview

Suggested content:

- Present the system as an 8-layer pipeline.
- Explain that each layer is intentionally isolated so it can be tested independently and replaced without tightly coupling the rest of the system.
- Emphasize clean separation between:
  - workload preparation
  - prompt construction
  - model invocation
  - output normalization
  - semantic validation
  - physical benchmarking
  - ranking
  - reporting/analysis

Suggested figure:

```mermaid
flowchart LR
    A["Layer 1: Workload Preparation"] --> B["Layer 2: Prompt Builder"]
    B --> C["Layer 3: Candidate Generation"]
    C --> D["Layer 4: Candidate Normalization"]
    D --> E["Layer 5: Validation Gate"]
    E --> F["Layer 6: Benchmark"]
    F --> G["Layer 7: Ranking"]
    G --> H["Layer 8: Analysis / Reporting"]
```

Suggested per-layer summary:

- `Layer 1`: reads raw queries from files or direct strings and attaches schema/index context
- `Layer 2`: constructs the prompt according to prompt and reasoning strategy
- `Layer 3`: calls the selected LLM backend and generates candidate rewrites
- `Layer 4`: extracts executable SQL from model output
- `Layer 5`: checks semantic equivalence against the baseline query
- `Layer 6`: benchmarks valid candidates against the baseline query
- `Layer 7`: scores and ranks candidates using speed and memory signals
- `Layer 8`: produces structured outputs for analysis and reporting

Useful repo references:

- [llm_arch_proposal.md](llm_arch_proposal.md)
- [pipeline/sql_optimization_pipeline.py](sql_optimization_pipeline.py)
- [layers_index.md](layers_index.md)

## 4. Dataset and Workload

Suggested content:

- The evaluation workload is based on `TPC-DS`.
- The test set consists of `99 SQL queries`.
- In the current experiment setup, the testing queries are stored under:
  - `datasets/testing_query`
- The target database is:
  - PostgreSQL
  - TPC-DS SF=1 dataset
- Example DSN used during experiments:
  - `postgresql://postgres:abcd1234@localhost:5432/tpcds_sf1`

Why TPC-DS should be explained:

- It is a widely used decision-support benchmark.
- It contains complex analytical SQL with:
  - multi-table joins
  - nested subqueries
  - aggregation
  - filtering
  - ordering
- It is suitable for evaluating whether LLM-generated rewrites preserve semantics while improving execution efficiency.

Useful repo references:

- [use_of_tcp_ds.md](use_of_tcp_ds.md)
- [experimental_setup.md](experimental_setup.md)

## 5. Prompting and Model Search Space

Suggested content:

- Experiments vary three main factors:
  - prompt strategy `P`
  - reasoning strategy `R`
  - model `M`

Current active prompt strategies:

- `P0`: base prompt with minimal instructions
- `P1`: engine-aware prompt
- `P2`: minimal schema-aware prompt
- `P3`: richer schema/statistics-aware prompt

Current reasoning strategies:

- `R0`: direct generation
- `R1`: chain-of-thought style prompting with explicit SQL extraction tags
- `R2`: two-pass prompting with a planning stage followed by SQL generation

Current model families:

- primary:
  - `gpt-5`
  - `gpt-5-mini`
  - `gpt-5-nano`
- optional extension:
  - `gpt-5.4`
  - `gpt-5.4-mini`
  - `gpt-5.4-nano`
- supported alternative provider integration:
  - MiniMax models

What to explain:

- These factors are studied independently as controllable experimental variables.
- The goal is to understand not only whether an LLM can optimize SQL, but which prompt configuration and model family work best.
- The search space is designed to balance experimental coverage and cost.

Useful repo references:

- [experimental_setup.md](experimental_setup.md)
- [layer2_prompt_builder.md](layer2_prompt_builder.md)
- [layer3_candidate_generation.md](layer3_candidate_generation.md)

## 6. Candidate Generation and Normalization

Suggested content:

- Layer 3 generates multiple candidate rewrites for each baseline query.
- The current experiment configuration uses:
  - `3 candidate queries per run`
- Model output may contain extra text, reasoning traces, or formatting artifacts.
- Therefore Layer 4 normalizes model output into executable SQL candidates.

What to explain:

- Candidate generation is stochastic and may return:
  - valid SQL
  - non-SQL text
  - malformed SQL
  - semantically unsafe rewrites
- The normalization stage is necessary before validation.
- Invalid or unparseable candidates are not benchmarked as successful rewrites.

Useful repo references:

- [layer3_candidate_generation.md](layer3_candidate_generation.md)
- [layer4_candidate_normalization.md](layer4_candidate_normalization.md)

## 7. Semantic Correctness Validation

This is a core part of the methodology and should be written clearly.

Suggested content:

- A generated SQL query is accepted only if it is semantically equivalent to the baseline query.
- Semantic correctness is enforced by executing both the baseline query and the candidate query and comparing their result sets.
- This prevents the system from selecting a faster query that changes the answer.

Important behaviors to describe:

- support for ordered and unordered comparison
- support for duplicate rows through multiset comparison
- handling of `NULL`
- handling of floating-point tolerance
- column-structure checking
- normalization of values before comparison

Important scalability detail:

- For large TPC-DS outputs, the system supports `hash-based` validation with streaming execution.
- This avoids loading large full result sets into memory when comparing baseline and candidate results.
- The hash strategy is intended as a practical large-scale validation mechanism for benchmarking workloads.

Important caveat to mention carefully:

- hash-based comparison is a deterministic fingerprinting approach, not a formal proof of equality
- it is used because it is much more practical for large outputs than fully materializing both result sets in memory

Error handling to mention:

- if the baseline query fails, the run is marked invalid
- if a candidate query fails to execute, it is rejected but the pipeline continues
- query failures do not crash the full evaluation pipeline

Useful repo references:

- [validation_flow.md](validation_flow.md)
- [validator_design.md](validator_design.md)
- [layer5_validation_gate.md](layer5_validation_gate.md)

## 8. Performance Evaluation Protocol

Even though the final metrics are not ready yet, the methodology section should still explain how performance will be measured.

Suggested content:

- Only semantically valid candidates proceed to benchmarking.
- Benchmarking is conducted on PostgreSQL using execution-plan-based instrumentation.
- Metrics are extracted using `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`.

What should be stated explicitly:

- baseline and candidate queries are benchmarked under the same database engine
- each benchmark is repeated multiple times to reduce noise
- the current setup uses `5 execution trials per query`
- repeated measurements are aggregated for later comparison

Metrics to describe:

- execution time
- planning time
- speedup relative to the baseline query
- buffer statistics
- memory-related score derived from buffer usage
- success/failure status of execution

Why this matters:

- latency alone is not enough for analytical workloads
- memory and buffer behavior also affect query quality and system efficiency

Useful repo references:

- [layer6_benchmark.md](layer6_benchmark.md)
- [layer7_ranking.md](layer7_ranking.md)

## 9. Candidate Ranking and Final Selection

Suggested content:

- After validation and benchmarking, valid candidates are ranked.
- Ranking is not based solely on speedup.
- The current implementation combines:
  - speedup
  - memory score
- The highest-ranked valid candidate is selected as the optimized query.
- If no valid candidate exists, the system returns no optimized query.

What to emphasize:

- the selection mechanism is conservative
- correctness is a hard gate
- performance is only considered after correctness is established

## 10. Experimental Procedure

This subsection should describe how the experiments are actually run.

Current agreed setup:

- Phase 1:
  - randomly sample `10` queries from the `99` TPC-DS queries
  - evaluate all `(P, R, M)` combinations
  - use `5 execution trials per query`
  - choose the best configuration based on correctness and benchmark outcomes
- Phase 2:
  - randomly sample `30` queries from the `99` TPC-DS queries
  - run the best configuration from Phase 1
  - use `5 execution trials per query`
  - compare optimized candidates against the baseline queries

What to explain:

- Phase 1 is a controlled search over prompt, reasoning, and model combinations
- Phase 2 tests whether the best configuration generalizes to a larger query subset
- random sampling should be described as deterministic when driven by fixed seeds

Useful repo references:

- [experimental_setup.md](experimental_setup.md)
- [experiments/runner.py](experiments/runner.py)

## 11. Reproducibility and Artifact Collection

Suggested content:

- The system persists structured artifacts for later analysis.
- Outputs are written to:
  - `datasets/artifacts/candidate_combo`
  - `datasets/artifacts/fullset_combo`

What the report can mention:

- run configuration
- per-query results
- combo summaries
- selected best combo
- prompts
- raw model outputs
- normalized SQL
- validation reports
- benchmark reports

Why this matters:

- supports auditability
- supports later reporting and failure analysis
- allows recomputation of rankings without rerunning the full experiment

## 12. Implementation Environment

Suggested content:

- implementation language:
  - Python 3.10+
- target evaluation database:
  - PostgreSQL
- validation layer is designed with a database adapter abstraction so it is not tightly coupled to PostgreSQL
- experiment execution may use cloud infrastructure and containerized database deployment

Optional details if needed:

- CPU / RAM of the experiment host
- operating system
- Dockerized TPC-DS PostgreSQL image
- API providers used for LLM inference

## 13. Threats to Validity / Limitations

This is useful even in the methodology section or as a transition into evaluation.

Points to consider:

- benchmark results may vary due to caching and system noise
- LLM outputs are stochastic
- phase-based sampling uses subsets rather than the full 99-query set
- hash-based validation is scalable but fingerprint-based
- a query that is valid on PostgreSQL may not be portable to another engine
- prompt effectiveness may differ across model families

## 14. Suggested Tables for the Final Report

These are useful to mention now so the future report drafting is easier.

### Table A: System pipeline

Suggested columns:

- Layer
- Name
- Input
- Output
- Responsibility

### Table B: Experimental variables

Suggested columns:

- Variable
- Values
- Description

Example:

- Prompt strategy: `P0`, `P1`, `P2`, `P3`
- Reasoning strategy: `R0`, `R1`, `R2`
- Model: `gpt-5`, `gpt-5-mini`, `gpt-5-nano`

### Table C: Evaluation metrics

Suggested columns:

- Metric
- Meaning
- Used for validation or benchmarking

Examples:

- semantic equivalence
- execution time
- planning time
- speedup
- buffer hits/reads
- memory score

## 15. Suggested Inputs for LLM-Based Report Drafting

If this outline is later fed into another LLM for prose generation, include:

- this file
- [experimental_setup.md](experimental_setup.md)
- [validation_flow.md](validation_flow.md)
- [layer6_benchmark.md](layer6_benchmark.md)
- [layer7_ranking.md](layer7_ranking.md)
- [use_of_tcp_ds.md](use_of_tcp_ds.md)

Suggested instruction to that LLM:

- convert the outline into a formal academic `Methodology` section
- preserve technical correctness
- avoid inventing metrics that are not part of the implementation
- keep the section aligned with PostgreSQL + TPC-DS + semantic validation + two-phase experiment design
