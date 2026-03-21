Your design is already strong. It has the right research spirit: **prompt layer as experimental control**, **multi-candidate generation**, and **benchmark-driven selection**.

What I would do is **refine it into a more rigorous research architecture**, because for an academic paper, the architecture must not only work, but also make it easy to answer:

- what exactly is the independent variable?
- what exactly is being measured?
- where is correctness enforced?
- where do we separate “generation” from “selection”?
- how do we prevent unfair advantage or hidden leakage?

Below I will first evaluate your design, then propose my version, then merge both into a **best-fit final architecture** for this topic.

------

# 1. Review of Your Design

## What is already very good

Your design correctly captures these important ideas:

### A. Prompt preparation is a separate stage

This is excellent, because prompt design is one of the key research variables.

### B. Chain-of-thought is explicitly included

Good for interpretability and for studying whether reasoning helps optimization.

### C. Self-consistency is included

Very suitable for SQL rewriting because one query may have several valid alternative rewrites.

### D. Benchmarking is not just latency measurement

You already included:

- result verification
- metric collection
- baseline comparison
- tool comparison
- candidate selection

That is exactly the right direction.

------

## What I would improve

There are 6 important refinements needed.

### 1. Separate “prompt construction” from “LLM inference configuration”

Right now your design mixes:

- prompt content
- CoT instruction
- self-consistency
- API call

These are related, but experimentally they should be distinct.

Because:

- **prompt template** is one variable
- **reasoning style** is another variable
- **number of candidates N** is another variable
- **model family** is another variable

These should be independently controlled.

------

### 2. Add a strict semantic validation stage before benchmarking

Your current “resultset verification” is inside performance benchmark.

I would pull it out into its own explicit module:

- syntax validation
- schema compatibility validation
- semantic equivalence validation

Because academically, a rewritten query that changes the answer is **not an optimization**.

------

### 3. Add a candidate normalization / sanitization stage

LLM outputs are messy:

- markdown fences
- explanations mixed with SQL
- multiple query variants in one response
- unsupported dialect features

You need an explicit parser/normalizer stage before execution.

------

### 4. Separate “generation” from “selection”

The architecture should show clearly:

- LLM generates candidate queries
- evaluation engine scores them
- selector chooses the best candidate

This separation is important because the LLM should not be credited with performance gains that actually came from the selection mechanism.

------

### 5. Add a failure taxonomy stage

For the paper, it is extremely useful to classify failures:

- syntax invalid
- execution error
- semantic mismatch
- slower than baseline
- timeout
- unsupported rewrite pattern

This becomes a very valuable analysis section.

------

### 6. Be careful with “iterative rerun”

This is a good idea, but in a research setting it can easily become unfair if unconstrained.

You need a rule like:

- maximum 1 or 2 refinement loops
- same budget for all models/prompts
- same token/attempt budget per query

Otherwise reviewers may say the comparison is biased.

------

# 2. My Proposed Architecture

I would structure the project into **8 pipeline layers**.

------

## Layer 1 — Workload & Metadata Preparation

### Input

- benchmark query set, e.g. TPC-DS queries
- schema DDL
- index metadata
- DB engine configuration
- optional statistics metadata

### Purpose

Prepare the context that may or may not be exposed to the LLM.

### Why this matters

This lets you test context strategies:

- no schema
- schema only
- schema + index
- schema + engine-specific notes

This becomes one experimental dimension.

------

## Layer 2 — Prompt Strategy Builder

### Input

- raw query
- optional schema context
- optional index context
- prompt template type
- reasoning mode

### Output

- finalized prompt package

### Prompt dimensions to test

- zero-shot
- few-shot
- rule-guided
- schema-aware
- engine-aware

### Reasoning dimensions

- direct answer
- CoT
- structured reasoning fields

This layer should not call the LLM yet. It just constructs controlled prompts.

------

## Layer 3 — LLM Candidate Generation

### Input

- prompt package
- model config
- decoding config
- candidate count N

### Output

- raw LLM responses
- extracted reasoning text
- extracted candidate SQLs

### Notes

This is where you vary:

- model family
- temperature
- number of samples
- CoT vs no-CoT
- N candidates

This stage should store everything for later analysis.

------

## Layer 4 — Candidate Extraction & Normalization

### Input

- raw LLM response

### Steps

- extract SQL only
- remove markdown/code fences
- standardize formatting
- detect multiple statements
- reject unsafe/non-SELECT queries if applicable
- normalize dialect if needed

### Output

- executable candidate SQLs

This stage is essential in practice.

------

## Layer 5 — Correctness & Validity Gate

This is one of the most important additions.

### Steps

#### A. Syntax validation

Use parser / EXPLAIN / dry-run.

#### B. Execution validation

Ensure the query runs successfully.

#### C. Semantic equivalence validation

Compare:

- row count
- checksum / hash of resultset
- exact result equality where feasible

#### D. Safety rule validation

Reject candidates that:

- add unsupported hints
- reference non-existent indexes/tables
- change projection semantics
- introduce LIMIT unless original had it

### Output

- valid candidates
- rejected candidates with reasons

This is your execution-guided decoding / execution-guided filtering stage.

------

## Layer 6 — Benchmark & Metric Collector

### Input

- valid candidate SQL
- raw baseline query
- rule-based rewritten query

### Metrics

Collect more than just runtime.

#### Core metrics

- execution time
- planning time
- total cost from EXPLAIN
- actual rows
- shared buffers hit/read
- temp read/write
- memory spill if available
- CPU time if measurable

#### Robustness metrics

- cold cache execution
- warm cache execution
- repeated-run variance

### Output

- per-candidate metric vector

------

## Layer 7 — Candidate Scoring & Selection

### Input

- validated candidates
- benchmark metrics

### Scoring idea

A candidate should not win only because it is fast once.

Example score:

- semantic correctness: mandatory gate
- speedup ratio
- execution stability
- plan cost reduction
- success confidence

You can use either:

- hard filtering + best latency
  or
- weighted score

### Output

- best candidate
- all ranked candidates

This stage is especially useful for self-consistency experiments.

------

## Layer 8 — Analysis & Research Reporting Layer

### Outputs for the paper

- prompt performance table
- model comparison table
- valid rewrite rate
- speedup distribution
- failure taxonomy
- query complexity vs success rate
- examples of good/bad rewrites

This turns the system into a publishable research workflow rather than just a demo pipeline.

------

# 3. Comparison: Your Design vs My Design

Here is the conceptual difference.

## Your design emphasizes

- practical end-to-end flow
- prompt-driven optimization
- iterative candidate selection

## My design adds

- explicit scientific control points
- correctness gate as its own stage
- extraction/normalization stage
- selection separated from generation
- stronger reporting/failure analysis

So the right answer is not “mine replaces yours”.

The right answer is:

**your design is the right foundation, and mine makes it academically rigorous.**

------

# 4. Best Combined Architecture for Your Project

Below is the merged architecture I recommend you use in the paper.

------

## Final Proposed Architecture

### Stage 1 — Benchmark Query & Metadata Preparation

Input:

- raw query from TPC-DS
- schema
- index metadata
- DB engine info

Output:

- query package
- optional optimization context

------

### Stage 2 — Prompt Construction

Input:

- raw query
- context package
- selected prompt template

Sub-components:

- prompt engineering layer
- optimization rules injection
- CoT instruction option
- zero-shot / few-shot / schema-aware / rule-guided variants

Output:

- optimization prompt

This corresponds closely to your “Prompts Preparation”, but now it is a clean experimental stage.

------

### Stage 3 — LLM Rewrite Generation

Input:

- optimization prompt
- selected LLM model
- decoding config
- candidate count N

Steps:

- call LLM API
- generate N candidates
- record reasoning traces
- parse response blocks

Output:

- candidate SQL set
- candidate reasoning set

This is your “Query Optimization via LLM”.

------

### Stage 4 — Candidate Normalization

Steps:

- extract SQL
- remove non-SQL text
- canonicalize format
- reject non-query statements
- dialect cleanup

Output:

- normalized candidate SQLs

This is missing in your current design and should be added.

------

### Stage 5 — Execution-Guided Validation

Steps:

- syntax validation
- execution validation
- semantic equivalence validation against baseline resultset

Output:

- valid optimized candidates
- invalid candidates with failure reasons

This should be a standalone stage, not buried inside benchmarking.

------

### Stage 6 — Performance Benchmark

Input:

- baseline query
- pg_query or other rewriting-tool output
- valid LLM candidates

Steps:

- repeated execution
- collect runtime and plan metrics
- compare warm/cold runs if feasible
- compute speedup

Output:

- benchmark results per candidate

This corresponds to your “Performance Benchmark”, but tightened.

------

### Stage 7 — Candidate Ranking & Best Query Selection

Steps:

- assign score to each valid candidate
- rank by correctness + performance + stability
- select best LLM rewrite

Output:

- best_llm_optimized_query
- ranked candidate report

This matches your candidate selection, but with clearer separation from execution.

------

### Stage 8 — Research Analysis

Steps:

- compare prompt families
- compare model families
- analyze failure cases
- correlate performance with query complexity
- identify which rewrite patterns helped most

Output:

- tables, graphs, conclusions for paper

------

# 5. Architectural Diagram

Here is the flow in compact text form:

```text
TPC-DS Raw Query + Schema/Index Metadata
                ↓
        Prompt Construction Layer
                ↓
        LLM Rewrite Generation
                ↓
      Candidate Extraction/Normalization
                ↓
      Execution-Guided Validation Gate
                ↓
        Performance Benchmark Engine
                ↓
     Candidate Ranking & Best Selection
                ↓
        Research Analysis & Reporting
```

------

# 6. What Research Variables Should Be Controlled

For the paper, this is very important.

## Independent variables

These are what you deliberately change:

- prompt type
- CoT vs non-CoT
- schema-aware vs non-schema-aware
- candidate count N
- model family
- temperature / decoding configuration

## Dependent variables

These are what you measure:

- syntax-valid rate
- execution-valid rate
- semantic-equivalence rate
- average speedup
- p95 speedup
- variance
- failure rate
- token cost / API cost if you want

## Controlled variables

These must stay fixed:

- DB engine version
- hardware
- dataset size factor
- query set
- benchmark procedure
- execution repetition count
- timeouts
- maximum refinement loops

This framing will make your methodology section much stronger.

------

# 7. My Recommendation on Iteration / Refinement Loops

You proposed rerunning when none of the optimized candidates performs well. That is a good idea, but I recommend controlling it like this:

## Recommended rule

- Round 1: generate N candidates
- If none valid, allow 1 repair round
- If valid but all slower, stop and record failure
- Maximum total LLM attempts per query must be fixed

Why this is better:

- fair across all queries and models
- reproducible
- easy to explain in methodology

If you keep rerunning until you get a good answer, the experiment becomes biased.

------

# 8. What Should Be Included as Context to the LLM

Your current prompt design mentions:

- db engine
- schema design
- index
- frequent query field

I mostly agree, but I would rank them.

## High priority context

- DB engine type
- schema definition
- join keys / PK-FK relationships
- indexed columns
- query objective: preserve exact semantics

## Medium priority

- known optimization rules
- example rewrites
- dialect restrictions

## Lower priority / use carefully

- “frequent query field”
- runtime statistics
- manually suggested rewrite patterns

Reason: too much context may distract the LLM or create leakage/bias.

For the first version of the project, I would test these 3 context bundles:

### Context A

raw query only

### Context B

raw query + schema + DB engine

### Context C

raw query + schema + DB engine + indexes + optimization rules

That gives a clean experiment.

------

# 9. Best Architecture Choice for Your Specific Topic

Since your topic is not “build a production SQL optimizer”, but rather:

**“Illustrate and evaluate the capabilities of LLM to rewrite complex SQL queries”**

the architecture should emphasize:

- controlled input conditions
- reproducible candidate generation
- strict correctness gate
- fair comparison
- rich failure analysis

So the final design should be more like a **research evaluation pipeline** than a product pipeline.

That is why I recommend the combined architecture above.

------

# 10. Final Verdict

## Your design is strong in:

- practical workflow
- prompt experimentation
- candidate generation and selection

## My added refinements improve:

- scientific rigor
- validity of evaluation
- reproducibility
- paper quality

## Best final architecture:

Use **your pipeline structure**, but insert these critical additions:

- candidate normalization stage
- explicit execution-guided validation gate
- separate ranking/selection stage
- constrained retry policy
- failure taxonomy/reporting layer

------

# 11. Recommended Final Module List

If you want a concise finalized version, I would name the modules like this:

1. **Workload Preparation Module**
2. **Prompt Construction Module**
3. **LLM Candidate Generation Module**
4. **Candidate Normalization Module**
5. **Semantic Validation Module**
6. **Benchmark Execution Module**
7. **Candidate Scoring and Selection Module**
8. **Research Analysis Module**

That naming is suitable for your methodology chapter.

If you want, next I can turn this into a **formal methodology section draft** for the paper, or convert it into a **Mermaid architecture diagram** directly usable in your report.