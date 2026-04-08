## **Experimental Setup**

Each experiment is defined as a combination of three components:

### **1. Prompt Strategy (P)**

- **P0: BASE** — No additional context or optimization guidance
- **P1: ENGINE** — Includes database engine-specific information
- **P2: SCHEMA_MIN** — Includes minimal schema information
- **P3: SCHEMA_STATS** — Includes schema statistics (e.g., indexes, distributions)

### **2. Reasoning Strategy (R)**

- **R0: DIRECT** — Direct query generation without reasoning steps
- **R1: COT (Chain-of-Thought)** — Step-by-step reasoning is encouraged
- **R2: TWO_PASS** — Two-stage refinement (initial generation + optimization pass)

### **3. Model (M)**

- **OpenAI**: gpt5 / gpt5-mini / gpt5-nano

------

## **Execution Configuration**

- **Number of generated query candidates per run**: 3
- **Number of execution trials per query**: 5
- **Phase 1 workload size**: randomly select 10 queries from the 99 testing queries
- **Phase 2 workload size**: randomly select 30 queries from the 99 testing queries
- **Primary model family for current experiment**: gpt-5 / gpt-5-mini / gpt-5-nano
- **Secondary model family (time permitting)**: gpt-5.4 / gpt-5.4-mini / gpt-5.4-nano

------

## **Evaluation Procedure**

### **Phase 1: Small-Scale Exploration**

To identify the most effective configuration, a preliminary evaluation is conducted:

- Randomly select **10 queries** from the total **99 TPC-DS queries**
- For each combination of *(P, R, M)*:
  - Generate optimized query candidates
  - Execute and record performance metrics using **5 execution trials per query**
- Example configuration:
  - *(P0 + R0 + gpt-5-mini)*

The goal of this phase is to **determine the best-performing combination** based on execution efficiency and correctness.

------

### **Phase 2: Large-Scale Evaluation**

After identifying the optimal configuration:

- Apply the **best-performing combination** to the randomly selected 30 query **99 queries**
- Apply the **best-performing combination** to a randomly selected **30-query subset** from the same **99 TPC-DS queries**
- Use **5 execution trials per query** when benchmarking
- Compare results against:
  - **Baseline queries** (original SQL)
  - **Rewrite tool outputs** (if applicable)

This phase evaluates the **overall effectiveness and generalizability** of the selected approach.

------

## **Summary of Workflow**

- **Small-scale evaluation (10 queries):**
  $$
  (P_x + R_y + M_z) \rightarrow \text{performance results}
  $$

- **Large-scale evaluation (30 queries):**
  $$
  \text{Best } (P, R, M) \rightarrow \text{final evaluation}
  $$

------

## **Expected Outcome**

- Identification of the most effective **prompt + reasoning + model combination**
- Quantitative comparison of performance improvements over:
  - Baseline queries
  - Existing rewrite approaches
