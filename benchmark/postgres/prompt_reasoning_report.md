# Prompt & Reasoning Engineering 实验报告（Pro 模型）

## 1. 实验设置

- engine: `postgresql`
- dsn: `postgresql://bench:bench@localhost:5432/tpcds_sf1`
- model: `doubao-seed-2-0-pro-260215`
- workload\_dir: `workloads/tpcds/sf1/queries_10`
- queries: `9` (q1, q2, q3, q6, q7, q8, q9, q10, q12)
- repeat: `1`
- statement\_timeout\_ms: `300000`
- generated\_at: `2026-03-21T07:28:01.450515+00:00`

指标口径：

- 使用 `EXPLAIN (ANALYZE, FORMAT JSON)` 的 `Execution Time` 作为执行时间（ms）
- speedup = baseline\_median\_execution\_time\_ms / variant\_median\_execution\_time\_ms（仅统计 success 的样本）

## 2. Prompt Engineering 结果汇总

| variant           | success | median speedup | 优于 baseline 比例 | speedup 样本数 |
| ----------------- | ------- | -------------- | -------------- | ----------- |
| P0\_BASE          | 9/9     | 1.103          | 5/9            | 9           |
| P1\_ENGINE        | 9/9     | 1.258          | 8/9            | 9           |
| P2\_SCHEMA\_MIN   | 9/9     | 1.270          | 8/9            | 9           |
| P3\_SCHEMA\_STATS | 9/9     | 1.298          | 9/9            | 9           |
| P4\_RULES         | 9/9     | 1.019          | 5/9            | 9           |

## 3. Reasoning Engineering 结果汇总

| variant        | success | median speedup | 优于 baseline 比例 | speedup 样本数 |
| -------------- | ------- | -------------- | -------------- | ----------- |
| R0\_DIRECT     | 8/9     | 1.121          | 6/8            | 8           |
| R1\_COT\_DELIM | 9/9     | 1.288          | 8/9            | 9           |
| R2\_TWO\_PASS  | 7/9     | 1.189          | 4/7            | 7           |

## 4. Prompt 具体内容（示例：q1）

### P0\_BASE

```text
You are a SQL optimizer.
Rewrite the SQL to be semantically equivalent but potentially faster.
Constraints:
- Return only ONE SQL query.
- Do NOT output explanations or markdown.
- Preserve the result set exactly (columns, ordering, LIMIT).
- Use only standard syntax (no hints).

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

### P1\_ENGINE

```text
You are a PostgreSQL 16 SQL optimizer.
Target engine: PostgreSQL 16.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.
Constraints:
- Return only ONE SQL query.
- Do NOT output explanations or markdown.
- Preserve the result set exactly (columns, ordering, LIMIT).
- Use only standard PostgreSQL syntax (no hints, no proprietary keywords).

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

### P2\_SCHEMA\_MIN

```text
You are a PostgreSQL 16 SQL optimizer.
Target engine: PostgreSQL 16.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.
Constraints:
- Return only ONE SQL query.
- Do NOT output explanations or markdown.

Schema (subset):
- customer(c_customer_sk, c_customer_id, c_current_addr_sk, c_first_name, c_last_name, c_email_address, c_birth_year, c_birth_month, c_birth_day, c_birth_country, c_preferred_cust_flag)
- store(s_store_sk, s_state)
- store_returns(sr_returned_date_sk, sr_store_sk, sr_customer_sk, sr_return_amt)
- date_dim(d_date_sk, d_year)

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

### P3\_SCHEMA\_STATS

```text
You are a PostgreSQL 16 SQL optimizer.
Target engine: PostgreSQL 16.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.
Constraints:
- Return only ONE SQL query.
- Do NOT output explanations or markdown.

Schema (subset):
Table stats (approx):
- customer: approx_rows=100000
- date_dim: approx_rows=73049
- store: approx_rows=12
- store_returns: approx_rows=287514

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

### P4\_RULES

```text
You are a PostgreSQL 16 SQL optimizer.
Target engine: PostgreSQL 16.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.
Constraints:
- Return only ONE SQL query.
- Do NOT output explanations or markdown.
- Preserve the result set exactly (columns, ordering, LIMIT).
- Use only standard PostgreSQL syntax (no hints).
- Prefer explicit JOIN syntax over implicit joins.
- Avoid unnecessary SELECT * (keep columns identical to the original query output).

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

## 5. Reasoning Prompt 具体内容（示例：q1）

### R0\_DIRECT

```text
You are a PostgreSQL 16 SQL optimizer.
Target engine: PostgreSQL 16.
Rewrite the SQL to be semantically equivalent but potentially faster on PostgreSQL.
Constraints:
- Return only ONE SQL query.
- Do NOT output explanations or markdown.
- Preserve the result set exactly (columns, ordering, LIMIT).
- Use only standard PostgreSQL syntax (no hints).

Schema (subset):
(none)

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

### R1\_COT\_DELIM

```text
You are a PostgreSQL 16 SQL optimizer.
Target engine: PostgreSQL 16.
Analyze performance bottlenecks and propose rewrite steps briefly.
Then output the final optimized SQL between <SQL> and </SQL> tags.
Constraints:
- The final SQL MUST be inside <SQL>...</SQL>.
- Preserve result set exactly (columns, ordering, LIMIT).
- Use only standard PostgreSQL syntax (no hints).

Schema (subset):
(none)

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

### R2\_TWO\_PASS

```text
You are a PostgreSQL 16 SQL optimizer.
Target engine: PostgreSQL 16.
Apply the optimization plan to rewrite the SQL.
Constraints:
- Return only ONE SQL query.
- Do NOT output explanations or markdown.
- Preserve result set exactly (columns, ordering, LIMIT).
- Use only standard PostgreSQL syntax (no hints).

Optimization plan:
1. Create a covering index on date_dim on (d_year, d_date_sk) to quickly retrieve all date keys for the year 2000 without full table access.
2. Create a covering index on store_returns on (sr_returned_date_sk, sr_customer_sk, sr_store_sk) including sr_return_amt to enable index-only scans for the customer_total_return CTE aggregation, eliminating full scans on store_returns.
3. Create a covering index on store on (s_state, s_store_sk) to quickly fetch all store keys for Tennessee stores, and enable early pushdown of the s_state = 'TN' filter.
4. Create a covering index on customer on (c_customer_sk) including c_customer_id to enable index-only scans when joining to the customer table to retrieve customer IDs.
5. Decorrelate the per-store average return subquery by pre-aggregating average total return per store from the customer_total_return CTE once, instead of executing the subquery for every row from the outer ctr1 dataset.
6. Materialize the customer_total_return CTE to avoid recomputing the store returns aggregation twice (once for the outer query and once for the per-store average calculation).
7. Optimize join order to start with the filtered small dataset of Tennessee stores first, then join to the customer_total_return CTE, to reduce the number of rows processed in subsequent steps early.
8. Use hash aggregation for all GROUP BY operations (both the CTE aggregation and the per-store average calculation) to avoid expensive sort operations for large datasets.
9. Enable PostgreSQL 16 incremental sort optimization for the final ORDER BY c_customer_id step to reduce sort overhead before applying the LIMIT 100 clause.
10. Push down all applicable filters as early as possible, including restricting customer_total_return rows to only those associated with Tennessee stores before comparing against the per-store average threshold.

Schema (subset):
(none)

SQL:
WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return >
  (SELECT avg(ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

## 6. 失败样例（截断）

| query | variant                 | error (truncated)                                                                                                                                                                     |
| ----- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| q1    | reasoning/R2\_TWO\_PASS | syntax error at or near "customer\_total\_return" LINE 1: EXPLAIN (ANALYZE, FORMAT JSON) WITH MATERIALIZED customer\_to...                                                          ^ |
| q8    | reasoning/R2\_TWO\_PASS | syntax error at or near "V1" LINE 1: EXPLAIN (ANALYZE, FORMAT JSON) WITH MATERIALIZED V1 AS (                                                          ^                              |
| q10   | reasoning/R0\_DIRECT    | canceling statement due to statement timeout                                                                                                                                          |

## 7. 结果分析

### 7.1 Prompt Engineering

- **总体趋势**：在 9 条 query 上，P1/P2/P3 明显优于 P0 与 P4。P3 的 median speedup 最高（1.298），且 9/9 都优于 baseline；P1 与 P2 紧随其后（median speedup 分别为 1.258 与 1.270）。
- **P0\_BASE → P1\_ENGINE 的增益**：仅补充“目标引擎=PostgreSQL 16 + 禁止非 PG 方言”这一信息，就将 median speedup 从 1.103 提升到 1.258，并把“优于 baseline”的比例从 5/9 提升到 8/9，说明引擎约束能显著提升改写质量与一致性。
- **P4\_RULES 的退化**：虽然加入了更多规则，但 median speedup 仅 1.019（5/9 优于 baseline）。在 q1 示例中，P4 生成的 SQL 没有将 `s_state='TN'` 尽早下推到聚合之前，也保留了相关子查询式的阈值计算，从而导致性能几乎不变甚至略差；这提示“规则过多/过泛”可能让模型更保守，反而抑制关键结构性改写。

### 7.2 Reasoning Engineering

- **单轮推理 + 明确分隔（R1\_COT\_DELIM）最好**：成功率 9/9，median speedup 1.288（8/9 优于 baseline）。这种方式兼顾了“让模型先想清楚”与“产出可抽取的最终 SQL”。
- **直接输出（R0\_DIRECT）更容易触发超时**：成功率 8/9；失败样例为 q10 的 statement timeout。这表明在复杂 query 上，缺少结构化约束时，模型更可能生成执行代价更高的改写，从而超过超时阈值。
- **两阶段计划（R2\_TWO\_PASS）可靠性最低**：成功率 7/9。失败主要来自“生成了不可执行 SQL/占位符”（例如 `V1`）或在 `WITH MATERIALIZED` 等细节上出现语法问题，说明两阶段流程对“计划格式与落地 SQL 的严谨性”要求更高，需要更严格的模板与校验才能稳定受益。

