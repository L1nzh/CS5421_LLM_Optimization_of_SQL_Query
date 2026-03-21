Since your **research topic is LLM optimization of SQL queries**, using **TPC-DS** is actually a **very appropriate benchmark**. Many database research papers use it to evaluate query optimization techniques, so using it will make your paper academically credible.

I'll explain it in **four parts** so you can directly integrate it into your **Methodology and Evaluation sections**.

------

# 1. What is TPC-DS?

**TPC-DS** stands for:

> **Transaction Processing Council – Decision Support Benchmark**

It is a **standardized benchmark dataset and workload** designed to evaluate **decision-support systems (OLAP databases)**.

It simulates a **large retail company data warehouse**.

Key characteristics:

| Feature   | Description                                             |
| --------- | ------------------------------------------------------- |
| Domain    | Retail sales business                                   |
| Data type | Data warehouse (star schema)                            |
| Tables    | 24 tables                                               |
| Queries   | 99 complex SQL queries                                  |
| Workload  | Analytical queries with joins, aggregations, subqueries |
| Scale     | Configurable dataset sizes                              |

TPC-DS is specifically designed to **stress the SQL optimizer**.

Which is exactly what your project needs.

------

# 2. What does TPC-DS contain?

## 2.1 Schema

TPC-DS uses a **snowflake schema**.

Typical tables:

### Fact tables

- `store_sales`
- `catalog_sales`
- `web_sales`
- `store_returns`
- `catalog_returns`
- `web_returns`
- `inventory`

### Dimension tables

- `date_dim`
- `customer`
- `customer_address`
- `customer_demographics`
- `item`
- `promotion`
- `store`
- `warehouse`
- `time_dim`
- `ship_mode`

This structure creates **very complex joins**.

Example:

```
store_sales
   |
   +-- customer
   |
   +-- item
   |
   +-- store
   |
   +-- date_dim
```

------

## 2.2 Query workload

TPC-DS provides **99 SQL queries**.

These queries are intentionally complex and include:

- multi-table joins
- nested subqueries
- CTEs
- aggregations
- window functions
- correlated queries
- group by / rollups

Example query fragment:

```sql
SELECT
    i_item_id,
    SUM(ss_sales_price) AS revenue
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2001
GROUP BY i_item_id
ORDER BY revenue DESC
LIMIT 100;
```

These queries simulate **real analytics workloads**.

------

# 3. Why TPC-DS is useful for your research

Your research goal:

> Evaluate whether LLM can rewrite SQL queries to improve performance.

TPC-DS is ideal because:

| Reason             | Explanation                     |
| ------------------ | ------------------------------- |
| Standard benchmark | Used by academia and industry   |
| Complex SQL        | Good test for query rewriting   |
| Known workload     | Baseline results available      |
| Realistic schema   | Reflects real data warehouse    |
| Reproducibility    | Anyone can replicate experiment |

Many research papers compare:

```
Original TPC-DS query
vs
Optimized query
```

Exactly like your LLM experiment.

------

# 4. How to use TPC-DS in your experiment

Your evaluation workflow should look like this:

```
TPC-DS Query
      |
      v
Run on database
      |
      v
Measure execution time
      |
      v
Send query to LLM
      |
      v
LLM generates optimized query
      |
      v
Run optimized query
      |
      v
Compare performance
```

------

# 5. Step-by-step experimental setup

## Step 1 — Generate dataset

TPC-DS provides a tool called:

```
dsdgen
```

This generates the dataset.

Example:

```
dsdgen -scale 10
```

Scale factor determines dataset size.

| Scale | Approx size |
| ----- | ----------- |
| 1     | ~1GB        |
| 10    | ~10GB       |
| 100   | ~100GB      |

For research projects:

**Scale factor 10 or 100** is common.

------

## Step 2 — Load dataset into database

You can use:

- PostgreSQL
- DuckDB
- MySQL
- Spark SQL
- Snowflake

Example:

```
COPY store_sales FROM 'store_sales.dat';
```

------

## Step 3 — Run baseline query

Run the **original TPC-DS query**.

Measure:

```
Execution time
CPU usage
I/O
```

Example:

```
EXPLAIN ANALYZE
SELECT ...
```

Record results.

------

## Step 4 — Send query to LLM

Example prompt:

```
Rewrite the following SQL query to improve performance.
Ensure the output is semantically equivalent.

<SQL QUERY>
```

LLM generates:

```
Optimized SQL
```

------

## Step 5 — Execute optimized query

Run:

```
EXPLAIN ANALYZE
<LLM optimized query>
```

------

## Step 6 — Compare performance

Metrics:

| Metric                  | Description    |
| ----------------------- | -------------- |
| Query execution time    | Primary metric |
| CPU time                | Optional       |
| Logical plan complexity | Optional       |
| Join order improvements | Optional       |

Example result table:

| Query | Original Time | LLM Time | Improvement |
| ----- | ------------- | -------- | ----------- |
| Q12   | 12.4s         | 7.3s     | 41%         |
| Q35   | 8.9s          | 9.1s     | -2%         |
| Q48   | 22.1s         | 15.7s    | 29%         |

------

# 6. Important experimental control

For a fair comparison you must:

### Run multiple trials

Example:

```
run query 5 times
average result
```

------

### Clear cache

Otherwise caching may affect results.

Example:

```
DISCARD ALL
```

or restart DB.

------

### Same hardware environment

Ensure:

- same memory
- same CPU
- same dataset

------

# 7. Example evaluation pipeline

Your methodology may look like:

```
TPC-DS Query Set (99 queries)
        |
        v
Baseline Execution
        |
        v
LLM Query Rewriting
        |
        v
Optimized Query Execution
        |
        v
Performance Comparison
```

------

# 8. Example text you can use in your paper

**Methodology description example**

> The evaluation uses the TPC-DS benchmark, a widely adopted decision-support benchmark developed by the Transaction Processing Council. TPC-DS simulates a retail data warehouse environment consisting of 24 tables and 99 complex analytical queries involving joins, aggregations, and nested subqueries.
>
> In this study, each original TPC-DS query is executed to establish a baseline execution time. The same query is then provided to a Large Language Model (LLM) which generates an optimized equivalent query. Both queries are executed under identical conditions and their execution times are compared to evaluate the effectiveness of LLM-based SQL query optimization.

------

# 9. Practical tools you should use

Recommended stack:

| Tool            | Purpose                 |
| --------------- | ----------------------- |
| PostgreSQL      | database                |
| TPC-DS toolkit  | dataset generation      |
| Python          | experiment automation   |
| LLM API         | query rewriting         |
| EXPLAIN ANALYZE | performance measurement |

------

# 10. Strong suggestion for your project

Because your topic is **LLM optimization**, you should:

Do experiments on:

```
10–20 representative TPC-DS queries
```

instead of all 99.

Choose queries with:

- nested queries
- multiple joins
- CTEs

These are **challenging for LLM**.

------

✅ If you want, I can also show you something **extremely useful for your research**:

- **Which TPC-DS queries are hardest to optimize**
- **Which queries LLM usually fails**
- **How to design prompts for SQL optimization experiments**

These will make your **paper much stronger**.