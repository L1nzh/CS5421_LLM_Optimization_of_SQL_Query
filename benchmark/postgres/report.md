# PostgreSQL TPC-DS(SF=1) — Baseline vs Layer3 汇总报告

## 1. 汇总指标（按模型）
指标口径：

- success：compare JSON 中 `success == true` 的 query 数 / workload query 数
- speedup：`baseline_median_execution_time_ms / median_execution_time_ms`（仅统计有数值的条目）
- median speedup：每模型所有 speedup 样本的中位数
- 优于 baseline 比例：speedup > 1.0 的占比（分母为 speedup 样本数）

## 1.1 汇总指标

只统计集合：`q1, q2, q3, q6, q7, q8, q9, q10, q12`。

| model | success（在这 9 条上） | median speedup | success rate | score（median_speedup × success_rate） |
| --- | ---: | ---: | ---: | ---: |
| doubao-seed-2-0-pro-260215 | 9/9 | 1.240 | 1.000 | 1.240 |
| doubao-seed-2-0-lite-260215 | 9/9 | 1.072 | 1.000 | 1.072 |
| doubao-seed-2-0-mini-260215 | 8/9 | 1.115 | 0.889 | 0.991 |

## 2. 实验输入与运行参数

- baseline: `benchmark/results/postgres_tpcds_sf1_queries10_baseline.json`
- layer3 compare: `benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json`
- generated_at: `2026-03-20T07:59:38.685848+00:00`
- workload_dir: `workloads/tpcds/sf1/queries_10`
- queries: `10` (q1, q10, q12, q2, q3, q5, q6, q7, q8, q9)
- baseline repeat: `3`
- compare repeat: `3`
- statement_timeout_ms: `300000`
- dsn: `postgresql://bench:bench@localhost:5432/tpcds_sf1`

## 3. 可复现实验说明（从零开始）

### 3.1 安装与启动 PostgreSQL（macOS Homebrew）

按文档操作：`docs/postgres_macos_homebrew.md`。最小目标是能用 DSN 连接：

```bash
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -c "SELECT 1;"
```

### 3.2 导入 TPC-DS SF=1 数据（schema + load + validate）

准备数据文件：把 TPC-DS SF=1 的 `.dat` 文件放到：`datasets/tpcds/sf1/*.dat`。

在仓库根目录执行：

```bash
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/schema.sql
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/load_sf1.sql
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/validate_sf1.sql
```

### 3.3 跑 baseline（q1~q10）

```bash
python3 benchmark/postgres/baseline_benchmark.py \
  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \
  --repeat 3 \
  --output-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --output-csv benchmark/results/postgres_tpcds_sf1_queries10_baseline.csv
```

### 3.4 跑 layer3 对比（3 模型）

先配置 LLM key：

```bash
export ARK_API_KEY="your_api_key_here"
```

执行：

```bash
python3 benchmark/postgres/layer3_benchmark.py \
  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \
  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --repeat 3 \
  --output-json benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json \
  --output-csv benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.csv \
  --artifacts-dir benchmark/results/artifacts_layer3_sf1_q10
```

### 3.5 生成汇总报告（本文件）

```bash
python3 benchmark/postgres/generate_report.py \
  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --compare-json benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json \
  --output-md benchmark/postgres/report.md
```
