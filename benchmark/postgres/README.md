# PostgreSQL Benchmark 资源

## TPC-DS SF=1（从 `datasets/tpcds/sf1/*.dat` 导入）

在仓库根目录执行（确保相对路径能找到数据文件）：

```bash
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/schema.sql
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/load_sf1.sql
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -f benchmark/postgres/tpcds/validate_sf1.sql
```

如果导入时报 `extra data after last expected column`，说明 `.dat` 每行末尾带了多余的 `|` 终止符，请用 dsdgen 重新生成数据（关闭 TERMINATE）：

```bash
cd third_party/DSGen-software-code-4.0.0/tools
./dsdgen -SCALE 1 -DIR ../../../datasets/tpcds/sf1 -FORCE Y -TERMINATE N -QUIET Y
```

目录说明：

- `benchmark/postgres/tpcds/schema.sql`：TPC-DS 24 表 DDL（PostgreSQL）
- `benchmark/postgres/tpcds/load_sf1.sql`：使用 `\copy` 从 `datasets/tpcds/sf1/*.dat` 导入
- `benchmark/postgres/tpcds/validate_sf1.sql`：导入后的行数校验（包含核心表的期望行数）

## 跑 baseline（默认 10 条 query）

默认跑 10 条：`1,2,3,5,6,7,8,9,10,12`（本机 SF=1 上 `q4` 往往非常慢，容易超时）。

```bash
python3 benchmark/postgres/baseline_benchmark.py \
  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \
  --query-ids "1,2,3,5,6,7,8,9,10,12" \
  --repeat 3 \
  --output-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --output-csv benchmark/results/postgres_tpcds_sf1_queries10_baseline.csv
```

## 跑 layer3 对比（3 模型）

配置火山方舟 API Key：

```bash
export ARK_API_KEY="your_api_key_here"
```

执行：

```bash
python3 benchmark/postgres/layer3_benchmark.py \
  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \
  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --query-ids "1,2,3,5,6,7,8,9,10,12" \
  --repeat 3 \
  --output-json benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json \
  --output-csv benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.csv \
  --artifacts-dir benchmark/results/artifacts_layer3_sf1_q10
```

## 生成汇总报告（Task5）

```bash
python3 benchmark/postgres/generate_report.py \
  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --compare-json benchmark/results/postgres_tpcds_sf1_queries10_layer3_compare.json \
  --output-md benchmark/postgres/report.md
```

报告输出到 [report.md](./report.md)。

## Prompt/Reasoning Engineering 实验（Pro 模型，9 条 query）

固定：

- model：`doubao-seed-2-0-pro-260215`
- queries：`q1,q2,q3,q6,q7,q8,q9,q10,q12`

运行（需要先 `export ARK_API_KEY=...`）：

```bash
python3 benchmark/postgres/ablation_experiments.py \
  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \
  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --mode all \
  --repeat 1 \
  --statement-timeout-ms 300000
```

如果你已经提前生成了 `benchmark/results/ablation_artifacts/`（LLM 输出的 raw/sql），可以只执行 PostgreSQL 的 EXPLAIN 计时（不再调用模型）：

```bash
python3 benchmark/postgres/ablation_experiments.py \
  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \
  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --mode all \
  --repeat 1 \
  --statement-timeout-ms 300000 \
  --execute-only \
  --artifacts-dir benchmark/results/ablation_artifacts \
  --output-json benchmark/results/postgres_tpcds_sf1_q9_pro_ablations_exec.json \
  --output-csv benchmark/results/postgres_tpcds_sf1_q9_pro_ablations_exec.csv
```

生成实验报告：

```bash
python3 benchmark/postgres/generate_ablation_report.py \
  --ablations-json benchmark/results/postgres_tpcds_sf1_q9_pro_ablations_exec.json \
  --output-md benchmark/postgres/prompt_reasoning_report.md
```
