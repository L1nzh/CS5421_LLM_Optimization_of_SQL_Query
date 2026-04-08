# CS5421-LLM-Optimization-of-SQL-Query

## Multi-Layer Research Pipeline

仓库现在额外提供一个可组合的 8-layer pipeline，用于把 workload preparation、prompt construction、LLM candidate generation、candidate normalization、validation、benchmark、ranking、analysis 串联起来，同时保持各层可独立替换与测试。

主要入口：

- `pipeline/sql_optimization_pipeline.py`
- `cli/optimization_pipeline_cli.py`

当前实现状态：

- Layer 1：已实现 `FileOrStringWorkloadPreparationLayer`
- Layer 2：已实现 `DefaultPromptBuilderLayer`
- Layer 3：支持 Ark 与 OpenAI GPT 模型的 Responses API 生成逻辑
- Layer 4：已实现 `DefaultCandidateNormalizationLayer`
- Layer 5：复用现有 validator 作为 `ValidatorValidationGateLayer`
- Layer 6：提供 PostgreSQL `EXPLAIN ANALYZE` 实现与 placeholder 实现
- Layer 7：提供基于 speedup 的默认 ranking
- Layer 8：当前为 summary placeholder，后续可替换为研究分析模块

示例：

```bash
python -m cli.optimization_pipeline_cli \
  --raw-query "SELECT * FROM store_sales" \
  --prompt-strategy P1_ENGINE \
  --reasoning-mode DIRECT \
  --candidate-count 3
```

如果提供 `--dsn`，Layer 5 和 Layer 6 会使用真实数据库进行语义验证与 benchmark；如果不提供，则会走 placeholder 路径，仍然返回统一结构的 ranked output。

Layer-by-layer docs:

- `docs/layers_index.md`

## Layer 3 — LLM Candidate Generation

本仓库当前先实现 Layer 3：给定输入文本（这里用 SQL 优化任务 prompt），调用 LLM 并解析 Responses API 的结构化响应，最终返回模型输出的文本。

### 支持的模型

当前已内置 Ark 与 OpenAI 两类模型常量（见 `layer3/models.py`）：

- `doubao-seed-2-0-pro-260215`
- `doubao-seed-2-0-lite-260215`
- `doubao-seed-2-0-mini-260215`
- `gpt-5.4`
- `gpt-5.4-mini`
- `gpt-5.4-nano`

### 环境准备

建议使用 Python 3.10+ 并创建虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
```

配置火山方舟 API Key（需要你自行在控制台创建并开通对应模型权限）：

```bash
export ARK_API_KEY="your_api_key_here"
```

如果使用 OpenAI GPT 模型，例如 `gpt-5.4-nano`，配置：

```bash
export OPENAI_API_KEY="your_openai_api_key_here"
```

### Quickstart（3 模型 × 3 个 SQL 任务）

```bash
python demo/quickstart_layer3.py
```

### 作为库使用

```python
from layer3 import DOUBAO_SEED_2_0_PRO_260215, generate_text

prompt = "Return only optimized SQL.\\nSQL: SELECT * FROM t;"
out = generate_text(prompt, DOUBAO_SEED_2_0_PRO_260215)
print(out)
```

## Layer 2 — Prompt Engineering & Reasoning Engineering（实验框架）

本仓库的 Layer 2 以“可控实验”的形式实现：固定模型与 workload，通过系统化改变 prompt / reasoning 方式，评估对 SQL 可执行性与性能（speedup）的影响。实现入口在 `benchmark/postgres/ablation_experiments.py`，报告生成在 `benchmark/postgres/generate_ablation_report.py`。

### 变体设计

- Prompt Engineering（P0\~P3）：只改变 prompt 信息量与约束
  - P0\_BASE：基础约束（只输出 SQL、语义等价、PG 语法）
  - P1\_ENGINE：明确引擎与版本（PostgreSQL 16）+ 禁止非 PG 方言
  - P2\_SCHEMA\_MIN：注入精简 schema（仅 query 涉及表/列）
  - P3\_SCHEMA\_STATS：注入 schema + 统计信息（近似行数）
- Reasoning Engineering（R0\~R2）：只改变推理引导方式
  - R0\_DIRECT：直接输出最终 SQL
  - R1\_COT\_DELIM：允许推理，但要求最终 SQL 放在 `<SQL>...</SQL>`，便于抽取执行
  - R2\_TWO\_PASS：两阶段（先 plan，再 apply plan 输出 SQL）

### 运行（PostgreSQL 本地 benchmark）

前置：

- 需要本地 PostgreSQL（并已导入 TPC-DS SF=1 数据）；详细步骤见 `benchmark/postgres/README.md`
- 需要配置 Ark Key：

```bash
export ARK_API_KEY="your_api_key_here"
```

运行 prompt/reasoning 实验（默认固定 9 条 query：q1,q2,q3,q6,q7,q8,q9,q10,q12，固定 pro 模型）：

```bash
python3 benchmark/postgres/ablation_experiments.py \
  --dsn "postgresql://bench:bench@localhost:5432/tpcds_sf1" \
  --baseline-json benchmark/results/postgres_tpcds_sf1_queries10_baseline.json \
  --mode all \
  --repeat 1 \
  --statement-timeout-ms 300000
```

生成 Markdown 报告：

```bash
python3 benchmark/postgres/generate_ablation_report.py \
  --ablations-json benchmark/results/postgres_tpcds_sf1_q9_pro_ablations_exec.json \
  --output-md benchmark/postgres/prompt_reasoning_report.md
```

说明：

- 若你已经有 `benchmark/results/ablation_artifacts/`（模型 raw/sql 产物），可用 `--execute-only` 仅重跑 EXPLAIN 计时，避免再次调用模型（示例见 `benchmark/postgres/README.md`）。
  ```shellscript
  ```

postgresql
