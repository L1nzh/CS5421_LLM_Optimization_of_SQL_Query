# Layer 3 Report — LLM Candidate Generation

## 1. Layer 3 的职责边界

输入：一段纯文本 `input_text`（在本项目 demo 中是“SQL 优化任务 prompt”）与 `model`（模型名常量）。

输出：模型返回的最终文本（本项目期望其内容是“优化后的 SQL”，但 Layer 3 本身不做 SQL 抽取与执行验证）。

Layer 3 只负责：

- 构造并发送 LLM 请求（Responses API）
- 解析结构化响应，抽取“最终输出文本”
- 屏蔽不同模型/平台的响应差异，让上层可以用统一函数批量调用

不负责：

- prompt 策略（Layer 2）
- SQL 抽取/规范化（Layer 4）
- 语义正确性验证与性能评测（Layer 5/6）
- 候选选择与分析（Layer 7/8）

## 2. 为什么使用 OpenAI Python SDK 的 Responses API

本项目采用 OpenAI Python SDK 的 Responses API，核心原因是它提供相对统一的调用与结构化返回：

- 调用方式统一：`client.responses.create(model=..., input=...)`
- 返回结构更可解析：输出通常位于 `response.output` 的 message/content parts 中
- 更利于后续研究：reasoning/usage/metadata 可保留用于 Layer 8 的分析（本阶段先只抽取最终文本）

## 3. 模块设计

代码位于 `layer3/` 包：

- `models.py`：模型名常量与列表
- `ark_client.py`：从环境变量读取 `ARK_API_KEY` 并构建 `OpenAI(base_url=..., api_key=...)`
- `response_parse.py`：`extract_output_text(response)`，从结构化响应中抽取最终文本
- `candidate_generation.py`：对外函数 `generate_text(input_text, model)`，完成“调用 + 解析”

对外入口：

- `generate_text(input_text: str, model: str) -> str`

## 4. 响应解析策略（extract_output_text）

目标是“拿到最终输出文本”，并尽量不依赖某个特定模型的字段细节。

解析优先级：

1. 若 SDK 暴露便捷字段 `output_text` 或 `text` 且为非空字符串，直接返回
2. 否则遍历 `response.output`：
   - 若 block `type == "message"`，遍历其 `content`：
     - 抽取 `type == "output_text"`（或兼容 `type == "text"`）的 `text` 字段
   - 将多个片段按顺序拼接为最终返回值
3. 若未找到任何文本，抛出异常提示 response 结构异常

这一策略能天然绕过非文本 parts（如 reasoning/thought_signature 等），避免把它们拼进最终输出。

## 5. 实验设计（Layer 3 的最小可复现实验）

本阶段实验目标：对比不同参数规模模型在“SQL 优化任务生成”上的输出差异，为后续 Layer 4/5/6 的可执行评测做准备。

### 5.1 自变量（Independent Variable）

- 模型参数规模（通过 pro / lite / mini 作为近似分组）：
  - `doubao-seed-2-0-pro-260215`
  - `doubao-seed-2-0-lite-260215`
  - `doubao-seed-2-0-mini-260215`

### 5.2 因变量（Dependent Variables）

本阶段先采用“生成质量可观察指标”（无需数据库执行）：

- 输出是否遵守约束（只输出 SQL、是否夹带解释）
- 输出是否看起来语义保持（主观检查）
- 输出是否产生明显的优化结构（如 EXISTS/JOIN 改写、谓词下推、字段裁剪等）

后续可扩展（进入 Layer 6 再做严格评测）：

- token 使用量与延迟（若 response 提供 usage/计时字段）

### 5.3 实验材料

demo 采用 3 个代表性任务（见 `demo/sql_tasks.py`）：

- 过滤条件下推与列裁剪
- IN 子查询改写为 EXISTS
- 聚合与 HAVING 场景

### 5.4 预期

预期参数规模更大的模型（pro）在以下方面更稳定：

- 更严格地遵守“只输出 SQL”的格式约束
- 对改写等价性更谨慎（减少语义漂移）
- 更容易产出结构化的优化模式

这些预期会在后续引入“可执行验证与 benchmark”后被严格检验。

