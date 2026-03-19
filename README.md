# CS5421-LLM-Optimization-of-SQL-Query

## Layer 3 — LLM Candidate Generation

本仓库当前先实现 Layer 3：给定输入文本（这里用 SQL 优化任务 prompt），调用 LLM 并解析 Responses API 的结构化响应，最终返回模型输出的文本。

### 支持的模型（火山方舟 Ark）

当前已内置 3 个模型常量（见 `layer3/models.py`）：

- `doubao-seed-2-0-pro-260215`
- `doubao-seed-2-0-lite-260215`
- `doubao-seed-2-0-mini-260215`

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
