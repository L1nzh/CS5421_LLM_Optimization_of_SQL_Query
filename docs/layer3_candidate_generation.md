# Layer 3: Candidate Generation

## Purpose

Layer 3 is responsible for invoking the model and returning raw candidate outputs.

This layer should not perform SQL extraction, validation, or benchmarking. Its job is generation only.

## Default Implementation

- Existing low-level generation: `layer3/candidate_generation.py`
- Default layer adapter: `layer3/generation_layer.py`
- Client routing: `layer3/client.py`

## Responsibilities

- Call the configured model through the Responses API
- Support multiple candidate generations for the same prompt package
- Support one-pass and two-pass generation
- Return raw model output without SQL execution or correctness assumptions

## Model Routing

The default implementation now supports two model providers.

### Ark models

Configured through:

- `ARK_API_KEY`

Used for models listed in `DOUBAO_MODELS`, such as:

- `doubao-seed-2-0-pro-260215`
- `doubao-seed-2-0-lite-260215`
- `doubao-seed-2-0-mini-260215`

### OpenAI GPT models

Configured through:

- `OPENAI_API_KEY`
- optional `OPENAI_BASE_URL`

Current exported model constants include:

- `gpt-5.4`
- `gpt-5.4-mini`
- `gpt-5.4-nano`

## Input

The layer consumes `PromptPackage`.

Important fields:

- `prompt_text`
- `stage1_prompt_text`
- `stage2_prompt_template`
- `model`
- `candidate_count`

## Output

The layer emits a list of `GeneratedCandidate`.

Each candidate contains:

- `candidate_id`
- `raw_text`
- `model`
- `stage1_text`

## Current Behavior

- For single-pass modes, the layer directly calls the model with `prompt_text`
- For `TWO_PASS`, the layer:
  - first generates a plan
  - then injects that plan into the second-stage prompt
  - then generates final candidate SQL text

## Design Notes

- This layer intentionally keeps raw output intact for downstream normalization and research analysis
- It is designed to support independent testing with injected generation functions

## TODO

- Add provider abstraction as an explicit interface instead of current model-name routing
- Add structured capture of token usage, latency, and provider metadata for Layer 8 research analysis
- Add configurable decoding controls such as temperature and top-p
- Add retry policy and bounded generation budget
