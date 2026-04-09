from __future__ import annotations

from .client import get_model_client
from .models import MINIMAX_MODELS
from .response_parse import extract_output_text


def generate_text(input_text: str, model: str) -> str:
    client = get_model_client(model)
    if model in MINIMAX_MODELS:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": input_text}],
        )
        content = response.choices[0].message.content
        if isinstance(content, str) and content.strip():
            return content
        raise RuntimeError("No output text found in MiniMax response")

    response = client.responses.create(
        model=model,
        input=input_text,
    )
    return extract_output_text(response)
