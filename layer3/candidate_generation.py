from __future__ import annotations

from .client import get_model_client
from .response_parse import extract_output_text


def generate_text(input_text: str, model: str) -> str:
    client = get_model_client(model)
    response = client.responses.create(
        model=model,
        input=input_text,
    )
    return extract_output_text(response)
