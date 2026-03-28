from __future__ import annotations

import os

from .models import ARK_BASE_URL, DOUBAO_MODELS


def _build_openai_client(**kwargs):
    from openai import OpenAI

    return OpenAI(**kwargs)


def get_model_client(model: str):
    if model in DOUBAO_MODELS:
        api_key = os.getenv("ARK_API_KEY")
        if not api_key:
            raise RuntimeError("ARK_API_KEY is not set in environment variables")
        return _build_openai_client(
            base_url=ARK_BASE_URL,
            api_key=api_key,
        )

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set in environment variables")

    base_url = os.getenv("OPENAI_BASE_URL")
    if base_url:
        return _build_openai_client(api_key=api_key, base_url=base_url)
    return _build_openai_client(api_key=api_key)
