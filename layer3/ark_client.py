import os

from openai import OpenAI

from .models import ARK_BASE_URL


def get_ark_client() -> OpenAI:
    api_key = os.getenv("ARK_API_KEY")
    if not api_key:
        raise RuntimeError("ARK_API_KEY is not set in environment variables")

    return OpenAI(
        base_url=ARK_BASE_URL,
        api_key=api_key,
    )
