from .models import (
    ARK_BASE_URL,
    DOUBAO_MODELS,
    DOUBAO_SEED_2_0_LITE_260215,
    DOUBAO_SEED_2_0_MINI_260215,
    DOUBAO_SEED_2_0_PRO_260215,
    GPT_5_4,
    GPT_5_4_MINI,
    GPT_5_4_NANO,
    OPENAI_MODELS,
)

__all__ = [
    "ARK_BASE_URL",
    "DOUBAO_SEED_2_0_PRO_260215",
    "DOUBAO_SEED_2_0_LITE_260215",
    "DOUBAO_SEED_2_0_MINI_260215",
    "DOUBAO_MODELS",
    "GPT_5_4",
    "GPT_5_4_MINI",
    "GPT_5_4_NANO",
    "OPENAI_MODELS",
    "generate_text",
    "DefaultCandidateGenerationLayer",
]


def generate_text(input_text: str, model: str) -> str:
    from .candidate_generation import generate_text as _generate_text

    return _generate_text(input_text, model)


def DefaultCandidateGenerationLayer(*args, **kwargs):
    from .generation_layer import DefaultCandidateGenerationLayer as _DefaultCandidateGenerationLayer

    return _DefaultCandidateGenerationLayer(*args, **kwargs)
