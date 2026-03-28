from __future__ import annotations

from collections.abc import Callable
from typing import Optional

from pipeline.models import GeneratedCandidate, PromptPackage


class DefaultCandidateGenerationLayer:
    """Layer 3: generates N LLM candidates from a prompt package."""

    def __init__(self, generate_fn: Optional[Callable[[str, str], str]] = None):
        self._generate_fn = generate_fn

    def generate(self, prompt_package: PromptPackage) -> list[GeneratedCandidate]:
        candidates: list[GeneratedCandidate] = []
        generate_fn = self._generate_fn or self._default_generate_fn
        for index in range(1, prompt_package.candidate_count + 1):
            stage1_text = None
            prompt_text = prompt_package.prompt_text
            if prompt_package.stage1_prompt_text and prompt_package.stage2_prompt_template:
                stage1_text = generate_fn(prompt_package.stage1_prompt_text, prompt_package.model)
                prompt_text = prompt_package.stage2_prompt_template.format(plan=stage1_text)

            raw_text = generate_fn(prompt_text, prompt_package.model)
            candidates.append(
                GeneratedCandidate(
                    candidate_id=f"{prompt_package.query_id}_cand_{index}",
                    raw_text=raw_text,
                    model=prompt_package.model,
                    stage1_text=stage1_text,
                )
            )
        return candidates

    @staticmethod
    def _default_generate_fn(input_text: str, model: str) -> str:
        from .candidate_generation import generate_text

        return generate_text(input_text, model)
