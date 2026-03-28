from __future__ import annotations

from pipeline.models import PipelineRequest, PromptPackage, WorkloadItem


class DefaultPromptBuilderLayer:
    """Layer 2: constructs prompt packages without calling the model."""

    def build(self, workload_item: WorkloadItem, request: PipelineRequest) -> PromptPackage:
        prompt_strategy = request.prompt_strategy.upper()
        reasoning_mode = request.reasoning_mode.upper()
        context_block = self._build_context_block(workload_item)

        if reasoning_mode == "TWO_PASS":
            stage1_prompt = self._build_plan_prompt(workload_item.raw_query, context_block, workload_item.engine)
            stage2_template = self._build_apply_plan_template(workload_item.raw_query, context_block, workload_item.engine)
            return PromptPackage(
                query_id=workload_item.query_id,
                raw_query=workload_item.raw_query,
                model=request.model,
                candidate_count=request.candidate_count,
                prompt_strategy=prompt_strategy,
                reasoning_mode=reasoning_mode,
                prompt_text=stage2_template,
                stage1_prompt_text=stage1_prompt,
                stage2_prompt_template=stage2_template,
                metadata={"context_block": context_block},
            )

        prompt_text = self._build_single_prompt(
            raw_query=workload_item.raw_query,
            engine=workload_item.engine,
            prompt_strategy=prompt_strategy,
            reasoning_mode=reasoning_mode,
            context_block=context_block,
        )
        return PromptPackage(
            query_id=workload_item.query_id,
            raw_query=workload_item.raw_query,
            model=request.model,
            candidate_count=request.candidate_count,
            prompt_strategy=prompt_strategy,
            reasoning_mode=reasoning_mode,
            prompt_text=prompt_text,
            metadata={"context_block": context_block},
        )

    def _build_single_prompt(
        self,
        *,
        raw_query: str,
        engine: str,
        prompt_strategy: str,
        reasoning_mode: str,
        context_block: str,
    ) -> str:
        lines = self._strategy_header(prompt_strategy, engine)
        if reasoning_mode == "COT_DELIM":
            lines.extend(
                [
                    "Briefly reason about likely bottlenecks first.",
                    "Then output the final SQL between <SQL> and </SQL>.",
                    "The final SQL must preserve exact semantics.",
                ]
            )
        else:
            lines.extend(
                [
                    "Return only one optimized SQL query.",
                    "Do not output explanations or markdown.",
                    "Preserve the result set exactly.",
                ]
            )

        if context_block:
            lines.extend(["", context_block])

        lines.extend(["", "SQL:", raw_query])
        return "\n".join(lines)

    @staticmethod
    def _strategy_header(prompt_strategy: str, engine: str) -> list[str]:
        header = [
            f"You are an expert {engine} SQL optimizer.",
            f"Target engine: {engine}.",
        ]
        if prompt_strategy == "P0_BASE":
            return header
        if prompt_strategy == "P4_RULES":
            return header + [
                "Prefer semantically safe rewrites such as predicate pushdown, join simplification, and avoiding unnecessary SELECT *.",
                "Do not introduce hints or non-standard syntax.",
            ]
        return header + [
            "Rewrite the SQL to be semantically equivalent but potentially faster.",
            "Use only syntax supported by the target engine.",
        ]

    @staticmethod
    def _build_context_block(workload_item: WorkloadItem) -> str:
        lines: list[str] = []
        if workload_item.schema_text:
            lines.extend(["Schema context:", workload_item.schema_text])
        if workload_item.index_text:
            if lines:
                lines.append("")
            lines.extend(["Index context:", workload_item.index_text])
        return "\n".join(lines)

    @staticmethod
    def _build_plan_prompt(raw_query: str, context_block: str, engine: str) -> str:
        lines = [
            f"You are an expert {engine} SQL optimizer.",
            "Produce a short optimization plan as numbered bullets.",
            "Do not output SQL in this step.",
        ]
        if context_block:
            lines.extend(["", context_block])
        lines.extend(["", "SQL:", raw_query])
        return "\n".join(lines)

    @staticmethod
    def _build_apply_plan_template(raw_query: str, context_block: str, engine: str) -> str:
        lines = [
            f"You are an expert {engine} SQL optimizer.",
            "Apply the optimization plan below to rewrite the SQL.",
            "Return only one optimized SQL query.",
            "Do not output explanations or markdown.",
            "Preserve the result set exactly.",
            "",
            "Optimization plan:",
            "{plan}",
        ]
        if context_block:
            lines.extend(["", context_block])
        lines.extend(["", "SQL:", raw_query])
        return "\n".join(lines)
