from __future__ import annotations

from pipeline.models import AnalysisReport, QueryOptimizationResult


class PlaceholderAnalysisLayer:
    """Layer 8 placeholder that summarizes the current run without paper analytics."""

    def analyze(self, result: QueryOptimizationResult) -> AnalysisReport:
        valid_count = sum(1 for candidate in result.ranked_candidates if candidate.is_valid)
        selected = result.selected_query is not None
        return AnalysisReport(
            summary=(
                f"Processed {len(result.ranked_candidates)} candidates; "
                f"{valid_count} passed validation; "
                f"selected_query={'yes' if selected else 'no'}."
            ),
            metadata={
                "candidate_count": len(result.ranked_candidates),
                "valid_candidate_count": valid_count,
                "selected_query": selected,
            },
        )
