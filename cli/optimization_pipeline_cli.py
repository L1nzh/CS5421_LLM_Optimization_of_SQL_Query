from __future__ import annotations

import argparse
import json

from config.settings import ValidationSettings
from pipeline import PipelineRequest, build_default_pipeline
from validator.comparison_strategy import ComparisonStrategy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the end-to-end SQL rewrite research pipeline.")
    parser.add_argument("--dsn", help="Database connection string for validation and benchmarking.", default=None)
    parser.add_argument("--raw-query", dest="raw_queries", action="append", default=[], help="Raw SQL query text.")
    parser.add_argument("--query-file", dest="query_files", action="append", default=[], help="Path to a file containing a raw SQL query.")
    parser.add_argument("--schema-text", default=None, help="Optional schema context text.")
    parser.add_argument("--schema-file", default=None, help="Optional file containing schema context.")
    parser.add_argument("--index-text", default=None, help="Optional index context text.")
    parser.add_argument("--index-file", default=None, help="Optional file containing index context.")
    parser.add_argument("--prompt-strategy", default="P1_ENGINE", help="Prompt strategy id, for example P0_BASE or P4_RULES.")
    parser.add_argument("--reasoning-mode", default="DIRECT", help="Reasoning mode, for example DIRECT, COT_DELIM, or TWO_PASS.")
    parser.add_argument("--model", default="doubao-seed-2-0-pro-260215", help="LLM model id.")
    parser.add_argument("--candidate-count", type=int, default=3, help="Number of LLM candidates to generate per query.")
    parser.add_argument("--benchmark-repeats", type=int, default=1, help="Benchmark repeats for layer 6.")
    parser.add_argument("--statement-timeout-ms", type=int, default=None, help="Optional PostgreSQL statement timeout for layer 6.")
    parser.add_argument(
        "--comparison-strategy",
        choices=[strategy.value for strategy in ComparisonStrategy],
        default=ComparisonStrategy.EXACT_UNORDERED.value,
        help="Layer 5 comparison strategy.",
    )
    parser.add_argument("--ordered", action="store_true", help="Preserve row order during validation.")
    parser.add_argument("--float-tolerance", type=float, default=1e-6, help="Float tolerance for validation.")
    parser.add_argument("--stream-batch-size", type=int, default=10_000, help="Streaming batch size for hash validation.")
    parser.add_argument("--trim-strings", action="store_true", help="Trim string values during validation normalization.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    validation_settings = ValidationSettings(
        comparison_strategy=ComparisonStrategy(args.comparison_strategy),
        float_tolerance=args.float_tolerance,
        preserve_row_order=args.ordered,
        stream_batch_size=args.stream_batch_size,
        trim_strings=args.trim_strings,
    )
    pipeline = build_default_pipeline(
        dsn=args.dsn,
        validation_settings=validation_settings,
        benchmark_repeats=args.benchmark_repeats,
        statement_timeout_ms=args.statement_timeout_ms,
    )
    request = PipelineRequest(
        raw_queries=tuple(args.raw_queries),
        query_files=tuple(args.query_files),
        schema_text=args.schema_text,
        schema_file=args.schema_file,
        index_text=args.index_text,
        index_file=args.index_file,
        prompt_strategy=args.prompt_strategy,
        reasoning_mode=args.reasoning_mode,
        model=args.model,
        candidate_count=args.candidate_count,
        benchmark_repeats=args.benchmark_repeats,
        statement_timeout_ms=args.statement_timeout_ms,
        validation_settings=validation_settings,
    )
    result = pipeline.run(request)
    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
