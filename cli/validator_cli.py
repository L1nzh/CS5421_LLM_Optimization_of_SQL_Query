from __future__ import annotations

import argparse
import json

from config.settings import ValidationSettings
from db.postgres_adapter import PostgresAdapter
from execution.query_executor import QueryExecutor
from validator.comparison_strategy import ComparisonStrategy
from validator.validation_pipeline import ValidationPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate optimized SQL queries against a baseline query.")
    parser.add_argument("--dsn", required=True, help="Database connection string.")
    parser.add_argument("--raw-query", required=True, help="Baseline SQL query.")
    parser.add_argument(
        "--candidate-query",
        dest="candidate_queries",
        action="append",
        required=True,
        help="Optimized SQL query candidate. Pass multiple times for multiple candidates.",
    )
    parser.add_argument(
        "--comparison-strategy",
        choices=[strategy.value for strategy in ComparisonStrategy],
        default=ComparisonStrategy.EXACT_UNORDERED.value,
        help="How to compare rows between baseline and candidate results.",
    )
    parser.add_argument(
        "--ordered",
        action="store_true",
        help="Preserve row order during normalization.",
    )
    parser.add_argument(
        "--float-tolerance",
        type=float,
        default=1e-6,
        help="Tolerance used to derive float normalization precision.",
    )
    parser.add_argument(
        "--trim-strings",
        action="store_true",
        help="Trim leading and trailing whitespace in string values and column names.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    settings = ValidationSettings(
        comparison_strategy=ComparisonStrategy(args.comparison_strategy),
        preserve_row_order=args.ordered,
        float_tolerance=args.float_tolerance,
        trim_strings=args.trim_strings,
    )

    adapter = PostgresAdapter(args.dsn)
    try:
        pipeline = ValidationPipeline(QueryExecutor(adapter), settings)
        report = pipeline.validate(args.raw_query, args.candidate_queries)
    finally:
        adapter.close()

    print(json.dumps(report.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
