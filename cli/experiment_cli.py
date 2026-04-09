from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import ValidationSettings
from experiments import QueryExperimentRunner
from validator.comparison_strategy import ComparisonStrategy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the SQL LLM performance experiment.")
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN.")
    parser.add_argument("--query-dir", default="datasets/testing_query", help="Directory containing testing SQL files.")
    parser.add_argument("--schema-file", default="benchmark/postgres/tpcds/schema.sql", help="Schema file used for prompt context.")
    parser.add_argument("--artifacts-root", default="datasets/artifacts", help="Artifact root directory.")
    parser.add_argument("--phase", choices=["phase1", "phase2", "all"], default="all", help="Which experiment phase to run.")
    parser.add_argument("--phase1-seed", type=int, default=20260407, help="Random seed for phase 1 sampling.")
    parser.add_argument("--phase2-seed", type=int, default=20260408, help="Random seed for phase 2 sampling.")
    parser.add_argument("--statement-timeout-ms", type=int, default=None, help="Optional PostgreSQL statement timeout.")
    parser.add_argument(
        "--comparison-strategy",
        choices=[strategy.value for strategy in ComparisonStrategy],
        default=ComparisonStrategy.HASH.value,
        help="Validation comparison strategy.",
    )
    parser.add_argument("--ordered", action="store_true", help="Preserve row order during validation.")
    parser.add_argument("--float-tolerance", type=float, default=1e-6, help="Float tolerance for validation.")
    parser.add_argument("--stream-batch-size", type=int, default=10_000, help="Stream batch size for hash validation.")
    parser.add_argument("--trim-strings", action="store_true", help="Trim strings during validation normalization.")
    parser.add_argument("--include-gpt54", action="store_true", help="Include gpt-5.4 family combos in phase 1 search.")
    parser.add_argument("--include-local", action="store_true", help="Include the local OpenAI-compatible chat-completions model in phase 1 search.")
    parser.add_argument(
        "--local-model",
        default="VladimirGav/gemma4-26b-16GB-VRAM:latest",
        help="Local model id to use when --include-local is set.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    query_files = sorted(str(path) for path in Path(args.query_dir).glob("*.sql"))
    validation_settings = ValidationSettings(
        comparison_strategy=ComparisonStrategy(args.comparison_strategy),
        float_tolerance=args.float_tolerance,
        preserve_row_order=args.ordered,
        stream_batch_size=args.stream_batch_size,
        trim_strings=args.trim_strings,
    )
    runner = QueryExperimentRunner(
        dsn=args.dsn,
        schema_file=args.schema_file,
        artifacts_root=args.artifacts_root,
        validation_settings=validation_settings,
        statement_timeout_ms=args.statement_timeout_ms,
    )

    phase1_config = QueryExperimentRunner.default_phase1_config()
    phase1_config = type(phase1_config)(
        phase_name=phase1_config.phase_name,
        sample_size=phase1_config.sample_size,
        benchmark_repeats=phase1_config.benchmark_repeats,
        random_seed=args.phase1_seed,
    )
    phase2_config = QueryExperimentRunner.default_phase2_config()
    phase2_config = type(phase2_config)(
        phase_name=phase2_config.phase_name,
        sample_size=phase2_config.sample_size,
        benchmark_repeats=phase2_config.benchmark_repeats,
        random_seed=args.phase2_seed,
    )

    combos = QueryExperimentRunner.default_combos()
    if args.include_gpt54:
        combos.extend(QueryExperimentRunner.default_gpt54_combos())
    if args.include_local:
        combos.extend(QueryExperimentRunner.default_local_combos(args.local_model))

    payload: dict[str, object] = {}
    phase1_result = None
    if args.phase in {"phase1", "all"}:
        phase1_result = runner.run_phase(
            phase_config=phase1_config,
            query_files=query_files,
            combos=combos,
            destination_subdir="candidate_combo",
        )
        payload["phase1"] = phase1_result.to_dict()

    if args.phase in {"phase2", "all"}:
        if phase1_result is None:
            raise SystemExit("Phase 2 requires phase 1 in this CLI run so the best combo can be selected.")
        if phase1_result.selected_best_combo is None:
            raise SystemExit("Phase 1 did not identify a best combo.")
        phase2_result = runner.run_phase(
            phase_config=phase2_config,
            query_files=query_files,
            combos=[phase1_result.selected_best_combo],
            destination_subdir="fullset_combo",
        )
        payload["phase2"] = phase2_result.to_dict()

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
