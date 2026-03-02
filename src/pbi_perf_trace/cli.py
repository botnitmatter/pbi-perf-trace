from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from . import __version__
from .analysis import AnalysisConfig, build_run_level_tables
from .api import run_all
from .metadata import event_metadata_frame
from .normalize import load_traces
from .pivot import pivot_durations


def _load_autorun_config(path: Path) -> tuple[dict[str, Path], str | None, str | None]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Config must be a JSON object")

    files_obj = data.get("files")
    if not isinstance(files_obj, dict) or not files_obj:
        raise ValueError("Config must include non-empty 'files' object mapping tag -> filepath")

    base = path.parent
    files: dict[str, Path] = {}
    for tag, p in files_obj.items():
        if not isinstance(tag, str) or not tag.strip():
            raise ValueError("Config 'files' keys (tags) must be non-empty strings")
        if not isinstance(p, str) or not p.strip():
            raise ValueError("Config 'files' values must be non-empty strings (file paths)")
        resolved = Path(p).expanduser()
        if not resolved.is_absolute():
            resolved = (base / resolved).resolve()
        files[tag] = resolved

    export_output = data.get("export_output", "output.csv")
    out_dir = data.get("out_dir", "out")
    if export_output is not None and not isinstance(export_output, str):
        raise ValueError("Config 'export_output' must be a string or null")
    if out_dir is not None and not isinstance(out_dir, str):
        raise ValueError("Config 'out_dir' must be a string or null")
    return files, export_output, out_dir


def _parse_files_kv(items: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --files value (expected name=tag): {item}")
        name, tag = item.split("=", 1)
        name = name.strip()
        tag = tag.strip()
        if not name or not tag:
            raise ValueError(f"Invalid --files value (empty name/tag): {item}")
        out[name] = tag
    return out


def cmd_export(args: argparse.Namespace) -> int:
    if not args.files:
        raise ValueError("At least one --files value is required (filename=tag)")

    base_path = Path(args.base_path).expanduser()
    if not base_path.is_dir():
        raise FileNotFoundError(f"Base path does not exist or is not a directory: {base_path}")
    files = _parse_files_kv(args.files)

    events_df = load_traces(files, base_path)
    if events_df.empty:
        raise ValueError("No events loaded")

    event_metadata_df = event_metadata_frame()

    select_cols = [
        "id",
        "parent_name",
        "top_parent_id",
        "visual",
        "parentId",
        "start_time",
        "label",
        "duration_ms",
        "tag",
    ]
    events_df = events_df[[c for c in select_cols if c in events_df.columns]]

    enriched = events_df.merge(
        event_metadata_df,
        left_on="label",
        right_on="Event",
        how="left",
    )

    engine_only = enriched[
        ~enriched["responsibility"].fillna("").str.contains(r"Client|System", regex=True)
    ].copy()

    pivoted = pivot_durations(engine_only)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pivoted.to_csv(output_path, index=False)
    return 0


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def cmd_analyze(args: argparse.Namespace) -> int:
    if not args.files:
        raise ValueError("At least one --files value is required (filename=tag)")

    base_path = Path(args.base_path).expanduser()
    if not base_path.is_dir():
        raise FileNotFoundError(f"Base path does not exist or is not a directory: {base_path}")
    out_dir = Path(args.out_dir)
    files = _parse_files_kv(args.files)

    events_df = load_traces(files, base_path)
    if events_df.empty:
        raise ValueError("No events loaded")

    event_metadata_df = event_metadata_frame()

    select_cols = [
        "id",
        "parent_name",
        "top_parent_id",
        "visual",
        "parentId",
        "start_time",
        "label",
        "duration_ms",
        "tag",
    ]
    events_df = events_df[[c for c in select_cols if c in events_df.columns]]

    enriched = events_df.merge(
        event_metadata_df,
        left_on="label",
        right_on="Event",
        how="left",
    )

    cfg = AnalysisConfig(
        outlier_keep_pct=float(args.outlier_keep_pct),
        outlier_min_runs_per_group=int(args.outlier_min_runs_per_group),
        enable_eviction_detection=not args.no_eviction_detection,
        eviction_vertipaq_factor=float(args.eviction_vertipaq_factor),
        min_warm_runs_for_eviction=int(args.min_warm_runs_for_eviction),
    )

    out = build_run_level_tables(enriched, cfg)

    kept_summary = out["kept_summary"]
    cold_reason_summary = out["cold_reason_summary"]
    run_layer = out["run_layer"]

    # Tag official roll-up
    tag_run_wide = (
        run_layer.pivot_table(
            index=["tag", "attribute", "mode", "run_id"],
            columns="layer",
            values="duration_ms",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    tag_layer_wide = (
        tag_run_wide.groupby(["tag", "attribute", "mode"], dropna=False)
        .mean(numeric_only=True)
        .reset_index()
    )
    layer_cols = [c for c in tag_layer_wide.columns if c not in {"tag", "attribute", "mode"}]
    tag_layer_wide["Total"] = tag_layer_wide[layer_cols].sum(axis=1)
    run_counts = (
        tag_run_wide.groupby(["tag", "attribute", "mode"], dropna=False)["run_id"]
        .nunique()
        .rename("Runs")
        .reset_index()
    )
    tag_layer_wide = tag_layer_wide.merge(run_counts, on=["tag", "attribute", "mode"], how="left")

    tag_official = tag_layer_wide.copy()
    tag_official["Direct query"] = tag_official.get("External Source", 0)
    tag_official["DAX query"] = tag_official.get("VertiPaq", 0) + tag_official.get(
        "Formula Engine", 0
    )
    tag_official = tag_official[
        ["tag", "attribute", "mode", "Runs", "DAX query", "Direct query", "Total"]
    ]

    # Cache official roll-up
    cache_run_wide = (
        run_layer.pivot_table(
            index=["tag", "attribute", "mode", "cache_state", "run_id"],
            columns="layer",
            values="duration_ms",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    cache_layer_wide = (
        cache_run_wide.groupby(["tag", "attribute", "mode", "cache_state"], dropna=False)
        .mean(numeric_only=True)
        .reset_index()
    )
    cache_layer_cols = [
        c for c in cache_layer_wide.columns if c not in {"tag", "attribute", "mode", "cache_state"}
    ]
    cache_layer_wide["Total"] = cache_layer_wide[cache_layer_cols].sum(axis=1)
    cache_official = cache_layer_wide.copy()
    cache_official["Direct query"] = cache_official.get("External Source", 0)
    cache_official["DAX query"] = cache_official.get("VertiPaq", 0) + cache_official.get(
        "Formula Engine", 0
    )
    cache_official = cache_official[
        [
            "tag",
            "attribute",
            "mode",
            "cache_state",
            "DAX query",
            "Direct query",
            "Total",
        ]
    ]

    _write_csv(kept_summary, out_dir / "kept_summary.csv")
    _write_csv(cold_reason_summary, out_dir / "cold_reason_summary.csv")
    _write_csv(tag_official, out_dir / "tag_official.csv")
    _write_csv(cache_official, out_dir / "cache_official.csv")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pbi-perf-trace",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--config",
        default="pbi-perf-trace.json",
        help="Config file used when running with no args (autorun)",
    )
    # Don't let argparse emit a hard error on empty invocation; we'll show help
    # ourselves in `main()` for a nicer UX.
    sub = parser.add_subparsers(dest="cmd", required=False)

    p_export = sub.add_parser("export", help="Export pivoted CSV from trace JSON")
    p_export.add_argument("--base-path", required=True)
    p_export.add_argument("--files", action="append", default=[], help="filename=tag (repeatable)")
    p_export.add_argument("--output", default="output.csv")
    p_export.set_defaults(func=cmd_export)

    p_analyze = sub.add_parser("analyze", help="Run cache-aware run-level analysis")
    p_analyze.add_argument("--base-path", required=True)
    p_analyze.add_argument("--files", action="append", default=[], help="filename=tag (repeatable)")
    p_analyze.add_argument("--out-dir", default="out")

    p_analyze.add_argument("--outlier-keep-pct", default=0.90)
    p_analyze.add_argument("--outlier-min-runs-per-group", default=8)

    p_analyze.add_argument("--no-eviction-detection", action="store_true")
    p_analyze.add_argument("--eviction-vertipaq-factor", default=1.5)
    p_analyze.add_argument("--min-warm-runs-for-eviction", default=5)

    p_analyze.set_defaults(func=cmd_analyze)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        if not hasattr(args, "func"):
            # No subcommand provided -> try autorun via config.
            cfg_path = Path(args.config).expanduser()
            if cfg_path.is_file():
                files, export_output, out_dir = _load_autorun_config(cfg_path)
                run_all(files, export_output=export_output, out_dir=out_dir)
                return 0

            parser.print_help(file=sys.stderr)
            print(
                f"\nAutorun config not found: {cfg_path}\n"
                "Create it to run export+analyze automatically with no args.\n"
                "Example: {\"files\": {\"tag\": \"C:/path/to/trace.json\"}}",
                file=sys.stderr,
            )
            return 2
        return int(args.func(args))
    except (ValueError, KeyError, FileNotFoundError) as e:
        print(f"pbi-perf-trace: error: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
