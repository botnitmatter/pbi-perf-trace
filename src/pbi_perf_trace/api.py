from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import pandas as pd

from .analysis import AnalysisConfig, build_run_level_tables
from .metadata import event_metadata_frame
from .normalize import load_traces_from_paths
from .pivot import pivot_durations


@dataclass(frozen=True)
class RunOutputs:
    """Outputs produced by :func:`run_all`.

    All tables are also returned in-memory (as pandas DataFrames) even when
    `write_outputs=True`.
    """

    pivoted: pd.DataFrame
    kept_summary: pd.DataFrame
    cold_reason_summary: pd.DataFrame
    tag_official: pd.DataFrame
    cache_official: pd.DataFrame


def _select_core_cols(events_df: pd.DataFrame) -> pd.DataFrame:
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
    return events_df[[c for c in select_cols if c in events_df.columns]]


def _enrich_with_metadata(events_df: pd.DataFrame) -> pd.DataFrame:
    events_df = _select_core_cols(events_df)
    meta = event_metadata_frame()
    return events_df.merge(meta, left_on="label", right_on="Event", how="left")


def _engine_only(enriched: pd.DataFrame) -> pd.DataFrame:
    return enriched[
        ~enriched["responsibility"].fillna("").str.contains(r"Client|System", regex=True)
    ].copy()


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _official_rollups(run_layer: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
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
        c
        for c in cache_layer_wide.columns
        if c not in {"tag", "attribute", "mode", "cache_state"}
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

    return tag_official, cache_official


def run_all(
    files: Mapping[str, str | Path | PathLike[str]],
    *,
    export_output: str | Path | PathLike[str] | None = "output.csv",
    out_dir: str | Path | PathLike[str] | None = "out",
    cfg: AnalysisConfig | None = None,
    write_outputs: bool = True,
) -> RunOutputs:
    """Run the end-to-end pipeline (export + analyze) programmatically.

    Args:
        files: Mapping of `tag -> path_to_trace_json`.
        export_output: If set, writes the pivoted CSV to this path.
        out_dir: If set, writes analysis CSVs into this directory.
        cfg: Optional analysis configuration overrides.
        write_outputs: Whether to write CSV outputs to disk.

    Returns:
        RunOutputs containing the main output tables.
    """

    if not files:
        raise ValueError("At least one file is required")

    events_df = load_traces_from_paths(files)
    if events_df.empty:
        raise ValueError("No events loaded")

    enriched = _enrich_with_metadata(events_df)

    pivoted = pivot_durations(_engine_only(enriched))

    analysis_cfg = cfg or AnalysisConfig()
    out = build_run_level_tables(enriched, analysis_cfg)

    kept_summary = out["kept_summary"]
    cold_reason_summary = out["cold_reason_summary"]
    run_layer = out["run_layer"]

    tag_official, cache_official = _official_rollups(run_layer)

    if write_outputs:
        if export_output is not None:
            _write_csv(pivoted, Path(export_output))
        if out_dir is not None:
            out_path = Path(out_dir)
            _write_csv(kept_summary, out_path / "kept_summary.csv")
            _write_csv(cold_reason_summary, out_path / "cold_reason_summary.csv")
            _write_csv(tag_official, out_path / "tag_official.csv")
            _write_csv(cache_official, out_path / "cache_official.csv")

    return RunOutputs(
        pivoted=pivoted,
        kept_summary=kept_summary,
        cold_reason_summary=cold_reason_summary,
        tag_official=tag_official,
        cache_official=cache_official,
    )


def pbi_perf_trace(
    path_dict: Mapping[str, str | Path | PathLike[str]],
    output_path: str | Path | PathLike[str] | None = None,
    *,
    cfg: AnalysisConfig | None = None,
) -> RunOutputs:
    """Public, pip-friendly API: run export + analyze in one call.

    This matches the intended usage:

    - `pip install pbi-perf-trace`
    - `from pbi_perf_trace import pbi_perf_trace`
    - `pbi_perf_trace(path_dict, output_path)`

    Args:
        path_dict: Mapping of `tag -> trace_json_path`.
        output_path: If provided, writes output CSVs into this directory.
            If None, does not write any files.
        cfg: Optional analysis configuration overrides.

    Returns:
        RunOutputs containing the resulting DataFrames.
    """

    if output_path is None:
        return run_all(
            path_dict,
            export_output=None,
            out_dir=None,
            cfg=cfg,
            write_outputs=False,
        )

    out_dir = Path(output_path).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    return run_all(
        path_dict,
        export_output=out_dir / "output.csv",
        out_dir=out_dir,
        cfg=cfg,
        write_outputs=True,
    )
