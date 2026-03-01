from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypedDict

import pandas as pd


@dataclass(frozen=True)
class AnalysisConfig:
    """Configuration for run-level, cache-aware, outlier-robust analysis."""

    outlier_keep_pct: float = 0.90
    outlier_min_runs_per_group: int = 8

    enable_eviction_detection: bool = True
    eviction_vertipaq_factor: float = 1.5
    min_warm_runs_for_eviction: int = 5

    test_suffix_pattern: str = r"\s*\(?test\)?\s*$"


_TAG_RE = re.compile(r"(?P<attribute>[^_]+)_on_(?P<mode>[^_]+)")


def _require_cols(df: pd.DataFrame, cols: Iterable[str], *, name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{name} is missing required columns: {missing}")


def parse_tag(tag: object) -> dict[str, str]:
    m = _TAG_RE.fullmatch(str(tag).strip())
    if not m:
        return {"attribute": "unknown", "mode": "unknown"}
    return {"attribute": m.group("attribute"), "mode": m.group("mode")}


def normalize_visual_title(title: object, *, cfg: AnalysisConfig | None = None) -> str:
    pattern = (cfg.test_suffix_pattern if cfg else AnalysisConfig().test_suffix_pattern)
    test_suffix_re = re.compile(pattern, flags=re.IGNORECASE)

    s = "" if title is None else str(title)
    s = s.strip()
    return test_suffix_re.sub("", s).strip()


def responsibility_bucket(responsibility: object) -> str:
    value = ("" if responsibility is None else str(responsibility)).lower()
    if "vertipaq" in value:
        return "VertiPaq"
    if "external source" in value:
        return "External Source"
    if "formula engine" in value:
        return "Formula Engine"
    if "client" in value:
        return "Client"
    if "system" in value:
        return "System"
    return "Other"


def mark_leaf_events(df: pd.DataFrame) -> pd.Series:
    if "id" not in df.columns or "parentId" not in df.columns:
        return pd.Series([True] * len(df), index=df.index)

    parent_ids = df.loc[df["parentId"].notna() & (df["parentId"] != df["id"]), "parentId"]
    parent_set = set(parent_ids.tolist())
    return ~df["id"].isin(parent_set)


def compute_exclusive_duration_ms(
    df: pd.DataFrame,
    *,
    id_col: str = "id",
    parent_col: str = "parentId",
    duration_col: str = "duration_ms",
) -> pd.Series:
    if id_col not in df.columns or parent_col not in df.columns or duration_col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="float")

    working = df[[id_col, parent_col, duration_col]].copy()
    working[duration_col] = pd.to_numeric(working[duration_col], errors="coerce")
    working.loc[working[parent_col] == working[id_col], parent_col] = pd.NA

    child_sum = working.groupby(parent_col, dropna=False)[duration_col].sum()
    children_total = working[id_col].map(child_sum)
    exclusive = working[duration_col] - children_total.fillna(0)
    return exclusive.clip(lower=0)


def choose_run_id_column(df: pd.DataFrame) -> str:
    for c in ("top_parent_id", "parentId", "id"):
        if c in df.columns:
            return c
    raise KeyError("No suitable run id column found (expected one of: top_parent_id, parentId, id)")


def add_percentile_filter_flags(
    df: pd.DataFrame,
    *,
    group_cols: list[str],
    value_col: str,
    keep_pct: float,
    min_group_size: int,
) -> pd.Series:
    keep_pct = float(keep_pct)
    if not (0 < keep_pct <= 1):
        raise ValueError("keep_pct must be in (0, 1]")

    lower_q = (1 - keep_pct) / 2
    upper_q = 1 - lower_q

    values = pd.to_numeric(df[value_col], errors="coerce")
    group_sizes = df.groupby(group_cols, dropna=False)[value_col].transform("size")

    tmp = df[group_cols].copy()
    tmp["_v"] = values
    q_low = tmp.groupby(group_cols, dropna=False)["_v"].transform(lambda s: s.quantile(lower_q))
    q_high = tmp.groupby(group_cols, dropna=False)["_v"].transform(lambda s: s.quantile(upper_q))

    in_band = values.ge(q_low) & values.le(q_high)
    apply_filter = group_sizes.ge(int(min_group_size))
    return (~apply_filter) | in_band


def _build_run_meta(engine_df: pd.DataFrame) -> pd.DataFrame:
    _require_cols(
        engine_df,
        ["tag", "attribute", "mode", "visual_key", "run_id"],
        name="analysis_engine",
    )

    cols = ["tag", "attribute", "mode", "visual_key", "run_id"]
    if "run_start" in engine_df.columns:
        cols.append("run_start")

    run_meta = engine_df[cols].drop_duplicates().copy()
    sort_cols = [
        "tag",
        "visual_key",
        *(["run_start"] if "run_start" in run_meta.columns else []),
        "run_id",
    ]
    run_meta = run_meta.sort_values(sort_cols, na_position="last")
    run_meta["run_number"] = run_meta.groupby(["tag", "visual_key"], dropna=False).cumcount() + 1
    run_meta["cache_state"] = run_meta["run_number"].eq(1).map(
        lambda is_first: "cold" if is_first else "warm"
    )
    run_meta["cold_reason"] = run_meta["run_number"].eq(1).map(
        lambda is_first: "first_run" if is_first else ""
    )
    return run_meta


def _apply_eviction_detection(
    *,
    run_layer_raw: pd.DataFrame,
    run_meta: pd.DataFrame,
    cfg: AnalysisConfig,
) -> pd.DataFrame:
    if not cfg.enable_eviction_detection:
        return run_meta

    run_keys = ["tag", "attribute", "mode", "visual_key", "run_id"]
    _require_cols(run_layer_raw, run_keys + ["layer", "duration_ms"], name="run_layer_raw")

    run_layers_wide = (
        run_layer_raw.pivot_table(
            index=run_keys,
            columns="layer",
            values="duration_ms",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    run_layers_wide = run_layers_wide.merge(
        run_meta[run_keys + ["run_number"]],
        on=run_keys,
        how="left",
    )

    run_layers_wide["VertiPaq_ms"] = run_layers_wide.get("VertiPaq", 0.0)

    warm_candidates = run_layers_wide[run_layers_wide["run_number"].gt(1)].copy()
    warm_baseline = (
        warm_candidates.groupby(
            ["tag", "attribute", "mode", "visual_key"],
            dropna=False,
        )["VertiPaq_ms"]
        .agg(warm_vertipaq_median="median", warm_runs="count")
        .reset_index()
    )
    run_layers_wide = run_layers_wide.merge(
        warm_baseline,
        on=["tag", "attribute", "mode", "visual_key"],
        how="left",
    )

    warm_runs = run_layers_wide["warm_runs"].fillna(0)
    warm_med = run_layers_wide["warm_vertipaq_median"].fillna(0)
    evict_mask = (
        warm_runs.ge(cfg.min_warm_runs_for_eviction)
        & warm_med.gt(0)
        & run_layers_wide["VertiPaq_ms"].gt(warm_med * cfg.eviction_vertipaq_factor)
    )

    refined = run_meta.copy()
    refined = refined.merge(
        run_layers_wide[run_keys].assign(eviction_cold=evict_mask.values),
        on=run_keys,
        how="left",
    )
    refined["eviction_cold"] = refined["eviction_cold"].fillna(False)

    refined.loc[refined["eviction_cold"] & refined["run_number"].ne(1), "cache_state"] = "cold"
    refined.loc[refined["eviction_cold"] & refined["run_number"].ne(1), "cold_reason"] = "eviction"
    return refined.drop(columns=["eviction_cold"], errors="ignore")


class RunLevelTables(TypedDict):
    analysis_events: pd.DataFrame
    analysis_engine: pd.DataFrame
    analysis_engine_inclusive: pd.DataFrame
    analysis_engine_leaf: pd.DataFrame
    run_id_col: str
    run_meta: pd.DataFrame
    run_layer_raw: pd.DataFrame
    run_total_raw: pd.DataFrame
    run_total_labeled: pd.DataFrame
    run_layer: pd.DataFrame
    run_total: pd.DataFrame
    kept_summary: pd.DataFrame
    cold_reason_summary: pd.DataFrame


def build_run_level_tables(
    enriched_df: pd.DataFrame,
    cfg: AnalysisConfig | None = None,
) -> RunLevelTables:
    """Build run-level analysis tables.

    Returns a dict with a stable key set used by both the CLI and library users.
    """
    cfg = cfg or AnalysisConfig()

    _require_cols(enriched_df, ["tag", "duration_ms"], name="enriched")

    analysis_events = enriched_df.copy()
    analysis_events["duration_ms"] = pd.to_numeric(analysis_events["duration_ms"], errors="coerce")
    analysis_events = analysis_events.dropna(subset=["duration_ms"]).copy()

    tag_dims = analysis_events["tag"].apply(parse_tag).apply(pd.Series)
    analysis_events = pd.concat([analysis_events, tag_dims], axis=1)

    analysis_events["layer"] = analysis_events.get("responsibility", "").apply(
        responsibility_bucket
    )
    analysis_events["visual_key"] = analysis_events.get(
        "visual", pd.Series(index=analysis_events.index, dtype="object")
    ).apply(lambda s: normalize_visual_title(s, cfg=cfg))

    analysis_events["exclusive_duration_ms"] = compute_exclusive_duration_ms(analysis_events)

    analysis_engine_inclusive = analysis_events[
        ~analysis_events["layer"].isin(["Client", "System"])
    ].copy()
    analysis_engine = analysis_engine_inclusive.copy()
    analysis_engine["duration_ms_inclusive"] = analysis_engine["duration_ms"]
    analysis_engine["duration_ms"] = analysis_engine["exclusive_duration_ms"]

    run_id_col = choose_run_id_column(analysis_engine)
    analysis_engine["run_id"] = analysis_engine[run_id_col]

    if "start_time" in analysis_engine.columns:
        analysis_engine["run_start"] = analysis_engine.groupby(
            ["tag", "run_id"],
            dropna=False,
        )["start_time"].transform("min")

    run_meta = _build_run_meta(analysis_engine)

    run_layer_raw = (
        analysis_engine.groupby(
            ["tag", "attribute", "mode", "visual_key", "run_id", "layer"],
            dropna=False,
        )["duration_ms"]
        .sum()
        .reset_index()
    )

    run_meta = _apply_eviction_detection(run_layer_raw=run_layer_raw, run_meta=run_meta, cfg=cfg)

    run_layer_raw = run_layer_raw.merge(
        run_meta[
            [
                "tag",
                "attribute",
                "mode",
                "visual_key",
                "run_id",
                "cache_state",
                "run_number",
                "cold_reason",
            ]
        ],
        on=["tag", "attribute", "mode", "visual_key", "run_id"],
        how="left",
    )

    run_total_raw = (
        run_layer_raw.groupby(
            ["tag", "attribute", "mode", "visual_key", "run_id"],
            dropna=False,
        )["duration_ms"]
        .sum()
        .rename("run_total_ms")
        .reset_index()
    )
    run_total_labeled = run_total_raw.merge(
        run_meta[
            [
                "tag",
                "attribute",
                "mode",
                "visual_key",
                "run_id",
                "cache_state",
                "run_number",
                "cold_reason",
            ]
        ],
        on=["tag", "attribute", "mode", "visual_key", "run_id"],
        how="left",
    )

    run_total_labeled["keep_for_avg"] = add_percentile_filter_flags(
        run_total_labeled,
        group_cols=["tag", "attribute", "mode", "visual_key", "cache_state"],
        value_col="run_total_ms",
        keep_pct=cfg.outlier_keep_pct,
        min_group_size=cfg.outlier_min_runs_per_group,
    )

    run_total = run_total_labeled[run_total_labeled["keep_for_avg"]].copy()
    keep_keys = run_total[["tag", "attribute", "mode", "visual_key", "run_id"]].drop_duplicates()
    run_layer = run_layer_raw.merge(
        keep_keys,
        on=["tag", "attribute", "mode", "visual_key", "run_id"],
        how="inner",
    )

    kept_summary = (
        run_total_labeled.groupby(["tag", "attribute", "mode", "cache_state"], dropna=False)
        .agg(total_runs=("run_id", "nunique"), kept_runs=("keep_for_avg", "sum"))
        .reset_index()
    )
    kept_summary["dropped_runs"] = kept_summary["total_runs"] - kept_summary["kept_runs"]

    cold_reason_summary = (
        run_total_labeled[run_total_labeled["cache_state"].eq("cold")]
        .groupby(["tag", "attribute", "mode", "cold_reason"], dropna=False)["run_id"]
        .nunique()
        .rename("cold_runs")
        .reset_index()
    )

    analysis_engine_inclusive["is_leaf"] = mark_leaf_events(analysis_engine_inclusive)
    analysis_engine_leaf = analysis_engine_inclusive[analysis_engine_inclusive["is_leaf"]].copy()

    return {
        "analysis_events": analysis_events,
        "analysis_engine": analysis_engine,
        "analysis_engine_inclusive": analysis_engine_inclusive,
        "analysis_engine_leaf": analysis_engine_leaf,
        "run_id_col": run_id_col,
        "run_meta": run_meta,
        "run_layer_raw": run_layer_raw,
        "run_total_raw": run_total_raw,
        "run_total_labeled": run_total_labeled,
        "run_layer": run_layer,
        "run_total": run_total,
        "kept_summary": kept_summary,
        "cold_reason_summary": cold_reason_summary,
    }
