from __future__ import annotations

import pandas as pd


def _min_start_per_group(df: pd.DataFrame, meta_cols: list[str]) -> pd.DataFrame:
    return df.groupby(meta_cols, as_index=False)["start_time"].min()


def _sum_duration_per_event(df: pd.DataFrame, meta_cols: list[str]) -> pd.DataFrame:
    group_cols = meta_cols + ["label"] + (["order"] if "order" in df.columns else [])
    return df.groupby(group_cols, as_index=False)["duration_ms"].sum()


def _order_event_columns(
    pivot: pd.DataFrame,
    meta_cols: list[str],
    df_source: pd.DataFrame,
) -> list[str]:
    event_cols = [c for c in pivot.columns if c not in set(meta_cols + ["start_time"])]
    if "order" not in df_source.columns:
        return sorted(event_cols)

    label_order = (
        df_source[["label", "order"]]
        .dropna()
        .drop_duplicates()
        .sort_values("order")
        .loc[:, "label"]
        .tolist()
    )
    ordered = [c for c in label_order if c in event_cols]
    remaining = [c for c in event_cols if c not in set(label_order)]
    return ordered + remaining


def pivot_durations(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot durations to one row per (visual, top_parent_id, tag)."""
    meta_cols = ["visual", "top_parent_id", "tag"]

    required = {"start_time", "label", "duration_ms"} | set(meta_cols)
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns for pivot: {sorted(missing)}")

    min_start = _min_start_per_group(df, meta_cols)
    summed = _sum_duration_per_event(df, meta_cols)

    pivot = (
        summed.pivot_table(
            index=meta_cols,
            columns="label",
            values="duration_ms",
            fill_value=0,
            aggfunc="sum",
        )
        .reset_index()
    )

    pivot = pivot.merge(min_start, on=meta_cols, how="left")
    ordered_events = _order_event_columns(pivot, meta_cols, df)
    return pivot[meta_cols + ["start_time"] + ordered_events]
