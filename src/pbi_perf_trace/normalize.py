from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import Any

import pandas as pd

from .io import read_trace_json


def _ensure_parent_id(df: pd.DataFrame) -> pd.DataFrame:
    if "parentId" not in df.columns:
        df["parentId"] = df.get("id")
    df["parentId"] = df["parentId"].fillna(df.get("id"))
    return df


def _add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["start"] = pd.to_datetime(df.get("start"), errors="coerce")
    df["end"] = pd.to_datetime(df.get("end"), errors="coerce")
    df["duration_ms"] = (df["end"] - df["start"]).dt.total_seconds() * 1000
    df["start_time"] = df["start"].dt.strftime("%H:%M:%S")
    return df


def _build_lookups(df: pd.DataFrame) -> tuple[dict[Any, Any], dict[Any, Any], dict[Any, Any]]:
    if "id" not in df.columns:
        return {}, {}, {}

    id_to_parent = df.set_index("id")["parentId"].to_dict() if "parentId" in df.columns else {}
    id_to_name = df.set_index("id")["name"].to_dict() if "name" in df.columns else {}

    visual_col = "metrics.visualTitle"
    id_to_visual = df.set_index("id")[visual_col].to_dict() if visual_col in df.columns else {}
    return id_to_parent, id_to_name, id_to_visual


def _walk_to_root(event_id: Any, id_to_parent: dict[Any, Any]) -> Any:
    current = event_id
    seen: set[Any] = set()
    while True:
        if current in seen:
            return event_id
        seen.add(current)
        parent = id_to_parent.get(current, current)
        if parent == current:
            return current
        current = parent


def _resolve_visual_title(
    event_id: Any,
    id_to_parent: dict[Any, Any],
    id_to_visual: dict[Any, Any],
) -> Any:
    current = event_id
    seen: set[Any] = set()
    while True:
        if current in seen:
            return None
        seen.add(current)

        title = id_to_visual.get(current)
        if pd.notna(title):
            return title

        parent = id_to_parent.get(current, current)
        if parent == current:
            return None
        current = parent


def _add_hierarchy_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "id" not in df.columns:
        df["top_parent_id"] = None
        df["visual"] = None
        df["top_parent_name"] = None
        df["parent_name"] = None
        return df

    id_to_parent, id_to_name, id_to_visual = _build_lookups(df)

    df["top_parent_id"] = df["id"].apply(lambda x: _walk_to_root(x, id_to_parent))
    df["visual"] = df["id"].apply(lambda x: _resolve_visual_title(x, id_to_parent, id_to_visual))

    df["top_parent_name"] = df["top_parent_id"].map(id_to_name)
    df["parent_name"] = df["parentId"].map(id_to_name)
    df.loc[df["id"] == df["parentId"], "parent_name"] = None
    return df


def _filter_noise_events(df: pd.DataFrame) -> pd.DataFrame:
    if "name" in df.columns:
        return df[df["name"] != "User Action"]
    return df


def events_to_frame(trace: dict[str, Any]) -> pd.DataFrame:
    events = trace.get("events")
    if not isinstance(events, list):
        raise ValueError("Expected `events` to be a list in the trace JSON")

    df = pd.json_normalize(events)
    if df.empty:
        return df

    df["label"] = df.get("name")

    df = _ensure_parent_id(df)
    df = _add_time_columns(df)
    df = _add_hierarchy_columns(df)
    df = _filter_noise_events(df)
    return df


def load_traces(files: dict[str, str], base_path: str | Path | PathLike[str]) -> pd.DataFrame:
    base_path = Path(base_path)
    frames: list[pd.DataFrame] = []
    for filename, tag in files.items():
        frame = events_to_frame(read_trace_json(base_path / filename))
        frame["tag"] = tag
        frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
