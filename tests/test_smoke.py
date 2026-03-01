from __future__ import annotations

from pathlib import Path

import pandas as pd

from pbi_perf_trace.analysis import AnalysisConfig, build_run_level_tables
from pbi_perf_trace.metadata import event_metadata_frame
from pbi_perf_trace.normalize import events_to_frame, read_trace_json
from pbi_perf_trace.pivot import pivot_durations


def test_smoke_pipeline_runs() -> None:
    sample = Path(__file__).parent / "data" / "perf_sample.json"
    trace = read_trace_json(sample)
    events = events_to_frame(trace)
    events["tag"] = "measure_on_import"

    # Enrich with metadata
    meta = event_metadata_frame()
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
    events = events[[c for c in select_cols if c in events.columns]]
    enriched = events.merge(meta, left_on="label", right_on="Event", how="left")

    # Pivot works
    engine_only = enriched[
        ~enriched["responsibility"].fillna("").str.contains(r"Client|System", regex=True)
    ].copy()
    pivoted = pivot_durations(engine_only)
    assert not pivoted.empty

    # Analysis runs
    out = build_run_level_tables(
        enriched,
        AnalysisConfig(outlier_keep_pct=1.0, outlier_min_runs_per_group=1),
    )
    assert isinstance(out["run_total"], pd.DataFrame)
    assert "run_total_ms" in out["run_total"].columns
