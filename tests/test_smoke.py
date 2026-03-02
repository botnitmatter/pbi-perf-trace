from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pbi_perf_trace import cli, pbi_perf_trace, run_all
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


def test_run_all_api_smoke(tmp_path: Path) -> None:
    sample = Path(__file__).parent / "data" / "perf_sample.json"
    out_csv = tmp_path / "output.csv"
    out_dir = tmp_path / "out"

    outputs = run_all(
        {"measure_on_import": sample},
        export_output=out_csv,
        out_dir=out_dir,
        write_outputs=True,
    )

    assert not outputs.pivoted.empty
    assert out_csv.is_file()
    assert (out_dir / "kept_summary.csv").is_file()
    assert (out_dir / "tag_official.csv").is_file()


def test_cli_autorun_with_config(tmp_path: Path, monkeypatch) -> None:
    sample = Path(__file__).parent / "data" / "perf_sample.json"
    monkeypatch.chdir(tmp_path)

    (tmp_path / "pbi-perf-trace.json").write_text(
        json.dumps(
            {
                "files": {"sample": str(sample)},
                "export_output": "output.csv",
                "out_dir": "out",
            }
        ),
        encoding="utf-8",
    )

    rc = cli.main([])
    assert rc == 0
    assert (tmp_path / "output.csv").is_file()
    assert (tmp_path / "out" / "tag_official.csv").is_file()


def test_cli_autorun_with_config_argv_none(tmp_path: Path, monkeypatch) -> None:
    sample = Path(__file__).parent / "data" / "perf_sample.json"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.sys, "argv", ["pbi-perf-trace"], raising=True)

    (tmp_path / "pbi-perf-trace.json").write_text(
        json.dumps({"files": {"sample": str(sample)}}),
        encoding="utf-8",
    )

    rc = cli.main(None)
    assert rc == 0
    assert (tmp_path / "output.csv").is_file()
    assert (tmp_path / "out" / "tag_official.csv").is_file()


def test_pbi_perf_trace_api_no_write() -> None:
    sample = Path(__file__).parent / "data" / "perf_sample.json"
    outputs = pbi_perf_trace({"sample": sample}, None)
    assert not outputs.pivoted.empty


def test_pbi_perf_trace_api_writes(tmp_path: Path) -> None:
    sample = Path(__file__).parent / "data" / "perf_sample.json"
    outputs = pbi_perf_trace({"sample": sample}, tmp_path)
    assert not outputs.pivoted.empty
    assert (tmp_path / "output.csv").is_file()
    assert (tmp_path / "tag_official.csv").is_file()
