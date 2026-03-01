"""Power BI Performance Analyzer trace utilities."""

from importlib.metadata import PackageNotFoundError, version

from .analysis import AnalysisConfig, build_run_level_tables
from .io import read_trace_json
from .normalize import events_to_frame, load_traces
from .pivot import pivot_durations

try:
    __version__ = version("pbi-perf-trace")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = [
    "AnalysisConfig",
    "__version__",
    "build_run_level_tables",
    "events_to_frame",
    "load_traces",
    "read_trace_json",
    "pivot_durations",
]
