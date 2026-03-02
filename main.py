from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_src_layout() -> None:
    # Allow running `python main.py` from a repo that uses a `src/` layout
    # without requiring an editable install.
    root = Path(__file__).resolve().parent
    src = root / "src"
    src_str = str(src)
    if src.is_dir() and src_str not in sys.path:
        sys.path.insert(0, src_str)


def _run() -> int:
    _bootstrap_src_layout()
    from pbi_perf_trace.cli import main as cli_main

    return int(cli_main())


if __name__ == "__main__":
    raise SystemExit(_run())
