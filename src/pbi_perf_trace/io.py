from __future__ import annotations

import json
from os import PathLike
from pathlib import Path
from typing import Any


def read_trace_json(path: str | Path | PathLike[str]) -> dict[str, Any]:
    """Read a performance trace JSON file (handles UTF-8 BOM)."""
    path = Path(path)
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)
