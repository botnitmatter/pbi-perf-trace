# pbi-perf-trace

Utilities for analyzing Power BI **Performance Analyzer** JSON exports.

## Install (dev)

```bash
pip install -e .[dev]
```

## Development

- Run lint: `ruff check .`
- Run tests (venv): `.\.venv\Scripts\python.exe -m pytest -q`
- Build dist: `.\.venv\Scripts\python.exe -m build`

## CLI

### Autorun (no args): export + analyze via config

If you run `python main.py` (or `pbi-perf-trace`) with no arguments, the tool will look for a
`pbi-perf-trace.json` file in the current directory and run **both** export + analyze.

Example `pbi-perf-trace.json`:

```json
{
	"files": {
		"measure_on_import": "C:/traces/perf_measure_on_import.json",
		"measure_on_direct": "C:/traces/perf_measure_on_direct.json"
	},
	"export_output": "output.csv",
	"out_dir": "out"
}
```

### Export: traces -> pivoted CSV

```bash
pbi-perf-trace export --base-path "C:\\path\\to\\folder" --files "perf_measure_on_import.json=measure_on_import" --files "perf_measure_on_direct.json=measure_on_direct" --output output.csv
```

### Analyze: run-level cache-aware summaries

```bash
pbi-perf-trace analyze --base-path "C:\\path\\to\\folder" --files "perf_measure_on_import.json=measure_on_import" --files "perf_measure_on_direct.json=measure_on_direct" --out-dir out
```

Outputs written to `--out-dir`:
- `kept_summary.csv`
- `cold_reason_summary.csv`
- `tag_official.csv`
- `cache_official.csv`

## Library usage

### Run both export + analyze (Python API)

`pbi_perf_trace()` is the main pip-friendly API. It accepts a dict mapping `tag -> trace_json_path`.

- If `output_path` is provided, it writes CSV outputs into that directory.
- It always returns DataFrames in-memory.

```python
from pathlib import Path

from pbi_perf_trace import pbi_perf_trace

outputs = pbi_perf_trace(
	{
		"measure_on_import": Path(r"C:\\traces\\perf_measure_on_import.json"),
		"measure_on_direct": Path(r"C:\\traces\\perf_measure_on_direct.json"),
	},
	output_path="out",
)

# DataFrames are also returned in-memory:
print(outputs.tag_official.head())
```

## License

MIT. See LICENSE.
