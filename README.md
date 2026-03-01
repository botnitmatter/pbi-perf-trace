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

See the `pbi_perf_trace` package modules for programmatic usage.

## License

MIT. See LICENSE.
