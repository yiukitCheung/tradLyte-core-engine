# Archived batch jobs

Components removed from the active pipeline. Kept here for reference / one-off backfills.

| File | Original purpose |
|------|------------------|
| `resampler.py` | DuckDB + S3 Fibonacci resampling (3d, 5d, 8d, …) to a silver layer |
| `consolidator.py` | Merge bronze `date=*.parquet` into one `data.parquet` per symbol |
| `scan_partitioner.py` | Split the symbol universe into S3 chunk files for the old worker array |

These were retired by two pipeline changes:

- **Resampling moved to runtime** — multi-timeframe bars are computed on the fly from 1d (Polars `group_by_dynamic`), so the consolidator/resampler and silver tables are no longer written.
- **Scanner went single-pass** — the partitioner + 10-child Fargate worker array was replaced by the scanner Lambda (`dev-batch-scanner`), which scans the whole universe in one pass from the market snapshot.
