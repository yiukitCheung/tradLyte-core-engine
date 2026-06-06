# Database Migration

Tools to migrate data from the local Docker PostgreSQL to AWS RDS PostgreSQL.

| Script | Use for |
|---|---|
| `migrate_fast.py` | Bulk migration of large tables (1M+ rows) — PostgreSQL `COPY`, drops indexes during load |
| `migrate.py` | Small datasets / incremental updates — per-row UPSERT with conflict handling |

## Prerequisites

- Local Docker PostgreSQL running
- AWS credentials configured (for Secrets Manager)
- `pip install psycopg2-binary boto3 python-dotenv`
- RDS connection values in a local `.env` (`RDS_HOST`, `RDS_PORT`, `RDS_DATABASE`, `RDS_USER`, and the password sourced from Secrets Manager) — do not commit credentials

## Run

```bash
# Bulk (recommended for large tables)
python migrate_fast.py --use-test-tables --method copy --skip-indexes

# Incremental / small tables
python migrate.py --use-test-tables --batch-size 10000

# Single table
python migrate.py --table raw_ohlcv
```

Both scripts upsert (existing rows are updated), verify record counts after the run, and are safe to re-run if interrupted.
