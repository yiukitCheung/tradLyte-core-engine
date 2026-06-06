# Scanner snapshot storage lifecycle

The snapshot builder (`snapshot_builder.py`) writes to `s3://<datalake>/scanner-snapshots/`:

| Key | Purpose | Retention |
|-----|---------|-----------|
| `scanner-snapshots/latest/market_1d.parquet` | Stable key the vectorized scanner always reads; overwritten each run | Permanent |
| `scanner-snapshots/history/<date>/market_1d.parquet` | Point-in-time dated copy | 14 days (lifecycle) |

Dated copies live under a dedicated `history/` subprefix so the expiry rule targets them without ever touching the permanent `latest/` object.

## Apply / verify

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket dev-condvest-datalake \
  --lifecycle-configuration file://scanner_snapshot_lifecycle.json \
  --region ca-west-1

aws s3api get-bucket-lifecycle-configuration \
  --bucket dev-condvest-datalake --region ca-west-1
```

Rules: `expire-scanner-snapshot-history` deletes `history/` copies after 14 days; `abort-incomplete-mpu-scanner-snapshots` cleans up failed multipart uploads after 7 days.
