#!/usr/bin/env python3
"""
Repartition Silver Layer S3 Data (interval -> symbol -> year -> month)
======================================================================
Reads existing silver parquet files from the OLD layout (interval -> year -> month)
and rewrites them into the NEW layout (interval -> symbol -> year -> month).

OLD layout (no symbol in path):
  silver/silver_3d/year=2024/month=02/data_3d_202402.parquet
  or silver/silver_3d/2024/02/data_3d_202402.parquet

NEW layout (interval -> symbol -> year -> month; one file per month):
  silver/silver_3d/{symbol}/2024/02/data_3d_202402.parquet

Usage:
  python repartition_silver_s3.py --bucket my-bucket
  python repartition_silver_s3.py --bucket my-bucket --min-date 2000-01-01   # drop data before 2000 (default)
  python repartition_silver_s3.py --bucket my-bucket --intervals 3 5 --dry-run
  python repartition_silver_s3.py --bucket my-bucket --prefix-base silver --delete-old
  python repartition_silver_s3.py --bucket my-bucket --no-min-date            # keep all years (no filter)

Requires: boto3, pandas, pyarrow. Optional: python-dotenv for env vars.
"""

import os
import re
import sys
import argparse
import tempfile
from datetime import datetime
from typing import List, Optional, Tuple

import boto3
import pandas as pd

# Optional env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

S3_BUCKET_ENV = os.environ.get("S3_BUCKET", "")
AWS_REGION = os.environ.get("AWS_REGION", "ca-west-1")

INTERVALS = (3, 5, 8, 13, 21, 34)


def list_parquet_keys(s3_client, bucket: str, prefix: str) -> List[str]:
    """List all S3 keys under prefix that end with .parquet."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            k = obj["Key"]
            if k.endswith(".parquet"):
                keys.append(k)
    return keys


def is_old_layout_key(key: str, interval: int) -> bool:
    """True if key is old layout (no symbol in path)."""
    # Hive old: .../year=2024/month=02/...
    if "year=" in key and "month=" in key:
        return True
    # Plain old: .../silver_3d/2024/02/data_...
    if re.search(rf"silver_{interval}d/\d{{4}}/\d{{1,2}}/data_", key):
        return True
    return False


def repartition_one_key(
    s3_client,
    bucket: str,
    old_key: str,
    new_prefix_base: str,
    interval: int,
    dry_run: bool,
    delete_old: bool,
    min_date: Optional[datetime] = None,
) -> Tuple[int, Optional[str]]:
    """
    Read one parquet from old_key, split by symbol, write to new layout.
    If min_date is set, only rows with ts >= min_date are kept (drops older data).
    Returns (number of new keys written, error message if any).
    """
    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            s3_client.download_fileobj(bucket, old_key, tmp)
            tmp_path = tmp.name
        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
    except Exception as e:
        return 0, str(e)
    if "symbol" not in df.columns:
        return 0, "No 'symbol' column in parquet"
    ts_col = "ts" if "ts" in df.columns else "timestamp"
    if ts_col not in df.columns:
        return 0, "No 'ts' or 'timestamp' column"
    if min_date is not None:
        ser = pd.to_datetime(df[ts_col])
        # Align timezone: parquet may be tz-aware (e.g. Etc/UTC); min_date is naive
        tz = getattr(ser.dtype, "tz", None)
        if tz is not None:
            ser = ser.dt.tz_convert("UTC")
            threshold = pd.Timestamp(min_date).tz_localize("UTC")
        else:
            threshold = pd.Timestamp(min_date)
        df = df.loc[ser >= threshold].copy()
        if df.empty:
            return 0, None  # nothing to write after filter
    dt = pd.to_datetime(df[ts_col])
    df["year"] = dt.dt.year
    df["month"] = dt.dt.month.apply(lambda x: f"{x:02d}")
    prefix = f"{new_prefix_base}/silver_{interval}d".strip("/") if new_prefix_base else f"silver_{interval}d"
    written = 0
    # Partition: interval -> symbol -> year -> month (one file per month)
    for (symbol_val, year, month_str), group_df in df.groupby(["symbol", "year", "month"]):
        output_df = group_df.drop(columns=["year", "month"])
        new_key = f"{prefix}/{symbol_val}/{year}/{month_str}/data_{interval}d_{year}{month_str}.parquet"
        if dry_run:
            written += 1
            continue
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            output_df.to_parquet(tmp.name, engine="pyarrow", compression="snappy", index=False)
            s3_client.upload_file(tmp.name, bucket, new_key)
            os.unlink(tmp.name)
        written += 1
    if delete_old and written > 0 and not dry_run:
        s3_client.delete_object(Bucket=bucket, Key=old_key)
    return written, None


def main():
    parser = argparse.ArgumentParser(
        description="Repartition silver layer S3 data from interval/year/month to interval/symbol/year/month"
    )
    parser.add_argument("--bucket", default=S3_BUCKET_ENV, help="S3 bucket name (or set S3_BUCKET)")
    parser.add_argument(
        "--prefix-base",
        default="silver",
        help="Top-level prefix (default: silver -> silver/silver_3d). Use '' for silver_3d at root.",
    )
    parser.add_argument(
        "--intervals",
        type=int,
        nargs="+",
        default=INTERVALS,
        help=f"Intervals to repartition (default: {INTERVALS})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only list keys and print what would be done")
    parser.add_argument(
        "--delete-old",
        action="store_true",
        help="Delete old key after successfully writing new partitions (use with care)",
    )
    parser.add_argument(
        "--min-date",
        default="2000-01-01",
        metavar="YYYY-MM-DD",
        help="Only keep rows with ts >= this date; drops data before (default: 2000-01-01). Use --no-min-date to keep all.",
    )
    parser.add_argument(
        "--no-min-date",
        action="store_true",
        help="Do not filter by date; repartition all rows (ignore --min-date).",
    )
    parser.add_argument("--region", default=AWS_REGION, help="AWS region")
    args = parser.parse_args()

    if not args.bucket:
        print("Error: --bucket or S3_BUCKET env is required", file=sys.stderr)
        sys.exit(1)

    min_date = None
    if not args.no_min_date:
        try:
            min_date = datetime.strptime(args.min_date, "%Y-%m-%d")
            print(f"Filtering: only rows with ts >= {min_date.date()} will be written (data before is dropped).")
        except ValueError:
            print(f"Error: --min-date must be YYYY-MM-DD, got {args.min_date!r}", file=sys.stderr)
            sys.exit(1)
    else:
        print("No date filter (--no-min-date): repartitioning all rows.")

    s3_client = boto3.client("s3", region_name=args.region)
    prefix_base = args.prefix_base.strip("/") if args.prefix_base else ""
    total_old = 0
    total_new = 0
    errors = []

    for interval in args.intervals:
        prefix = f"{prefix_base}/silver_{interval}d".strip("/") if prefix_base else f"silver_{interval}d"
        keys = list_parquet_keys(s3_client, args.bucket, prefix + "/")
        old_layout_keys = [k for k in keys if is_old_layout_key(k, interval)]
        if not keys:
            print(f"  No parquet keys under {prefix}/")
            continue
        print(f"  Interval {interval}d: {len(old_layout_keys)} old-layout keys to repartition (of {len(keys)} total)")
        for old_key in old_layout_keys:
            total_old += 1
            n, err = repartition_one_key(
                s3_client,
                args.bucket,
                old_key,
                args.prefix_base or "",
                interval,
                args.dry_run,
                args.delete_old,
                min_date=min_date,
            )
            if err:
                errors.append(f"{old_key}: {err}")
            else:
                total_new += n
                if args.dry_run:
                    print(f"  Would repartition: {old_key} -> {n} new key(s)")
                else:
                    print(f"  Repartitioned: {old_key} -> {n} new key(s)")

    print(f"\nDone. Old keys processed: {total_old}, new keys written: {total_new}")
    if errors:
        print(f"Errors ({len(errors)}):", file=sys.stderr)
        for e in errors[:20]:
            print(f"  {e}", file=sys.stderr)
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more", file=sys.stderr)
    if args.dry_run:
        print("(Dry run: no data written.)")


if __name__ == "__main__":
    main()
