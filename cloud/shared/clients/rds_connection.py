"""RDS connection helpers shared across batch and serving runtimes."""

from __future__ import annotations

import json
import os
from typing import Optional

import boto3


def get_rds_connection_string(
    *,
    secret_arn: Optional[str] = None,
    region: Optional[str] = None,
) -> str:
    """
    Build a PostgreSQL URI from Secrets Manager.

    Reads ``RDS_SECRET_ARN`` and ``AWS_REGION`` from the environment when
    arguments are omitted.
    """
    secret_arn = (secret_arn or os.environ.get("RDS_SECRET_ARN") or "").strip()
    if not secret_arn:
        raise ValueError("RDS_SECRET_ARN environment variable not set")

    region = region or os.environ.get("AWS_REGION", "ca-west-1")
    client = boto3.client("secretsmanager", region_name=region)
    secret = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])

    host = secret["host"]
    port = secret.get("port", 5432)
    db = secret.get("database", secret.get("dbname", "postgres"))
    user = secret["username"]
    pwd = secret["password"]
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"
