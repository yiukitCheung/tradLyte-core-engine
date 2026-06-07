#!/bin/bash
# Build and deploy the snapshot_builder Lambda function.
#
# Maintains the consolidated long-format 1d OHLCV Parquet snapshot in S3 that
# the scanner reads. Self-contained: bundles only polars +
# psycopg2-binary (no shared/ modules — it resolves the RDS secret itself).
#
# Usage:
#   ./deploy_snapshot_lambda.sh              # update existing function (default)
#   ./deploy_snapshot_lambda.sh --create     # create function then deploy
#   ./deploy_snapshot_lambda.sh --help
#
# Required env vars (or set in .env):
#   AWS_REGION          (default: ca-west-1)
#   AWS_ACCOUNT_ID      (auto-detected via STS if not set)
#   RDS_SECRET_ARN      ARN of the Secrets Manager secret for RDS  (for --create)
#   S3_BUCKET_NAME      Datalake bucket (e.g. dev-condvest-datalake)
#
# Optional env vars:
#   FUNCTION_PREFIX     (default: dev-batch-)
#   LAMBDA_ROLE_ARN     IAM role for Lambda execution (auto-detected for --create)
#   LAMBDA_DEPLOY_BUCKET  S3 bucket for large packages (default: dev-condvest-lambda-deploy)
#   REFERENCE_FUNCTION  Existing Lambda to copy IAM role + VPC config from
#                       (default: dev-batch-daily-ohlcv-ingest-handler)

set -e

# ── path resolution ───────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_PROCESSING_DIR="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$(dirname "$INFRA_PROCESSING_DIR")"
BATCH_LAYER_DIR="$(dirname "$INFRA_DIR")"
CLOUD_DIR="$(dirname "$BATCH_LAYER_DIR")"

PROCESSING_DIR="$BATCH_LAYER_DIR/processing"

# ── configuration ─────────────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-ca-west-1}"
FUNCTION_PREFIX="${FUNCTION_PREFIX:-dev-batch-}"
FUNCTION_SUFFIX="scanner-snapshot-builder"
FUNCTION_NAME="${FUNCTION_PREFIX}${FUNCTION_SUFFIX}"   # dev-batch-scanner-snapshot-builder
FILE_NAME="snapshot_builder"
LAMBDA_HANDLER="${FILE_NAME}.lambda_handler"
LAMBDA_RUNTIME="python3.11"
LAMBDA_TIMEOUT=900       # 15 min — bootstrap reads years of history from RDS
LAMBDA_MEMORY=4096       # MB — full ~10M-row universe frame in RAM (bootstrap + incremental rewrite)
LAMBDA_EPHEMERAL=2048    # MB /tmp — stage existing + output Parquet on disk (avoids in-memory byte copies)
REFERENCE_FUNCTION="${REFERENCE_FUNCTION:-${FUNCTION_PREFIX}daily-ohlcv-ingest-handler}"

LAMBDA_DEPLOY_BUCKET="${LAMBDA_DEPLOY_BUCKET:-dev-condvest-lambda-deploy}"
PACKAGE_DIR="$SCRIPT_DIR/package/$FUNCTION_SUFFIX"
ZIP_FILE="$SCRIPT_DIR/${FUNCTION_SUFFIX}.zip"
REQUIREMENTS="$PROCESSING_DIR/lambda_functions/requirements.snapshot.txt"

# ── argument parsing ───────────────────────────────────────────────────────────
CREATE_MODE=false

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --create   Create the Lambda function before deploying (first-time setup)"
    echo "  --help     Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  AWS_REGION           Target region           (default: ca-west-1)"
    echo "  FUNCTION_PREFIX      Lambda name prefix      (default: dev-batch-)"
    echo "  RDS_SECRET_ARN       Secrets Manager ARN     (required for --create)"
    echo "  S3_BUCKET_NAME       Datalake S3 bucket      (required for --create)"
    echo "  LAMBDA_ROLE_ARN      IAM execution role ARN  (auto-detected for --create)"
    echo "  REFERENCE_FUNCTION   Lambda to copy role+VPC (default: dev-batch-daily-ohlcv-ingest-handler)"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --create) CREATE_MODE=true; shift ;;
        --help|-h) usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

PIP_CMD=$(command -v pip3 2>/dev/null || command -v pip 2>/dev/null || echo "pip3")
separator() { echo ""; echo "============================================================"; }

# ── pre-flight checks ─────────────────────────────────────────────────────────
separator
echo "  snapshot_builder Lambda — Deploy Script"
separator
echo "Region:       $AWS_REGION"
echo "Function:     $FUNCTION_NAME"
echo "Handler:      $LAMBDA_HANDLER"
echo "Source:       $PROCESSING_DIR/lambda_functions/${FILE_NAME}.py"
echo "Requirements: $REQUIREMENTS"
echo "Mode:         $( $CREATE_MODE && echo 'CREATE + DEPLOY' || echo 'UPDATE (code only)' )"
echo ""

if ! command -v aws &>/dev/null; then
    echo "ERROR: aws CLI not found. Install it and configure credentials first."
    exit 1
fi

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text --region "$AWS_REGION")}"
echo "Account ID:   $AWS_ACCOUNT_ID"

# ── build package ─────────────────────────────────────────────────────────────
separator
echo "  STEP 1 — Build deployment package"
separator

rm -rf "$PACKAGE_DIR" "$ZIP_FILE"
mkdir -p "$PACKAGE_DIR"

echo "Installing Python dependencies (Linux x86_64 / Python 3.11)..."
$PIP_CMD install \
    -r "$REQUIREMENTS" \
    -t "$PACKAGE_DIR" \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    --python-version 3.11 \
    --implementation cp \
    --no-cache-dir \
    --quiet 2>/dev/null \
|| \
$PIP_CMD install \
    -r "$REQUIREMENTS" \
    -t "$PACKAGE_DIR" \
    --no-cache-dir \
    --quiet

echo "Copying Lambda source + RDS connection helper..."
cp "$PROCESSING_DIR/lambda_functions/${FILE_NAME}.py" "$PACKAGE_DIR/${FILE_NAME}.py"
mkdir -p "$PACKAGE_DIR/clients"
cp "$CLOUD_DIR/shared/clients/rds_connection.py" "$PACKAGE_DIR/clients/rds_connection.py"
touch "$PACKAGE_DIR/clients/__init__.py"

echo "Removing cache files..."
find "$PACKAGE_DIR" -name "*.pyc" -delete
find "$PACKAGE_DIR" -name "*.pyo" -delete
find "$PACKAGE_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

echo "Creating ZIP..."
cd "$PACKAGE_DIR"
zip -r9 "$ZIP_FILE" . -x "*.pyc" "*/__pycache__/*" --quiet
cd "$SCRIPT_DIR"

SIZE=$(du -h "$ZIP_FILE" | cut -f1)
SIZE_BYTES=$(stat -f%z "$ZIP_FILE" 2>/dev/null || stat -c%s "$ZIP_FILE")
echo "Package ready: $ZIP_FILE ($SIZE)"

# ── create function (first-time only) ────────────────────────────────────────
if $CREATE_MODE; then
    separator
    echo "  STEP 2 — Create Lambda function"
    separator

    # Copy IAM role + VPC config from a reference Lambda that already reaches RDS.
    REF_CFG=$(aws lambda get-function-configuration \
        --function-name "$REFERENCE_FUNCTION" \
        --region "$AWS_REGION" \
        --output json 2>/dev/null || true)

    if [ -z "$LAMBDA_ROLE_ARN" ] && [ -n "$REF_CFG" ]; then
        LAMBDA_ROLE_ARN=$(echo "$REF_CFG" | jq -r '.Role')
    fi
    if [ -z "$LAMBDA_ROLE_ARN" ]; then
        echo "ERROR: LAMBDA_ROLE_ARN not set and could not be auto-detected from $REFERENCE_FUNCTION."
        rm -f "$ZIP_FILE"; exit 1
    fi

    VPC_ARGS=()
    if [ -n "$REF_CFG" ]; then
        SUBNETS=$(echo "$REF_CFG" | jq -r '.VpcConfig.SubnetIds | join(",")')
        SGS=$(echo "$REF_CFG" | jq -r '.VpcConfig.SecurityGroupIds | join(",")')
        if [ -n "$SUBNETS" ] && [ "$SUBNETS" != "null" ] && [ -n "$SGS" ] && [ "$SGS" != "null" ]; then
            VPC_ARGS=(--vpc-config "SubnetIds=$SUBNETS,SecurityGroupIds=$SGS")
            echo "VPC config:   subnets=[$SUBNETS] sgs=[$SGS]"
        fi
    fi

    echo "IAM Role:     $LAMBDA_ROLE_ARN"

    if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" &>/dev/null; then
        echo "Function $FUNCTION_NAME already exists — skipping create (will update code below)."
    else
        echo "Creating $FUNCTION_NAME..."
        aws lambda create-function \
            --function-name "$FUNCTION_NAME" \
            --runtime "$LAMBDA_RUNTIME" \
            --handler "$LAMBDA_HANDLER" \
            --role "$LAMBDA_ROLE_ARN" \
            --zip-file "fileb://$ZIP_FILE" \
            --timeout "$LAMBDA_TIMEOUT" \
            --memory-size "$LAMBDA_MEMORY" \
            --ephemeral-storage "Size=$LAMBDA_EPHEMERAL" \
            "${VPC_ARGS[@]}" \
            --environment "Variables={
                RDS_SECRET_ARN=${RDS_SECRET_ARN:-PLACEHOLDER_SET_IN_CONSOLE},
                S3_BUCKET_NAME=${S3_BUCKET_NAME:-dev-condvest-datalake},
                SNAPSHOT_PREFIX=scanner-snapshots,
                SNAPSHOT_RETENTION_DAYS=1825
            }" \
            --description "Builds/maintains the long-format 1d OHLCV Parquet snapshot for the scanner" \
            --region "$AWS_REGION" \
            --output json | jq '{FunctionName, Runtime, Handler, Timeout, MemorySize, LastModified}'

        echo "Function created."
        echo ""
        echo "NOTE: confirm RDS_SECRET_ARN is set, then run the one-time bootstrap:"
        echo "  aws lambda invoke --function-name $FUNCTION_NAME \\"
        echo "    --payload '{\"mode\":\"bootstrap\"}' --cli-binary-format raw-in-base64-out \\"
        echo "    --region $AWS_REGION response.json && cat response.json"
        rm -f "$ZIP_FILE"; rm -rf "$PACKAGE_DIR"
        exit 0
    fi
fi

# ── deploy (update code) ─────────────────────────────────────────────────────
separator
echo "  STEP $( $CREATE_MODE && echo 3 || echo 2 ) — Deploy to AWS Lambda"
separator

deploy_direct() {
    aws lambda update-function-code \
        --function-name "$1" \
        --zip-file "fileb://$ZIP_FILE" \
        --region "$AWS_REGION" \
        --output json | jq '{FunctionName, CodeSize, LastModified}'
}

deploy_via_s3() {
    local fname="$1"
    local s3_key="lambda-packages/${FUNCTION_SUFFIX}-$(date +%s).zip"

    echo "Package is large ($SIZE) — uploading via S3..."
    if ! aws s3 ls "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" &>/dev/null; then
        echo "Creating staging bucket: $LAMBDA_DEPLOY_BUCKET"
        aws s3 mb "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" 2>/dev/null || true
    fi

    aws s3 cp "$ZIP_FILE" "s3://$LAMBDA_DEPLOY_BUCKET/$s3_key" --region "$AWS_REGION"
    echo "Uploaded to s3://$LAMBDA_DEPLOY_BUCKET/$s3_key"

    aws lambda update-function-code \
        --function-name "$fname" \
        --s3-bucket "$LAMBDA_DEPLOY_BUCKET" \
        --s3-key "$s3_key" \
        --region "$AWS_REGION" \
        --output json | jq '{FunctionName, CodeSize, LastModified}'
}

deploy_to() {
    local fname="$1"
    if [ "$SIZE_BYTES" -gt 52428800 ]; then
        deploy_via_s3 "$fname"
    else
        deploy_direct "$fname"
    fi
}

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" &>/dev/null; then
    echo "Updating $FUNCTION_NAME..."
    deploy_to "$FUNCTION_NAME"
    echo "Deployed successfully."
else
    echo "ERROR: Function $FUNCTION_NAME not found in AWS."
    echo "First-time setup? Run with --create:  $0 --create"
    rm -f "$ZIP_FILE"; rm -rf "$PACKAGE_DIR"
    exit 1
fi

# ── cleanup ───────────────────────────────────────────────────────────────────
rm -f "$ZIP_FILE"; rm -rf "$PACKAGE_DIR"

# ── summary ───────────────────────────────────────────────────────────────────
separator
echo "  Deployment complete"
separator
echo ""
echo "Function:  $FUNCTION_NAME"
echo "Region:    $AWS_REGION"
echo ""
echo "Useful commands:"
echo ""
echo "  # One-time bootstrap (full rebuild from RDS)"
echo "  aws lambda invoke --function-name $FUNCTION_NAME \\"
echo "    --payload '{\"mode\":\"bootstrap\"}' --cli-binary-format raw-in-base64-out \\"
echo "    --region $AWS_REGION response.json && cat response.json"
echo ""
echo "  # Daily incremental (default mode) for a specific date"
echo "  aws lambda invoke --function-name $FUNCTION_NAME \\"
echo "    --payload '{\"scan_date\": \"$(date +%Y-%m-%d)\"}' --cli-binary-format raw-in-base64-out \\"
echo "    --region $AWS_REGION response.json && cat response.json"
echo ""
echo "  # Tail live logs"
echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow --region $AWS_REGION"
echo ""
