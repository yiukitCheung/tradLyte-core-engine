#!/bin/bash
# Build and deploy the scanner Lambda function (dev-batch-scanner).
#
# Single-pass full-universe scanner. Reads the long-format snapshot from S3,
# runs all strategies across the whole universe with Polars, and writes raw
# signals to daily_scan_signals (the staging table the aggregator reads).
#
# Self-contained: bundles polars + psycopg2-binary + pydantic + the whole
# analytics_core package (so the runner uses the shared, partition-aware
# strategy library — the same code the backtester runs).
#
# Usage:
#   ./deploy_scanner_lambda.sh            # update existing function
#   ./deploy_scanner_lambda.sh --create   # create then deploy
#   ./deploy_scanner_lambda.sh --help
#
# Required for --create:
#   RDS_SECRET_ARN, S3_BUCKET_NAME (or defaults)
# Optional:
#   FUNCTION_PREFIX (default dev-batch-), LAMBDA_ROLE_ARN, LAMBDA_DEPLOY_BUCKET,
#   REFERENCE_FUNCTION (default dev-batch-scanner-snapshot-builder — copies role+VPC)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_PROCESSING_DIR="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$(dirname "$INFRA_PROCESSING_DIR")"
BATCH_LAYER_DIR="$(dirname "$INFRA_DIR")"
CLOUD_DIR="$(dirname "$BATCH_LAYER_DIR")"

PROCESSING_DIR="$BATCH_LAYER_DIR/processing"
SHARED_DIR="$CLOUD_DIR/shared"

AWS_REGION="${AWS_REGION:-ca-west-1}"
FUNCTION_PREFIX="${FUNCTION_PREFIX:-dev-batch-}"
FUNCTION_SUFFIX="scanner"
FUNCTION_NAME="${FUNCTION_PREFIX}${FUNCTION_SUFFIX}"   # dev-batch-scanner
FILE_NAME="scanner"
LAMBDA_HANDLER="${FILE_NAME}.lambda_handler"
LAMBDA_RUNTIME="python3.11"
LAMBDA_TIMEOUT=900       # 15 min (runs in seconds; generous headroom)
LAMBDA_MEMORY=8192       # MB — full-universe frame + 1d/3d/5d indicators in RAM
LAMBDA_EPHEMERAL=2048    # MB /tmp — stage the snapshot parquet
REFERENCE_FUNCTION="${REFERENCE_FUNCTION:-${FUNCTION_PREFIX}scanner-snapshot-builder}"

LAMBDA_DEPLOY_BUCKET="${LAMBDA_DEPLOY_BUCKET:-dev-condvest-lambda-deploy}"
PACKAGE_DIR="$SCRIPT_DIR/package/$FUNCTION_SUFFIX"
ZIP_FILE="$SCRIPT_DIR/${FUNCTION_SUFFIX}.zip"
REQUIREMENTS="$PROCESSING_DIR/lambda_functions/requirements.scanner.txt"

CREATE_MODE=false
usage() {
    echo "Usage: $0 [--create] [--help]"
    echo "  --create   Create the Lambda (copies role+VPC from $REFERENCE_FUNCTION)"
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

separator
echo "  Scanner Lambda — Deploy"
separator
echo "Region:       $AWS_REGION"
echo "Function:     $FUNCTION_NAME"
echo "Handler:      $LAMBDA_HANDLER"
echo "Source:       $PROCESSING_DIR/lambda_functions/${FILE_NAME}.py"
echo "Bundled pkg:  $SHARED_DIR/analytics_core (partition-aware strategy library)"
echo "Mode:         $( $CREATE_MODE && echo 'CREATE + DEPLOY' || echo 'UPDATE (code only)' )"

if ! command -v aws &>/dev/null; then echo "ERROR: aws CLI not found."; exit 1; fi
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text --region "$AWS_REGION")}"
echo "Account ID:   $AWS_ACCOUNT_ID"

# ── build package ──────────────────────────────────────────────────────────
separator; echo "  STEP 1 — Build deployment package"; separator
rm -rf "$PACKAGE_DIR" "$ZIP_FILE"
mkdir -p "$PACKAGE_DIR"

echo "Installing Python dependencies (Linux x86_64 / Python 3.11)..."
$PIP_CMD install -r "$REQUIREMENTS" -t "$PACKAGE_DIR" \
    --platform manylinux2014_x86_64 --only-binary=:all: \
    --python-version 3.11 --implementation cp --no-cache-dir --quiet 2>/dev/null \
|| $PIP_CMD install -r "$REQUIREMENTS" -t "$PACKAGE_DIR" --no-cache-dir --quiet

echo "Copying Lambda source + bundled shared packages..."
cp "$PROCESSING_DIR/lambda_functions/${FILE_NAME}.py" "$PACKAGE_DIR/${FILE_NAME}.py"
rm -rf "$PACKAGE_DIR/analytics_core"
cp -r "$SHARED_DIR/analytics_core" "$PACKAGE_DIR/analytics_core"
rm -rf "$PACKAGE_DIR/analytics_core/_spikes" \
       "$PACKAGE_DIR/analytics_core/tests" \
       "$PACKAGE_DIR/analytics_core"/*.egg-info
mkdir -p "$PACKAGE_DIR/clients" "$PACKAGE_DIR/database/sql"
cp "$SHARED_DIR/clients/rds_connection.py" "$PACKAGE_DIR/clients/rds_connection.py"
touch "$PACKAGE_DIR/clients/__init__.py"
cp "$SHARED_DIR/database/staging.py" "$PACKAGE_DIR/database/staging.py"
cp "$SHARED_DIR/database/sql/daily_scan_signals.sql" "$PACKAGE_DIR/database/sql/daily_scan_signals.sql"
touch "$PACKAGE_DIR/database/__init__.py"

echo "Removing cache files..."
find "$PACKAGE_DIR" -name "*.pyc" -delete
find "$PACKAGE_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

echo "Creating ZIP..."
cd "$PACKAGE_DIR"; zip -r9 "$ZIP_FILE" . -x "*.pyc" "*/__pycache__/*" --quiet; cd "$SCRIPT_DIR"
SIZE=$(du -h "$ZIP_FILE" | cut -f1)
SIZE_BYTES=$(stat -f%z "$ZIP_FILE" 2>/dev/null || stat -c%s "$ZIP_FILE")
echo "Package ready: $ZIP_FILE ($SIZE)"

# ── create (first-time) ──────────────────────────────────────────────────────
if $CREATE_MODE; then
    separator; echo "  STEP 2 — Create Lambda function"; separator
    REF_CFG=$(aws lambda get-function-configuration --function-name "$REFERENCE_FUNCTION" --region "$AWS_REGION" --output json 2>/dev/null || true)
    if [ -z "$LAMBDA_ROLE_ARN" ] && [ -n "$REF_CFG" ]; then
        LAMBDA_ROLE_ARN=$(echo "$REF_CFG" | jq -r '.Role')
    fi
    if [ -z "$LAMBDA_ROLE_ARN" ]; then echo "ERROR: LAMBDA_ROLE_ARN not set / not detectable from $REFERENCE_FUNCTION."; rm -f "$ZIP_FILE"; exit 1; fi

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
        echo "Function exists — skipping create (will update code below)."
    else
        # Large packages (polars) exceed the direct create request limit — stage via S3.
        CODE_ARGS=(--zip-file "fileb://$ZIP_FILE")
        if [ "$SIZE_BYTES" -gt 52428800 ]; then
            S3_KEY="lambda-packages/${FUNCTION_SUFFIX}-create-$(date +%s).zip"
            echo "Package is large ($SIZE) — staging via s3://$LAMBDA_DEPLOY_BUCKET/$S3_KEY ..."
            aws s3 ls "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" &>/dev/null || aws s3 mb "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" 2>/dev/null || true
            aws s3 cp "$ZIP_FILE" "s3://$LAMBDA_DEPLOY_BUCKET/$S3_KEY" --region "$AWS_REGION"
            CODE_ARGS=(--code "S3Bucket=$LAMBDA_DEPLOY_BUCKET,S3Key=$S3_KEY")
        fi
        aws lambda create-function \
            --function-name "$FUNCTION_NAME" \
            --runtime "$LAMBDA_RUNTIME" --handler "$LAMBDA_HANDLER" --role "$LAMBDA_ROLE_ARN" \
            "${CODE_ARGS[@]}" --timeout "$LAMBDA_TIMEOUT" --memory-size "$LAMBDA_MEMORY" \
            --ephemeral-storage "Size=$LAMBDA_EPHEMERAL" "${VPC_ARGS[@]}" \
            --environment "Variables={
                RDS_SECRET_ARN=${RDS_SECRET_ARN:-PLACEHOLDER_SET_IN_CONSOLE},
                S3_BUCKET_NAME=${S3_BUCKET_NAME:-dev-condvest-datalake},
                SNAPSHOT_PREFIX=scanner-snapshots,
                SCAN_WINDOW_DAYS=1095
            }" \
            --description "Full-universe scanner: snapshot -> daily_scan_signals (single Polars pass)" \
            --region "$AWS_REGION" --output json | jq '{FunctionName, Runtime, Handler, Timeout, MemorySize, LastModified}'
        echo "Function created."
        echo ""
        echo "Test invoke:"
        echo "  aws lambda invoke --function-name $FUNCTION_NAME \\"
        echo "    --payload '{\"scan_date\":\"$(date +%Y-%m-%d)\"}' --cli-binary-format raw-in-base64-out \\"
        echo "    --region $AWS_REGION response.json && cat response.json"
        rm -f "$ZIP_FILE"; rm -rf "$PACKAGE_DIR"; exit 0
    fi
fi

# ── deploy (update code) ─────────────────────────────────────────────────────
separator; echo "  STEP $( $CREATE_MODE && echo 3 || echo 2 ) — Deploy to AWS Lambda"; separator

deploy_direct() {
    aws lambda update-function-code --function-name "$1" --zip-file "fileb://$ZIP_FILE" \
        --region "$AWS_REGION" --output json | jq '{FunctionName, CodeSize, LastModified}'
}
deploy_via_s3() {
    local fname="$1"; local s3_key="lambda-packages/${FUNCTION_SUFFIX}-$(date +%s).zip"
    echo "Package is large ($SIZE) — uploading via S3..."
    aws s3 ls "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" &>/dev/null || aws s3 mb "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" 2>/dev/null || true
    aws s3 cp "$ZIP_FILE" "s3://$LAMBDA_DEPLOY_BUCKET/$s3_key" --region "$AWS_REGION"
    aws lambda update-function-code --function-name "$fname" \
        --s3-bucket "$LAMBDA_DEPLOY_BUCKET" --s3-key "$s3_key" \
        --region "$AWS_REGION" --output json | jq '{FunctionName, CodeSize, LastModified}'
}
deploy_to() {
    if [ "$SIZE_BYTES" -gt 52428800 ]; then deploy_via_s3 "$1"; else deploy_direct "$1"; fi
}

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" &>/dev/null; then
    echo "Updating $FUNCTION_NAME..."; deploy_to "$FUNCTION_NAME"
    aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --handler "$LAMBDA_HANDLER" \
        --region "$AWS_REGION" --output json | jq '{FunctionName, Handler, LastModified}'
    echo "Deployed successfully."
else
    echo "ERROR: Function $FUNCTION_NAME not found. First-time setup? Run: $0 --create"
    rm -f "$ZIP_FILE"; rm -rf "$PACKAGE_DIR"; exit 1
fi

rm -f "$ZIP_FILE"; rm -rf "$PACKAGE_DIR"

separator; echo "  Deployment complete"; separator
echo "Function:  $FUNCTION_NAME"
echo ""
echo "  # Test invoke for a specific date"
echo "  aws lambda invoke --function-name $FUNCTION_NAME \\"
echo "    --payload '{\"scan_date\":\"$(date +%Y-%m-%d)\"}' --cli-binary-format raw-in-base64-out \\"
echo "    --region $AWS_REGION response.json && cat response.json"
echo ""
echo "  # Tail logs"
echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow --region $AWS_REGION"
