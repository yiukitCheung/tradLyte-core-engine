#!/bin/bash
# Build and deploy the scan_partitioner Lambda function.
#
# Usage:
#   ./deploy_processing_lambda.sh              # update existing function (default)
#   ./deploy_processing_lambda.sh --create     # create function then deploy
#   ./deploy_processing_lambda.sh --help
#
# Required env vars (or set in .env):
#   AWS_REGION          (default: ca-west-1)
#   AWS_ACCOUNT_ID      (auto-detected via STS if not set)
#   RDS_SECRET_ARN      ARN of the Secrets Manager secret for RDS
#   S3_BUCKET_NAME      Datalake bucket (e.g. dev-condvest-datalake)
#
# Optional env vars:
#   FUNCTION_PREFIX     (default: dev-batch-)
#   LAMBDA_ROLE_ARN     IAM role for Lambda execution (needed for --create)
#   LAMBDA_DEPLOY_BUCKET  S3 bucket for large packages  (default: dev-condvest-lambda-deploy)

set -e

# ── path resolution ───────────────────────────────────────────────────────────
# Script lives at: cloud/batch_layer/infrastructure/processing/lambda_functions/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_PROCESSING_DIR="$(dirname "$SCRIPT_DIR")"        # infrastructure/processing/lambda_functions → infrastructure/processing
INFRA_DIR="$(dirname "$INFRA_PROCESSING_DIR")"         # → infrastructure
BATCH_LAYER_DIR="$(dirname "$INFRA_DIR")"              # → batch_layer
CLOUD_DIR="$(dirname "$BATCH_LAYER_DIR")"              # → cloud

PROCESSING_DIR="$BATCH_LAYER_DIR/processing"          # cloud/batch_layer/processing  (application code)
SHARED_DIR="$CLOUD_DIR/shared"                         # cloud/shared

# ── configuration ─────────────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-ca-west-1}"
FUNCTION_PREFIX="${FUNCTION_PREFIX:-dev-batch-}"
FUNCTION_SUFFIX="scan-partitioner"
FUNCTION_NAME="${FUNCTION_PREFIX}${FUNCTION_SUFFIX}"   # dev-batch-scan-partitioner
FILE_NAME="scan_partitioner"                           # scan_partitioner.py
LAMBDA_HANDLER="${FILE_NAME}.lambda_handler"
LAMBDA_RUNTIME="python3.11"
LAMBDA_TIMEOUT=300     # 5 min — RDS query + 10 S3 writes
LAMBDA_MEMORY=256      # MB — minimal (no heavy computation)

LAMBDA_DEPLOY_BUCKET="${LAMBDA_DEPLOY_BUCKET:-dev-condvest-lambda-deploy}"
PACKAGE_DIR="$SCRIPT_DIR/package/$FUNCTION_SUFFIX"
ZIP_FILE="$SCRIPT_DIR/${FUNCTION_SUFFIX}.zip"

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
    echo "  LAMBDA_ROLE_ARN      IAM execution role ARN  (required for --create)"
    echo "  LAMBDA_DEPLOY_BUCKET Staging bucket for large packages"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --create) CREATE_MODE=true; shift ;;
        --help|-h) usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

# ── helpers ───────────────────────────────────────────────────────────────────
PIP_CMD=$(command -v pip3 2>/dev/null || command -v pip 2>/dev/null || echo "pip3")

separator() { echo ""; echo "============================================================"; }

# ── pre-flight checks ─────────────────────────────────────────────────────────
separator
echo "  scan_partitioner Lambda — Deploy Script"
separator
echo "Region:       $AWS_REGION"
echo "Function:     $FUNCTION_NAME"
echo "Handler:      $LAMBDA_HANDLER"
echo "Source:       $PROCESSING_DIR/lambda_functions/${FILE_NAME}.py"
echo "Shared:       $SHARED_DIR"
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
    -r "$PROCESSING_DIR/lambda_functions/requirements.txt" \
    -t "$PACKAGE_DIR" \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    --python-version 3.11 \
    --implementation cp \
    --no-cache-dir \
    --quiet 2>/dev/null \
|| \
$PIP_CMD install \
    -r "$PROCESSING_DIR/lambda_functions/requirements.txt" \
    -t "$PACKAGE_DIR" \
    --no-cache-dir \
    --quiet

echo "Copying Lambda source..."
cp "$PROCESSING_DIR/lambda_functions/${FILE_NAME}.py" "$PACKAGE_DIR/${FILE_NAME}.py"

echo "Copying shared modules..."
# The partitioner only needs the RDS client — no Polygon, no analytics_core.
mkdir -p "$PACKAGE_DIR/shared/clients"
mkdir -p "$PACKAGE_DIR/shared/models"

cp "$SHARED_DIR/__init__.py"                           "$PACKAGE_DIR/shared/"
cp "$SHARED_DIR/clients/rds_timescale_client.py"       "$PACKAGE_DIR/shared/clients/"
cp -r "$SHARED_DIR/models/"*                           "$PACKAGE_DIR/shared/models/" 2>/dev/null || true

cat > "$PACKAGE_DIR/shared/clients/__init__.py" << 'PYEOF'
from .rds_timescale_client import RDSTimescaleClient
__all__ = ['RDSTimescaleClient']
PYEOF

cat > "$PACKAGE_DIR/shared/models/__init__.py" << 'PYEOF'
PYEOF

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

    if [ -z "$LAMBDA_ROLE_ARN" ]; then
        # Try to reuse the role from an existing Lambda in this account
        LAMBDA_ROLE_ARN=$(aws lambda get-function-configuration \
            --function-name "${FUNCTION_PREFIX}daily-ohlcv-fetcher" \
            --region "$AWS_REGION" \
            --query 'Role' --output text 2>/dev/null || true)
    fi

    if [ -z "$LAMBDA_ROLE_ARN" ]; then
        echo "ERROR: LAMBDA_ROLE_ARN is not set and could not be auto-detected."
        echo "       Export it before running with --create:"
        echo "       export LAMBDA_ROLE_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:role/<your-lambda-role>"
        rm -f "$ZIP_FILE"
        exit 1
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
            --environment "Variables={
                RDS_SECRET_ARN=${RDS_SECRET_ARN:-PLACEHOLDER_SET_IN_CONSOLE},
                S3_BUCKET_NAME=${S3_BUCKET_NAME:-dev-condvest-datalake},
                AWS_REGION_OVERRIDE=$AWS_REGION
            }" \
            --description "Scanner partitioner: splits active symbols into S3 chunk files for Batch array job" \
            --region "$AWS_REGION" \
            --output json | jq '{FunctionName, Runtime, Handler, Timeout, MemorySize, LastModified}'

        echo "Function created."
        echo ""
        echo "NOTE: Set RDS_SECRET_ARN in the Lambda environment before invoking:"
        echo "  aws lambda update-function-configuration \\"
        echo "    --function-name $FUNCTION_NAME \\"
        echo "    --environment 'Variables={RDS_SECRET_ARN=<your-arn>,S3_BUCKET_NAME=dev-condvest-datalake}' \\"
        echo "    --region $AWS_REGION"
        rm -f "$ZIP_FILE"
        rm -rf "$PACKAGE_DIR"
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
    echo ""
    echo "First-time setup? Run with --create:"
    echo "  $0 --create"
    echo ""
    echo "Or create it manually in the AWS Console, then re-run this script."
    rm -f "$ZIP_FILE"
    rm -rf "$PACKAGE_DIR"
    exit 1
fi

# ── cleanup ───────────────────────────────────────────────────────────────────
rm -f "$ZIP_FILE"
rm -rf "$PACKAGE_DIR"

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
echo "  # Tail live logs"
echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow --region $AWS_REGION"
echo ""
echo "  # Test invoke (today's date, 10 chunks)"
echo "  aws lambda invoke \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    --payload '{\"array_size\": 10}' \\"
echo "    --cli-binary-format raw-in-base64-out \\"
echo "    --region $AWS_REGION \\"
echo "    response.json && cat response.json"
echo ""
echo "  # Test invoke for a specific date"
echo "  aws lambda invoke \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    --payload '{\"scan_date\": \"$(date +%Y-%m-%d)\", \"array_size\": 10}' \\"
echo "    --cli-binary-format raw-in-base64-out \\"
echo "    --region $AWS_REGION \\"
echo "    response.json && cat response.json"
echo ""
