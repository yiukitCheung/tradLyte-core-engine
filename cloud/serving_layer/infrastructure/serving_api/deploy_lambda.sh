#!/usr/bin/env bash
# Build and deploy the FastAPI serving Lambda (zip package).
#
# Usage:
#   AWS_REGION=ca-west-1 FUNCTION_NAME=dev-serving-api \
#   SOURCE_VPC_LAMBDA=dev-batch-daily-ohlcv-ingest-handler \
#   RDS_SECRET_ARN=arn:aws:secretsmanager:...:secret:... \
#   SERVING_API_KEY=change-me \
#   SERVING_API_KEY_SECRET_ARN=arn:aws:secretsmanager:...:secret:... \
#   POLYGON_API_KEY_SECRET_ARN=arn:aws:secretsmanager:...:secret:... \
#   ./cloud/serving_layer/infrastructure/serving_api/deploy_lambda.sh

set -euo pipefail
export AWS_PAGER=""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
SERVING_DIR="$REPO_ROOT/cloud/serving_layer/lambda_functions/serving_api"
COMMON_DIR="$REPO_ROOT/cloud/batch_layer/infrastructure/common"

# shellcheck source=/dev/null
source "$COMMON_DIR/pip_for_lambda.sh"

AWS_REGION="${AWS_REGION:-ca-west-1}"
FUNCTION_NAME="${FUNCTION_NAME:-dev-serving-api}"
LAMBDA_RUNTIME="${LAMBDA_RUNTIME:-python3.11}"
LAMBDA_ARCH="${LAMBDA_ARCH:-x86_64}"
LAMBDA_TIMEOUT="${LAMBDA_TIMEOUT:-10}"
LAMBDA_MEMORY="${LAMBDA_MEMORY:-512}"
LAMBDA_RESERVED_CONCURRENCY="${LAMBDA_RESERVED_CONCURRENCY:-50}"
SOURCE_VPC_LAMBDA="${SOURCE_VPC_LAMBDA:-dev-batch-daily-ohlcv-ingest-handler}"
LAMBDA_ROLE_ARN="${LAMBDA_ROLE_ARN:-}"

RDS_SECRET_ARN="${RDS_SECRET_ARN:-}"
SERVING_API_KEY="${SERVING_API_KEY:-}"
SERVING_API_KEY_SECRET_ARN="${SERVING_API_KEY_SECRET_ARN:-}"
POLYGON_API_KEY_SECRET_ARN="${POLYGON_API_KEY_SECRET_ARN:-}"
ALLOWED_ORIGIN="${ALLOWED_ORIGIN:-*}"
SCREENER_CACHE_TTL_S="${SCREENER_CACHE_TTL_S:-60}"
RETURNS_CACHE_TTL_S="${RETURNS_CACHE_TTL_S:-300}"
BACKTEST_FUNCTION_NAME="${BACKTEST_FUNCTION_NAME:-dev-serving-backtester}"
LAMBDA_UPLOAD_MAX_BYTES="${LAMBDA_UPLOAD_MAX_BYTES:-69000000}"
LAMBDA_DEPLOY_BUCKET="${LAMBDA_DEPLOY_BUCKET:-dev-condvest-lambda-deploy}"

PACKAGE_DIR="$SCRIPT_DIR/package"
ZIP_PATH="$SCRIPT_DIR/${FUNCTION_NAME}.zip"

cleanup() {
  rm -rf "$PACKAGE_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

wait_for_lambda_update_ready() {
  local attempts="${1:-30}"
  local sleep_seconds="${2:-5}"
  local i
  for ((i = 1; i <= attempts; i++)); do
    local status
    status="$(aws lambda get-function-configuration \
      --function-name "$FUNCTION_NAME" \
      --region "$AWS_REGION" \
      --query 'LastUpdateStatus' \
      --output text 2>/dev/null || echo "Unknown")"
    if [[ "$status" == "Successful" || "$status" == "Failed" ]]; then
      return 0
    fi
    echo "⏳ Lambda update in progress (status=$status), waiting ${sleep_seconds}s..."
    sleep "$sleep_seconds"
  done
  echo "❌ Timed out waiting for Lambda update readiness."
  return 1
}

zip_size_bytes() {
  local file_path="$1"
  if stat -f%z "$file_path" >/dev/null 2>&1; then
    stat -f%z "$file_path"
  else
    stat -c%s "$file_path"
  fi
}

ensure_deploy_bucket_exists() {
  if ! aws s3 ls "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "🪣 Creating deploy bucket: s3://$LAMBDA_DEPLOY_BUCKET"
    aws s3 mb "s3://$LAMBDA_DEPLOY_BUCKET" --region "$AWS_REGION" >/dev/null
  fi
}

update_lambda_code_auto() {
  local function_name="$1"
  local zip_path="$2"
  local size_bytes
  size_bytes="$(zip_size_bytes "$zip_path")"

  if (( size_bytes < LAMBDA_UPLOAD_MAX_BYTES )); then
    echo "⬆️  Updating function code via direct upload (${size_bytes} bytes)"
    aws lambda update-function-code \
      --function-name "$function_name" \
      --zip-file "fileb://$zip_path" \
      --region "$AWS_REGION" >/dev/null
    return
  fi

  ensure_deploy_bucket_exists
  local s3_key="lambda-packages/${function_name}-$(date +%Y%m%d%H%M%S).zip"
  echo "⬆️  Package too large for direct upload (${size_bytes} bytes). Using S3: s3://$LAMBDA_DEPLOY_BUCKET/$s3_key"
  aws s3 cp "$zip_path" "s3://$LAMBDA_DEPLOY_BUCKET/$s3_key" --region "$AWS_REGION" >/dev/null
  aws lambda update-function-code \
    --function-name "$function_name" \
    --s3-bucket "$LAMBDA_DEPLOY_BUCKET" \
    --s3-key "$s3_key" \
    --region "$AWS_REGION" >/dev/null
}

create_lambda_auto() {
  local function_name="$1"
  local role_arn="$2"
  local zip_path="$3"
  local size_bytes
  size_bytes="$(zip_size_bytes "$zip_path")"

  if (( size_bytes < LAMBDA_UPLOAD_MAX_BYTES )); then
    echo "➕ Creating function via direct upload (${size_bytes} bytes)"
    aws lambda create-function \
      --function-name "$function_name" \
      --runtime "$LAMBDA_RUNTIME" \
      --role "$role_arn" \
      --handler "serving_api.handler.lambda_handler" \
      --architectures "$LAMBDA_ARCH" \
      --zip-file "fileb://$zip_path" \
      --timeout "$LAMBDA_TIMEOUT" \
      --memory-size "$LAMBDA_MEMORY" \
      --vpc-config "SubnetIds=${SUBNET_IDS_CSV},SecurityGroupIds=${SECURITY_GROUP_IDS_CSV}" \
      --region "$AWS_REGION" >/dev/null
    return
  fi

  ensure_deploy_bucket_exists
  local s3_key="lambda-packages/${function_name}-$(date +%Y%m%d%H%M%S).zip"
  echo "➕ Package too large for direct create (${size_bytes} bytes). Using S3: s3://$LAMBDA_DEPLOY_BUCKET/$s3_key"
  aws s3 cp "$zip_path" "s3://$LAMBDA_DEPLOY_BUCKET/$s3_key" --region "$AWS_REGION" >/dev/null
  aws lambda create-function \
    --function-name "$function_name" \
    --runtime "$LAMBDA_RUNTIME" \
    --role "$role_arn" \
    --handler "serving_api.handler.lambda_handler" \
    --architectures "$LAMBDA_ARCH" \
    --code "S3Bucket=$LAMBDA_DEPLOY_BUCKET,S3Key=$s3_key" \
    --timeout "$LAMBDA_TIMEOUT" \
    --memory-size "$LAMBDA_MEMORY" \
    --vpc-config "SubnetIds=${SUBNET_IDS_CSV},SecurityGroupIds=${SECURITY_GROUP_IDS_CSV}" \
    --region "$AWS_REGION" >/dev/null
}

echo "🚀 Deploying Lambda: $FUNCTION_NAME"
echo "Region: $AWS_REGION"

if ! aws lambda get-function --function-name "$SOURCE_VPC_LAMBDA" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "❌ SOURCE_VPC_LAMBDA not found: $SOURCE_VPC_LAMBDA"
  exit 1
fi

VPC_JSON="$(aws lambda get-function-configuration --function-name "$SOURCE_VPC_LAMBDA" --region "$AWS_REGION" --query 'VpcConfig' --output json)"
SUBNET_IDS="$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(' '.join(d.get('SubnetIds', [])))" "$VPC_JSON")"
SECURITY_GROUP_IDS="$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(' '.join(d.get('SecurityGroupIds', [])))" "$VPC_JSON")"
SUBNET_IDS_CSV="$(echo "$SUBNET_IDS" | tr ' ' ',')"
SECURITY_GROUP_IDS_CSV="$(echo "$SECURITY_GROUP_IDS" | tr ' ' ',')"

if [[ -z "$SUBNET_IDS" || -z "$SECURITY_GROUP_IDS" ]]; then
  echo "❌ Could not derive VPC config from $SOURCE_VPC_LAMBDA"
  exit 1
fi

rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR/serving_api"

pip_for_lambda_x86_64 "$SERVING_DIR/requirements.txt" "$PACKAGE_DIR"
cp -R "$SERVING_DIR/"* "$PACKAGE_DIR/serving_api/"

find "$PACKAGE_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find "$PACKAGE_DIR" -name "*.pyc" -delete

rm -f "$ZIP_PATH"
cd "$PACKAGE_DIR"
zip -r9 "$ZIP_PATH" . -x "*.pyc" "*/__pycache__/*"
cd "$SCRIPT_DIR"

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  CURRENT_ENV_JSON="$(aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --query 'Environment.Variables' \
    --output json 2>/dev/null || echo '{}')"
  current_var() {
    python3 -c "import json,sys; data=json.loads(sys.argv[1] if sys.argv[1] else '{}'); print(data.get(sys.argv[2], ''))" "$CURRENT_ENV_JSON" "$1"
  }
  [[ -z "$RDS_SECRET_ARN" ]] && RDS_SECRET_ARN="$(current_var RDS_SECRET_ARN)"
  [[ -z "$SERVING_API_KEY" ]] && SERVING_API_KEY="$(current_var SERVING_API_KEY)"
  [[ -z "$SERVING_API_KEY_SECRET_ARN" ]] && SERVING_API_KEY_SECRET_ARN="$(current_var SERVING_API_KEY_SECRET_ARN)"
  [[ -z "$POLYGON_API_KEY_SECRET_ARN" ]] && POLYGON_API_KEY_SECRET_ARN="$(current_var POLYGON_API_KEY_SECRET_ARN)"
  [[ -z "${ALLOWED_ORIGIN:-}" || "$ALLOWED_ORIGIN" == "*" ]] && ALLOWED_ORIGIN="$(current_var ALLOWED_ORIGIN)"
  [[ -z "$SCREENER_CACHE_TTL_S" ]] && SCREENER_CACHE_TTL_S="$(current_var SCREENER_CACHE_TTL_S)"
  [[ -z "$RETURNS_CACHE_TTL_S" ]] && RETURNS_CACHE_TTL_S="$(current_var RETURNS_CACHE_TTL_S)"
  [[ -z "$BACKTEST_FUNCTION_NAME" ]] && BACKTEST_FUNCTION_NAME="$(current_var BACKTEST_FUNCTION_NAME)"

  echo "⬆️  Updating existing function code"
  update_lambda_code_auto "$FUNCTION_NAME" "$ZIP_PATH"
  wait_for_lambda_update_ready
else
  if [[ -z "$LAMBDA_ROLE_ARN" ]]; then
    echo "❌ LAMBDA_ROLE_ARN is required to create a new Lambda function"
    exit 1
  fi
  echo "➕ Creating new function"
  create_lambda_auto "$FUNCTION_NAME" "$LAMBDA_ROLE_ARN" "$ZIP_PATH"
  wait_for_lambda_update_ready 60 5
fi

echo "⚙️  Applying runtime/network/environment settings"
aws lambda update-function-configuration \
  --function-name "$FUNCTION_NAME" \
  --runtime "$LAMBDA_RUNTIME" \
  --handler "serving_api.handler.lambda_handler" \
  --timeout "$LAMBDA_TIMEOUT" \
  --memory-size "$LAMBDA_MEMORY" \
  --vpc-config "SubnetIds=${SUBNET_IDS_CSV},SecurityGroupIds=${SECURITY_GROUP_IDS_CSV}" \
  --environment "Variables={RDS_SECRET_ARN=${RDS_SECRET_ARN},SERVING_API_KEY=${SERVING_API_KEY},SERVING_API_KEY_SECRET_ARN=${SERVING_API_KEY_SECRET_ARN},POLYGON_API_KEY_SECRET_ARN=${POLYGON_API_KEY_SECRET_ARN},ALLOWED_ORIGIN=${ALLOWED_ORIGIN},SCREENER_CACHE_TTL_S=${SCREENER_CACHE_TTL_S},RETURNS_CACHE_TTL_S=${RETURNS_CACHE_TTL_S},BACKTEST_FUNCTION_NAME=${BACKTEST_FUNCTION_NAME}}" \
  --region "$AWS_REGION" >/dev/null
wait_for_lambda_update_ready

aws lambda put-function-concurrency \
  --function-name "$FUNCTION_NAME" \
  --reserved-concurrent-executions "$LAMBDA_RESERVED_CONCURRENCY" \
  --region "$AWS_REGION" >/dev/null

echo "✅ Lambda deployed: $FUNCTION_NAME"
