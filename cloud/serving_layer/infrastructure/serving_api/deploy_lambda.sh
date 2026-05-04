#!/usr/bin/env bash
# Build and deploy the FastAPI serving Lambda (zip package).
#
# Usage:
#   AWS_REGION=ca-west-1 FUNCTION_NAME=dev-serving-api \
#   SOURCE_VPC_LAMBDA=dev-batch-daily-ohlcv-ingest-handler \
#   RDS_SECRET_ARN=arn:aws:secretsmanager:...:secret:... \
#   SERVING_API_KEY=change-me \
#   ./cloud/serving_layer/infrastructure/serving_api/deploy_lambda.sh

set -euo pipefail
export AWS_PAGER=""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
SERVING_DIR="$REPO_ROOT/cloud/serving_layer/lambda_functions/serving_api"
SHARED_DIR="$REPO_ROOT/cloud/shared"
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
ALLOWED_ORIGIN="${ALLOWED_ORIGIN:-*}"
SCREENER_CACHE_TTL_S="${SCREENER_CACHE_TTL_S:-60}"
RETURNS_CACHE_TTL_S="${RETURNS_CACHE_TTL_S:-300}"

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
mkdir -p "$PACKAGE_DIR/shared"

pip_for_lambda_x86_64 "$SERVING_DIR/requirements.txt" "$PACKAGE_DIR"
cp -R "$SERVING_DIR/"* "$PACKAGE_DIR/serving_api/"
cp -R "$SHARED_DIR/"* "$PACKAGE_DIR/shared/"

find "$PACKAGE_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find "$PACKAGE_DIR" -name "*.pyc" -delete

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
  [[ -z "${ALLOWED_ORIGIN:-}" || "$ALLOWED_ORIGIN" == "*" ]] && ALLOWED_ORIGIN="$(current_var ALLOWED_ORIGIN)"
  [[ -z "$SCREENER_CACHE_TTL_S" ]] && SCREENER_CACHE_TTL_S="$(current_var SCREENER_CACHE_TTL_S)"
  [[ -z "$RETURNS_CACHE_TTL_S" ]] && RETURNS_CACHE_TTL_S="$(current_var RETURNS_CACHE_TTL_S)"

  echo "⬆️  Updating existing function code"
  aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH" \
    --region "$AWS_REGION" >/dev/null
  wait_for_lambda_update_ready
else
  if [[ -z "$LAMBDA_ROLE_ARN" ]]; then
    echo "❌ LAMBDA_ROLE_ARN is required to create a new Lambda function"
    exit 1
  fi
  echo "➕ Creating new function"
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "$LAMBDA_RUNTIME" \
    --role "$LAMBDA_ROLE_ARN" \
    --handler "serving_api.handler.lambda_handler" \
    --architectures "$LAMBDA_ARCH" \
    --zip-file "fileb://$ZIP_PATH" \
    --timeout "$LAMBDA_TIMEOUT" \
    --memory-size "$LAMBDA_MEMORY" \
    --vpc-config "SubnetIds=${SUBNET_IDS_CSV},SecurityGroupIds=${SECURITY_GROUP_IDS_CSV}" \
    --region "$AWS_REGION" >/dev/null
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
  --environment "Variables={RDS_SECRET_ARN=${RDS_SECRET_ARN},SERVING_API_KEY=${SERVING_API_KEY},ALLOWED_ORIGIN=${ALLOWED_ORIGIN},SCREENER_CACHE_TTL_S=${SCREENER_CACHE_TTL_S},RETURNS_CACHE_TTL_S=${RETURNS_CACHE_TTL_S}}" \
  --region "$AWS_REGION" >/dev/null
wait_for_lambda_update_ready

aws lambda put-function-concurrency \
  --function-name "$FUNCTION_NAME" \
  --reserved-concurrent-executions "$LAMBDA_RESERVED_CONCURRENCY" \
  --region "$AWS_REGION" >/dev/null

echo "✅ Lambda deployed: $FUNCTION_NAME"
