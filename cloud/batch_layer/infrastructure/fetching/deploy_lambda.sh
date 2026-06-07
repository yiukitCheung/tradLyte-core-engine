#!/bin/bash
# Build and deploy Lambda functions to AWS

set -e
export AWS_PAGER=""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Point to batch_layer/fetching (application code) not infrastructure/fetching
INFRA_FETCHING_DIR="$(dirname "$SCRIPT_DIR")"
BATCH_LAYER_DIR="$(dirname "$INFRA_FETCHING_DIR")"
AWS_ARCH_DIR="$(dirname "$BATCH_LAYER_DIR")"
FETCHING_DIR="$BATCH_LAYER_DIR/fetching"
SHARED_DIR="$AWS_ARCH_DIR/shared"  # Canonical shared package
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/../common/pip_for_lambda.sh"

# AWS Configuration
AWS_REGION="${AWS_REGION:-ca-west-1}"
FUNCTION_PREFIX="${FUNCTION_PREFIX:-dev-batch-}"  # Customize this prefix
LAMBDA_RUNTIME="${LAMBDA_RUNTIME:-python3.11}"

echo "🚀 Building and deploying Lambda functions..."
echo "Region: $AWS_REGION"

cleanup_artifacts() {
    # Always clean transient build artifacts, even on failure/interrupt.
    rm -rf "$SCRIPT_DIR/package" 2>/dev/null || true
    rm -f "$SCRIPT_DIR"/*.zip 2>/dev/null || true
}
trap cleanup_artifacts EXIT INT TERM

# Zip uses <file_name>.py (e.g. daily_ohlcv_planner.py); AWS default handler lambda_function.* breaks cold start.
set_lambda_python_handler() {
    local deployed_name=$1
    local module_base=$2
    local handler="${module_base}.lambda_handler"
    if ! aws lambda get-function --function-name "$deployed_name" --region "$AWS_REGION" &>/dev/null; then
        return 0
    fi
    echo "⚙️  Setting handler on $deployed_name → $handler"
    aws lambda wait function-updated-v2 --function-name "$deployed_name" --region "$AWS_REGION" 2>/dev/null || sleep 5
    aws lambda update-function-configuration \
        --function-name "$deployed_name" \
        --handler "$handler" \
        --region "$AWS_REGION" \
        --output json >/dev/null
}

ensure_lambda_runtime() {
    local deployed_name=$1
    if ! aws lambda get-function --function-name "$deployed_name" --region "$AWS_REGION" &>/dev/null; then
        return 0
    fi
    aws lambda wait function-updated-v2 --function-name "$deployed_name" --region "$AWS_REGION" 2>/dev/null || sleep 5
    local current_runtime
    current_runtime=$(aws lambda get-function-configuration \
        --function-name "$deployed_name" \
        --region "$AWS_REGION" \
        --query 'Runtime' \
        --output text)
    if [ "$current_runtime" != "$LAMBDA_RUNTIME" ]; then
        echo "⚙️  Updating runtime on $deployed_name: $current_runtime -> $LAMBDA_RUNTIME"
        aws lambda update-function-configuration \
            --function-name "$deployed_name" \
            --runtime "$LAMBDA_RUNTIME" \
            --region "$AWS_REGION" \
            --output json >/dev/null
        aws lambda wait function-updated-v2 --function-name "$deployed_name" --region "$AWS_REGION" 2>/dev/null || sleep 5
    fi
}

get_timeout_for_function() {
    local fn=$1
    case "$fn" in
        daily-ohlcv-planner) echo "${TIMEOUT_DAILY_OHLCV_PLANNER:-900}" ;;
        daily-ohlcv-fetcher) echo "${TIMEOUT_DAILY_OHLCV_FETCHER:-900}" ;;
        daily-meta-fetcher) echo "${TIMEOUT_DAILY_META_FETCHER:-900}" ;;
        *) echo "${TIMEOUT_DEFAULT:-60}" ;;
    esac
}

get_memory_for_function() {
    local fn=$1
    case "$fn" in
        daily-ohlcv-planner) echo "${MEMORY_DAILY_OHLCV_PLANNER:-512}" ;;
        daily-ohlcv-fetcher) echo "${MEMORY_DAILY_OHLCV_FETCHER:-2048}" ;;
        daily-meta-fetcher) echo "${MEMORY_DAILY_META_FETCHER:-2048}" ;;
        *) echo "${MEMORY_DEFAULT:-512}" ;;
    esac
}

ensure_lambda_perf_config() {
    local deployed_name=$1
    local function_name=$2
    if ! aws lambda get-function --function-name "$deployed_name" --region "$AWS_REGION" &>/dev/null; then
        return 0
    fi
    aws lambda wait function-updated-v2 --function-name "$deployed_name" --region "$AWS_REGION" 2>/dev/null || sleep 5
    local desired_timeout desired_memory current_timeout current_memory
    desired_timeout=$(get_timeout_for_function "$function_name")
    desired_memory=$(get_memory_for_function "$function_name")
    current_timeout=$(aws lambda get-function-configuration \
        --function-name "$deployed_name" \
        --region "$AWS_REGION" \
        --query 'Timeout' \
        --output text)
    current_memory=$(aws lambda get-function-configuration \
        --function-name "$deployed_name" \
        --region "$AWS_REGION" \
        --query 'MemorySize' \
        --output text)
    if [ "$current_timeout" != "$desired_timeout" ] || [ "$current_memory" != "$desired_memory" ]; then
        echo "⚙️  Updating perf on $deployed_name: timeout ${current_timeout}s -> ${desired_timeout}s, memory ${current_memory}MB -> ${desired_memory}MB"
        aws lambda update-function-configuration \
            --function-name "$deployed_name" \
            --timeout "$desired_timeout" \
            --memory-size "$desired_memory" \
            --region "$AWS_REGION" \
            --output json >/dev/null
        aws lambda wait function-updated-v2 --function-name "$deployed_name" --region "$AWS_REGION" 2>/dev/null || sleep 5
    fi
}

# Function to build and deploy a Lambda package
# Args: function_name [requirements_file] [shared_mode]
#   shared_mode: ohlcv (Polygon + models only) | meta (RDS + models + utils; no Polygon deps)
build_and_deploy_lambda() {
    local function_name=$1
    local req_file="${2:-$FETCHING_DIR/requirements.txt}"
    local shared_mode="${3:-meta}"
    local file_name=$(echo "$function_name" | tr '-' '_')  # Convert dashes to underscores for file name
    local package_dir="$SCRIPT_DIR/package/$function_name"
    
    echo ""
    echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
    echo "📦 Building $function_name..."
    echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
    
    # Create package directory
    mkdir -p "$package_dir"
    
    # Linux manylinux wheels only — never fallback to host pip (breaks psycopg2/pyarrow on Lambda).
    pip_for_lambda_x86_64 "$req_file" "$package_dir"

    # Copy Lambda function
    echo "📄 Copying Lambda function code..."
    cp "$FETCHING_DIR/lambda_functions/${file_name}.py" "$package_dir/${file_name}.py"
    
    # Copy shared modules (only what Lambda needs)
    echo "📁 Copying shared modules..."
    mkdir -p "$package_dir/shared/clients"
    mkdir -p "$package_dir/shared/models"
    mkdir -p "$package_dir/shared/utils"
    
    if [ "$shared_mode" = "ohlcv" ]; then
        cp "$SHARED_DIR/clients/polygon_client.py" "$package_dir/shared/clients/"
        cat > "$package_dir/shared/clients/__init__.py" << 'EOF'
"""Client modules for Lambda functions"""
from .polygon_client import PolygonClient

__all__ = ['PolygonClient']
EOF
    else
        cp "$SHARED_DIR/clients/rds_timescale_client.py" "$package_dir/shared/clients/"
        cat > "$package_dir/shared/clients/__init__.py" << 'EOF'
"""Client modules for Lambda functions"""
from .rds_timescale_client import RDSTimescaleClient

__all__ = ['RDSTimescaleClient']
EOF
    fi
    
    # Copy models and utils
    cp -r "$SHARED_DIR/models/"* "$package_dir/shared/models/" 2>/dev/null || true
    if [ -n "$(ls -A "$SHARED_DIR/utils/" 2>/dev/null)" ]; then
        cp -r "$SHARED_DIR/utils/"* "$package_dir/shared/utils/"
    fi
    cp "$SHARED_DIR/__init__.py" "$package_dir/shared/"
    
    # Remove cache files
    echo "🧹 Cleaning cache files..."
    find "$package_dir" -name "*.pyc" -delete
    find "$package_dir" -name "*.pyo" -delete
    find "$package_dir" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    
    # Create ZIP file
    echo "📦 Creating deployment package..."
    cd "$package_dir"
    zip -r9 "$SCRIPT_DIR/$function_name.zip" . -x "*.pyc" "*/__pycache__/*"
    cd "$SCRIPT_DIR"
    
    # Check package size
    local size=$(du -h "$SCRIPT_DIR/$function_name.zip" | cut -f1)
    local size_bytes=$(stat -f%z "$SCRIPT_DIR/$function_name.zip" 2>/dev/null || stat -c%s "$SCRIPT_DIR/$function_name.zip")
    echo "✅ Created $function_name.zip ($size)"
    
    # Deploy to AWS
    echo "🚀 Deploying to AWS Lambda..."
    
    # Try with prefix first, then without
    local aws_function_name="${FUNCTION_PREFIX}${function_name}"
    
    # For packages > 50MB, upload to S3 first
    if [ "$size_bytes" -gt 52428800 ]; then
        echo "📦 Package is large ($size), uploading to S3 first..."
        local s3_bucket="${LAMBDA_DEPLOY_BUCKET:-dev-condvest-lambda-deploy}"
        local s3_key="lambda-packages/$function_name-$(date +%s).zip"
        
        # Create S3 bucket if it doesn't exist
        if ! aws s3 ls "s3://$s3_bucket" --region "$AWS_REGION" 2>/dev/null; then
            echo "📦 Creating S3 bucket: $s3_bucket"
            aws s3 mb "s3://$s3_bucket" --region "$AWS_REGION" 2>/dev/null || true
        fi
        
        # Upload to S3
        echo "⬆️  Uploading to S3: s3://$s3_bucket/$s3_key"
        aws s3 cp "$SCRIPT_DIR/$function_name.zip" "s3://$s3_bucket/$s3_key" --region "$AWS_REGION"
        
        # Update Lambda from S3
        if aws lambda get-function --function-name "$aws_function_name" --region "$AWS_REGION" &>/dev/null; then
            echo "📝 Updating function from S3: $aws_function_name"
            result=$(aws lambda update-function-code \
                --function-name "$aws_function_name" \
                --s3-bucket "$s3_bucket" \
                --s3-key "$s3_key" \
                --region "$AWS_REGION" \
                --output json)
            
            echo "✅ Updated successfully from S3!"
            echo "   Last Modified: $(echo $result | jq -r '.LastModified')"
            echo "   Code Size: $(echo $result | jq -r '.CodeSize') bytes"
            ensure_lambda_runtime "$aws_function_name"
            ensure_lambda_perf_config "$aws_function_name" "$function_name"
            set_lambda_python_handler "$aws_function_name" "$file_name"
        elif aws lambda get-function --function-name "$function_name" --region "$AWS_REGION" &>/dev/null; then
            echo "📝 Updating function from S3: $function_name"
            result=$(aws lambda update-function-code \
                --function-name "$function_name" \
                --s3-bucket "$s3_bucket" \
                --s3-key "$s3_key" \
                --region "$AWS_REGION" \
                --output json)
            
            echo "✅ Updated successfully from S3!"
            echo "   Last Modified: $(echo $result | jq -r '.LastModified')"
            echo "   Code Size: $(echo $result | jq -r '.CodeSize') bytes"
            ensure_lambda_runtime "$function_name"
            ensure_lambda_perf_config "$function_name" "$function_name"
            set_lambda_python_handler "$function_name" "$file_name"
        else
            echo "❌ Function not found in AWS (tried: $aws_function_name and $function_name)"
            echo "💡 Create it first via AWS Console, then run this script again."
        fi
    else
        # Small package, direct upload
        if aws lambda get-function --function-name "$aws_function_name" --region "$AWS_REGION" &>/dev/null; then
            echo "📝 Updating function: $aws_function_name"
            result=$(aws lambda update-function-code \
                --function-name "$aws_function_name" \
                --zip-file "fileb://$SCRIPT_DIR/$function_name.zip" \
                --region "$AWS_REGION" \
                --output json)
            
            echo "✅ Updated successfully!"
            echo "   Last Modified: $(echo $result | jq -r '.LastModified')"
            echo "   Code Size: $(echo $result | jq -r '.CodeSize') bytes"
            ensure_lambda_runtime "$aws_function_name"
            ensure_lambda_perf_config "$aws_function_name" "$function_name"
            set_lambda_python_handler "$aws_function_name" "$file_name"
        elif aws lambda get-function --function-name "$function_name" --region "$AWS_REGION" &>/dev/null; then
            echo "📝 Updating function: $function_name"
            result=$(aws lambda update-function-code \
                --function-name "$function_name" \
                --zip-file "fileb://$SCRIPT_DIR/$function_name.zip" \
                --region "$AWS_REGION" \
                --output json)
            
            echo "✅ Updated successfully!"
            echo "   Last Modified: $(echo $result | jq -r '.LastModified')"
            echo "   Code Size: $(echo $result | jq -r '.CodeSize') bytes"
            ensure_lambda_runtime "$function_name"
            ensure_lambda_perf_config "$function_name" "$function_name"
            set_lambda_python_handler "$function_name" "$file_name"
        else
            echo "❌ Function not found in AWS (tried: $aws_function_name and $function_name)"
            echo "💡 Create it first via AWS Console, then run this script again."
        fi
    fi
}

# Clean up previous builds
echo "🧹 Cleaning previous builds..."
rm -rf "$SCRIPT_DIR"/package/
mkdir -p "$SCRIPT_DIR"/package

# Build and deploy Lambda functions (fetchers + OHLCV planner)
# Note: Consolidation moved to AWS Batch (see processing/batch_jobs/)
# OHLCV fetcher: S3 bronze only (Polygon); planner: VPC/RDS reads missing dates → async-invokes fetcher
# Meta fetcher: Polygon → S3 manifest (no RDS on fetcher)
build_and_deploy_lambda "daily-ohlcv-fetcher" "$FETCHING_DIR/requirements.ohlcv.txt" "ohlcv"
build_and_deploy_lambda "daily-ohlcv-planner" "$BATCH_LAYER_DIR/ingesting/requirements.txt" "meta"
build_and_deploy_lambda "daily-meta-fetcher" "$FETCHING_DIR/requirements.ohlcv.txt" "ohlcv"

echo ""
echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
echo "🎉 Deployment complete!"
echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
echo ""
echo "📊 Deployed packages (now stored in S3):"
for zip_file in "$SCRIPT_DIR"/*.zip; do
    if [ -f "$zip_file" ]; then
        size=$(du -h "$zip_file" | cut -f1)
        echo "  $(basename "$zip_file"): $size → s3://${LAMBDA_DEPLOY_BUCKET:-dev-condvest-lambda-deploy}/lambda-packages/"
    fi
done

echo ""
echo "🧹 Cleanup handled automatically on script exit."

echo ""
echo "💡 Tips:"
echo "  - View logs: aws logs tail /aws/lambda/daily_ohlcv_fetcher --follow"
echo "  - Test function: aws lambda invoke --function-name daily_ohlcv_fetcher response.json"
echo ""
echo "📦 Lambda packages stored in S3: s3://${LAMBDA_DEPLOY_BUCKET:-dev-condvest-lambda-deploy}/lambda-packages/"
echo ""
echo "📝 OHLCV path: planner (VPC) → fetcher → S3 bronze; ingest handler (VPC) ← S3 trigger → RDS."
echo "📝 VPC planner: RDS_SECRET_ARN + OHLCV_FETCHER_FUNCTION_NAME; private subnets need NAT or Secrets Manager VPC endpoint (see infrastructure/common/VPC_LAMBDA_SECRETS_MANAGER.txt)."
echo "   Consolidator/resampler Batch jobs: _archive/batch_layer_scripts/README_ARCHIVED_BATCH_JOBS.md"