#!/bin/bash
# ARCHIVED: Build Docker container for batch jobs (resampler, consolidator)
# Pipeline now uses raw 1d OHLCV only; resampling is done on-the-fly in backtester.
# Original location: infrastructure/processing/build_batch_container.sh
# Note: If run from archive_scripts, set SCRIPT_DIR to infrastructure/processing for Dockerfile path.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# When run from archive_scripts, point to original infra dir for Dockerfile
INFRA_PROCESSING="${SCRIPT_DIR}/../infrastructure/processing"
if [ ! -f "$INFRA_PROCESSING/Dockerfile" ]; then
    INFRA_PROCESSING="$(cd "$SCRIPT_DIR/../../infrastructure/processing" 2>/dev/null && pwd)" || true
fi
BATCH_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
AWS_ARCH_DIR="$(dirname "$BATCH_DIR")"
PROCESSING_DIR="$BATCH_DIR/processing"
SHARED_DIR="$AWS_ARCH_DIR/shared"

AWS_REGION=${AWS_REGION:-ca-west-1}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null)}
ECR_REPOSITORY=${ECR_REPOSITORY:-dev-batch-processor}
IMAGE_TAG=${IMAGE_TAG:-latest}
LOCAL_ONLY=false

usage() {
    echo "ARCHIVED: Builds container for resampler/consolidator (no longer in pipeline)."
    echo "Usage: $0 [--tag TAG] [--repo REPO] [--region REGION] [--local-only]"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --tag) IMAGE_TAG="$2"; shift 2 ;;
        --repo) ECR_REPOSITORY="$2"; shift 2 ;;
        --region) AWS_REGION="$2"; shift 2 ;;
        --local-only) LOCAL_ONLY=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

echo "============================================================"
echo "🔧 Building Docker container (ARCHIVED - resampler/consolidator)"
echo "============================================================"
DOCKERFILE_DIR="${INFRA_PROCESSING:-$SCRIPT_DIR/../infrastructure/processing}"
if [ ! -f "$DOCKERFILE_DIR/Dockerfile" ]; then
    echo "❌ Dockerfile not found at $DOCKERFILE_DIR - run from batch_layer or set paths"
    exit 1
fi

echo "📁 Copying shared modules..."
rm -rf "$PROCESSING_DIR/shared"
cp -r "$SHARED_DIR" "$PROCESSING_DIR/"

echo "🐳 Building Docker image..."
cd "$BATCH_DIR"
docker build \
    --platform linux/amd64 \
    -f "$DOCKERFILE_DIR/Dockerfile" \
    -t "$ECR_REPOSITORY:$IMAGE_TAG" \
    --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --build-arg VCS_REF="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
    .

if [ "$LOCAL_ONLY" = false ] && [ -n "$AWS_ACCOUNT_ID" ]; then
    echo "🏷️ Tagging and pushing to ECR..."
    docker tag "$ECR_REPOSITORY:$IMAGE_TAG" "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY:$IMAGE_TAG"
    aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
    docker push "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY:$IMAGE_TAG"
    echo "✅ Pushed to ECR"
else
    echo "✅ Built locally (--local-only or no AWS account)"
fi

rm -rf "$PROCESSING_DIR/shared"
echo "📦 Jobs in image: resampler, consolidator (archived)."
