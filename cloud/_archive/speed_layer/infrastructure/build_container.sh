#!/bin/bash
# Build and push Docker container for Speed Layer WebSocket Service
# Location: speed_layer/infrastructure/build_container.sh
#
# IMPORTANT: This script must be run from the aws_lambda_architecture/ root directory
# Usage: cd aws_lambda_architecture && ./speed_layer/infrastructure/build_container.sh

set -e

# Validate we're in the correct directory (aws_lambda_architecture root)
if [ ! -d "speed_layer" ] || [ ! -d "shared" ] || [ ! -f "speed_layer/fetching/data_stream_fetcher.py" ]; then
    echo "❌ Error: This script must be run from the aws_lambda_architecture/ root directory"
    echo "   Current directory: $(pwd)"
    echo "   Expected structure:"
    echo "     - speed_layer/fetching/data_stream_fetcher.py"
    echo "     - shared/"
    echo ""
    echo "   Please run: cd aws_lambda_architecture && ./speed_layer/infrastructure/build_container.sh"
    exit 1
fi

# Paths relative to aws_lambda_architecture root
DOCKERFILE_PATH="speed_layer/fetching/Dockerfile"
BUILD_CONTEXT="."

# Default values
AWS_REGION=${AWS_REGION:-ca-west-1}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")}
ECR_REPOSITORY=${ECR_REPOSITORY:-dev-speed-layer-websocket}
IMAGE_TAG=${IMAGE_TAG:-latest}

# Usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "IMPORTANT: Run this script from aws_lambda_architecture/ root directory"
    echo "  cd aws_lambda_architecture && ./speed_layer/infrastructure/build_container.sh"
    echo ""
    echo "Options:"
    echo "  --tag TAG        Image tag (default: latest)"
    echo "  --repo REPO      ECR repository name (default: dev-speed-layer-websocket)"
    echo "  --region REGION  AWS region (default: ca-west-1)"
    echo "  --local-only     Build locally without pushing to ECR"
    echo "  -h, --help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Build and push (default)"
    echo "  cd aws_lambda_architecture && ./speed_layer/infrastructure/build_container.sh"
    echo ""
    echo "  # Build with custom tag"
    echo "  cd aws_lambda_architecture && ./speed_layer/infrastructure/build_container.sh --tag v1.0.0"
    echo ""
    echo "  # Build locally only (no push)"
    echo "  cd aws_lambda_architecture && ./speed_layer/infrastructure/build_container.sh --local-only"
}

# Parse arguments
LOCAL_ONLY=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --repo)
            ECR_REPOSITORY="$2"
            shift 2
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --local-only)
            LOCAL_ONLY=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate AWS account ID
if [ -z "$AWS_ACCOUNT_ID" ] && [ "$LOCAL_ONLY" = false ]; then
    echo "❌ Error: Could not determine AWS Account ID"
    echo "   Please set AWS_ACCOUNT_ID environment variable or configure AWS CLI"
    exit 1
fi

echo "============================================================"
echo "🔧 Building Docker container for Speed Layer WebSocket Service"
echo "============================================================"
echo "📍 Region: $AWS_REGION"
echo "🏗️ Repository: $ECR_REPOSITORY"
echo "🏷️ Tag: $IMAGE_TAG"
if [ "$LOCAL_ONLY" = false ]; then
    echo "📦 Account: $AWS_ACCOUNT_ID"
fi
echo ""

# Get ECR login token (if not local-only)
if [ "$LOCAL_ONLY" = false ]; then
    echo "🔐 Authenticating with ECR..."
    ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
    aws ecr get-login-password --region "$AWS_REGION" | \
        docker login --username AWS --password-stdin "$ECR_URI"
    
    # Check if repository exists, create if not
    echo "🔍 Checking ECR repository..."
    if ! aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --region "$AWS_REGION" &>/dev/null; then
        echo "📦 Creating ECR repository: $ECR_REPOSITORY"
        aws ecr create-repository \
            --repository-name "$ECR_REPOSITORY" \
            --region "$AWS_REGION" \
            --image-scanning-configuration scanOnPush=true \
            --image-tag-mutability MUTABLE
    else
        echo "✅ Repository exists: $ECR_REPOSITORY"
    fi
fi

# Build Docker image
echo "🐳 Building Docker image..."
echo "   Build context: $(pwd)"
echo "   Dockerfile: $DOCKERFILE_PATH"
echo ""

docker build \
    --platform linux/amd64 \
    -f "$DOCKERFILE_PATH" \
    -t "$ECR_REPOSITORY:$IMAGE_TAG" \
    --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --build-arg VCS_REF="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
    "$BUILD_CONTEXT"

if [ "$LOCAL_ONLY" = false ]; then
    # Tag for ECR
    FULL_IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"
    echo "🏷️ Tagging image for ECR..."
    docker tag "$ECR_REPOSITORY:$IMAGE_TAG" "$FULL_IMAGE_URI"

    # Push to ECR
    echo "⬆️ Pushing to ECR..."
    docker push "$FULL_IMAGE_URI"
    
    echo ""
    echo "✅ Docker container built and pushed successfully!"
    echo "📍 Image URI: $FULL_IMAGE_URI"
else
    echo ""
    echo "✅ Docker container built locally (not pushed to ECR)"
fi

# Show image details
echo ""
echo "📊 Image details:"
docker images "$ECR_REPOSITORY:$IMAGE_TAG" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"

echo ""
echo "============================================================"
echo "📝 Next Steps"
echo "============================================================"
echo ""
echo "1. Create ECS Task Definition (see DEPLOYMENT_GUIDE.md)"
echo "2. Create ECS Service"
echo "3. Monitor logs: aws logs tail /ecs/speed-layer-websocket --follow"
echo ""
