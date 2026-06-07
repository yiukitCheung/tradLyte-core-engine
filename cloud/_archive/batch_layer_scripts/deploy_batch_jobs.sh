#!/bin/bash
# ARCHIVED: Deploy AWS Batch Job Definitions (consolidator, resampler)
# Pipeline now uses raw 1d OHLCV only; resampling is done on-the-fly in backtester.
# Original location: infrastructure/processing/deploy_batch_jobs.sh

set -e

# Configuration
AWS_REGION="${AWS_REGION:-ca-west-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
ECR_REPOSITORY="${ECR_REPOSITORY:-dev-batch-processor}"
JOB_QUEUE="${JOB_QUEUE:-dev-batch-duckdb-resampler}"
LOG_GROUP="${LOG_GROUP:-/aws/batch/dev-batch-duckdb-resampler}"

CONSOLIDATOR_JOB_NAME="${ENVIRONMENT}-batch-bronze-consolidator"
RESAMPLER_JOB_NAME="${ENVIRONMENT}-batch-duckdb-resampler"

usage() {
    echo "ARCHIVED: This script deployed consolidator/resampler Batch jobs."
    echo "Usage: $0 [OPTIONS]"
    echo "  --job TYPE       consolidator, resampler, or all"
    echo "  --region REGION  AWS region"
}

JOB_TYPE="all"
while [[ $# -gt 0 ]]; do
    case $1 in
        --job) JOB_TYPE="$2"; shift 2 ;;
        --region) AWS_REGION="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

get_job_roles() {
    EXISTING_JOB=$(aws batch describe-job-definitions \
    --job-definition-name "$RESAMPLER_JOB_NAME" \
    --status ACTIVE \
    --region "$AWS_REGION" \
    --query 'jobDefinitions[0]' \
    --output json 2>/dev/null || echo "{}")
    if [ "$EXISTING_JOB" == "{}" ] || [ "$EXISTING_JOB" == "null" ]; then
        JOB_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ENVIRONMENT}-condvest-batch-processing-execution-role"
        EXECUTION_ROLE_ARN="$JOB_ROLE_ARN"
    else
        JOB_ROLE_ARN=$(echo "$EXISTING_JOB" | jq -r '.containerProperties.jobRoleArn // empty')
        EXECUTION_ROLE_ARN=$(echo "$EXISTING_JOB" | jq -r '.containerProperties.executionRoleArn // empty')
        JOB_ROLE_ARN="${JOB_ROLE_ARN:-arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ENVIRONMENT}-condvest-batch-processing-execution-role}"
        EXECUTION_ROLE_ARN="${EXECUTION_ROLE_ARN:-$JOB_ROLE_ARN}"
    fi
}

deploy_consolidator() {
    echo "📦 Deploying Consolidator (archived)..."
    JOB_DEF_JSON=$(cat <<EOF
{
    "jobDefinitionName": "${CONSOLIDATOR_JOB_NAME}",
    "type": "container",
    "containerProperties": {
        "image": "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:latest",
        "command": ["python", "consolidator.py"],
        "jobRoleArn": "${JOB_ROLE_ARN}",
        "executionRoleArn": "${EXECUTION_ROLE_ARN}",
        "resourceRequirements": [{"type": "VCPU", "value": "2"}, {"type": "MEMORY", "value": "4096"}],
        "environment": [
            {"name": "AWS_REGION", "value": "${AWS_REGION}"},
            {"name": "S3_BUCKET", "value": "dev-condvest-datalake"},
            {"name": "S3_PREFIX", "value": "bronze/raw_ohlcv"},
            {"name": "MODE", "value": "incremental"},
            {"name": "MAX_WORKERS", "value": "10"},
            {"name": "RETENTION_DAYS", "value": "30"},
            {"name": "SKIP_CLEANUP", "value": "false"}
        ],
        "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "${LOG_GROUP}", "awslogs-region": "${AWS_REGION}", "awslogs-stream-prefix": "consolidator"}},
        "networkConfiguration": {"assignPublicIp": "ENABLED"},
        "fargatePlatformConfiguration": {"platformVersion": "LATEST"}
    },
    "retryStrategy": {"attempts": 2},
    "timeout": {"attemptDurationSeconds": 1800},
    "platformCapabilities": ["FARGATE"],
    "tags": {"Environment": "${ENVIRONMENT}", "Component": "consolidator"}
}
EOF
)
    aws batch register-job-definition --cli-input-json "$JOB_DEF_JSON" --region "$AWS_REGION" --output json >/dev/null
    echo "   ✅ Consolidator: $CONSOLIDATOR_JOB_NAME"
}

deploy_resampler() {
    echo "📦 Deploying Resampler (archived)..."
    JOB_DEF_JSON=$(cat <<EOF
{
    "jobDefinitionName": "${RESAMPLER_JOB_NAME}",
    "type": "container",
    "containerProperties": {
        "image": "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:latest",
        "command": ["python", "resampler.py"],
        "jobRoleArn": "${JOB_ROLE_ARN}",
        "executionRoleArn": "${EXECUTION_ROLE_ARN}",
        "resourceRequirements": [{"type": "VCPU", "value": "2"}, {"type": "MEMORY", "value": "4096"}],
        "environment": [
            {"name": "AWS_REGION", "value": "${AWS_REGION}"},
            {"name": "S3_BUCKET_NAME", "value": "dev-condvest-datalake"},
            {"name": "RESAMPLING_INTERVALS", "value": "3,5,8,13,21,34"}
        ],
        "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "${LOG_GROUP}", "awslogs-region": "${AWS_REGION}", "awslogs-stream-prefix": "resampler"}},
        "networkConfiguration": {"assignPublicIp": "ENABLED"},
        "fargatePlatformConfiguration": {"platformVersion": "LATEST"}
    },
    "retryStrategy": {"attempts": 3},
    "timeout": {"attemptDurationSeconds": 3600},
    "platformCapabilities": ["FARGATE"],
    "tags": {"Environment": "${ENVIRONMENT}", "Component": "resampler"}
}
EOF
)
    aws batch register-job-definition --cli-input-json "$JOB_DEF_JSON" --region "$AWS_REGION" --output json >/dev/null
    echo "   ✅ Resampler: $RESAMPLER_JOB_NAME"
}

get_job_roles
case $JOB_TYPE in
    consolidator) deploy_consolidator ;;
    resampler) deploy_resampler ;;
    all) deploy_consolidator; deploy_resampler ;;
    *) echo "Unknown job type: $JOB_TYPE"; usage; exit 1 ;;
esac
echo "✅ Archived batch job deployment complete."
