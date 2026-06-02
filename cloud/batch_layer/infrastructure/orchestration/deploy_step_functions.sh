#!/bin/bash
# Deploy Step Functions State Machine for Daily OHLCV Pipeline
# Location: infrastructure/orchestration/deploy_step_functions.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_REGION=${AWS_REGION:-ca-west-1}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}

STATE_MACHINE_NAME="dev-daily-ohlcv-pipeline"
ROLE_NAME="dev-step-functions-pipeline-role"
SNS_TOPIC_NAME="condvest-pipeline-alerts"

echo "============================================================"
echo "🚀 Deploying Step Functions State Machine"
echo "============================================================"
echo "📍 Region: $AWS_REGION"
echo "📋 State Machine: $STATE_MACHINE_NAME"
echo ""

# Step 1: Create SNS Topic for alerts (if not exists)
echo "📢 Creating SNS topic for pipeline alerts..."
SNS_TOPIC_ARN=$(aws sns create-topic \
    --name "$SNS_TOPIC_NAME" \
    --region "$AWS_REGION" \
    --query 'TopicArn' \
    --output text 2>/dev/null || echo "")

if [ -n "$SNS_TOPIC_ARN" ]; then
    echo "✅ SNS Topic: $SNS_TOPIC_ARN"
else
    SNS_TOPIC_ARN="arn:aws:sns:$AWS_REGION:$AWS_ACCOUNT_ID:$SNS_TOPIC_NAME"
    echo "ℹ️  Using existing SNS Topic: $SNS_TOPIC_ARN"
fi

# Step 2: Create IAM Role for Step Functions (if not exists)
echo ""
echo "🔐 Creating IAM role for Step Functions..."

TRUST_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "states.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'

# Check if role exists
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || echo "")

if [ -z "$ROLE_ARN" ]; then
    echo "Creating new role: $ROLE_NAME"
    ROLE_ARN=$(aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY" \
        --query 'Role.Arn' \
        --output text)
    
    # Wait for role to propagate
    echo "⏳ Waiting for role to propagate..."
    sleep 10
else
    echo "ℹ️  Using existing role: $ROLE_ARN"
fi

# Step 3: Attach policies to the role
echo ""
echo "📎 Attaching policies to role..."

# Policy for Lambda invocation
LAMBDA_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:'$AWS_REGION':'$AWS_ACCOUNT_ID':function:dev-batch-daily-ohlcv-fetcher",
                "arn:aws:lambda:'$AWS_REGION':'$AWS_ACCOUNT_ID':function:dev-batch-daily-ohlcv-planner",
                "arn:aws:lambda:'$AWS_REGION':'$AWS_ACCOUNT_ID':function:dev-batch-daily-meta-fetcher",
                "arn:aws:lambda:'$AWS_REGION':'$AWS_ACCOUNT_ID':function:dev-batch-daily-ohlcv-ingest-handler",
                "arn:aws:lambda:'$AWS_REGION':'$AWS_ACCOUNT_ID':function:dev-batch-daily-meta-ingest-handler",
                "arn:aws:lambda:'$AWS_REGION':'$AWS_ACCOUNT_ID':function:dev-batch-scanner-snapshot-builder",
                "arn:aws:lambda:'$AWS_REGION':'$AWS_ACCOUNT_ID':function:dev-batch-vectorized-scanner"
            ]
        }
    ]
}'

aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "LambdaInvokePolicy" \
    --policy-document "$LAMBDA_POLICY" 2>/dev/null || echo "Lambda policy already exists"

# Policy for Batch job submission
BATCH_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "batch:SubmitJob",
                "batch:DescribeJobs",
                "batch:TerminateJob"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule"
            ],
            "Resource": "arn:aws:events:'$AWS_REGION':'$AWS_ACCOUNT_ID':rule/StepFunctionsGetEventsForBatchJobsRule"
        }
    ]
}'

aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "BatchJobPolicy" \
    --policy-document "$BATCH_POLICY" 2>/dev/null || echo "Batch policy already exists"

# Policy for SNS publishing
SNS_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sns:Publish",
            "Resource": "'$SNS_TOPIC_ARN'"
        }
    ]
}'

aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "SNSPublishPolicy" \
    --policy-document "$SNS_POLICY" 2>/dev/null || echo "SNS policy already exists"

# Policy for X-Ray tracing (optional but recommended)
aws iam attach-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess" 2>/dev/null || echo "X-Ray policy already attached"

echo "✅ Policies attached"

# Step 4: Update SNS Topic ARN in state machine definition
echo ""
echo "📝 Updating state machine definition with SNS Topic ARN..."
sed -i.bak "s|arn:aws:sns:ca-west-1:471112909340:condvest-pipeline-alerts|$SNS_TOPIC_ARN|g" "$SCRIPT_DIR/state_machine_definition.json"

# Step 5: Create or update State Machine
echo ""
echo "🔧 Creating/updating State Machine..."

STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines \
    --region "$AWS_REGION" \
    --query "stateMachines[?name=='$STATE_MACHINE_NAME'].stateMachineArn" \
    --output text)

DEFINITION=$(cat "$SCRIPT_DIR/state_machine_definition.json")

if [ -z "$STATE_MACHINE_ARN" ]; then
    echo "Creating new state machine: $STATE_MACHINE_NAME"
    STATE_MACHINE_ARN=$(aws stepfunctions create-state-machine \
        --name "$STATE_MACHINE_NAME" \
        --definition "$DEFINITION" \
        --role-arn "$ROLE_ARN" \
        --type "STANDARD" \
        --tracing-configuration enabled=true \
        --region "$AWS_REGION" \
        --query 'stateMachineArn' \
        --output text)
    echo "✅ Created: $STATE_MACHINE_ARN"
else
    echo "Updating existing state machine: $STATE_MACHINE_NAME"
    aws stepfunctions update-state-machine \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --definition "$DEFINITION" \
        --role-arn "$ROLE_ARN" \
        --tracing-configuration enabled=true \
        --region "$AWS_REGION"
    echo "✅ Updated: $STATE_MACHINE_ARN"
fi

# Step 6: Create EventBridge Schedule (daily at 4:05 PM America/New_York)
echo ""
echo "⏰ Creating EventBridge schedule..."

SCHEDULE_ROLE_NAME="dev-eventbridge-stepfunctions-role"

# Create EventBridge role if not exists
SCHEDULE_ROLE_ARN=$(aws iam get-role --role-name "$SCHEDULE_ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || echo "")

if [ -z "$SCHEDULE_ROLE_ARN" ]; then
    SCHEDULE_TRUST_POLICY='{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "scheduler.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }'
    
    SCHEDULE_ROLE_ARN=$(aws iam create-role \
        --role-name "$SCHEDULE_ROLE_NAME" \
        --assume-role-policy-document "$SCHEDULE_TRUST_POLICY" \
        --query 'Role.Arn' \
        --output text)
    
    # Add permission to start state machine
    STEPFUNCTIONS_POLICY='{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "states:StartExecution",
                "Resource": "'$STATE_MACHINE_ARN'"
            }
        ]
    }'
    
    aws iam put-role-policy \
        --role-name "$SCHEDULE_ROLE_NAME" \
        --policy-name "StartStepFunctionsPolicy" \
        --policy-document "$STEPFUNCTIONS_POLICY"
    
    echo "⏳ Waiting for schedule role to propagate..."
    sleep 10
fi

# Create EventBridge Scheduler schedule
SCHEDULE_NAME="dev-daily-ohlcv-pipeline-schedule"
SCHEDULE_EXPRESSION="cron(5 16 ? * MON-FRI *)"
SCHEDULE_TIMEZONE="America/New_York"

aws scheduler create-schedule \
    --name "$SCHEDULE_NAME" \
    --schedule-expression "$SCHEDULE_EXPRESSION" \
    --schedule-expression-timezone "$SCHEDULE_TIMEZONE" \
    --flexible-time-window '{"Mode": "OFF"}' \
    --target "{
        \"Arn\": \"$STATE_MACHINE_ARN\",
        \"RoleArn\": \"$SCHEDULE_ROLE_ARN\",
        \"Input\": \"{}\"
    }" \
    --state "ENABLED" \
    --region "$AWS_REGION" 2>/dev/null || \
aws scheduler update-schedule \
    --name "$SCHEDULE_NAME" \
    --schedule-expression "$SCHEDULE_EXPRESSION" \
    --schedule-expression-timezone "$SCHEDULE_TIMEZONE" \
    --flexible-time-window '{"Mode": "OFF"}' \
    --target "{
        \"Arn\": \"$STATE_MACHINE_ARN\",
        \"RoleArn\": \"$SCHEDULE_ROLE_ARN\",
        \"Input\": \"{}\"
    }" \
    --state "ENABLED" \
    --region "$AWS_REGION"

echo "✅ Schedule created: $SCHEDULE_NAME (4:05 PM America/New_York, Mon-Fri)"

# Restore original state machine definition
mv "$SCRIPT_DIR/state_machine_definition.json.bak" "$SCRIPT_DIR/state_machine_definition.json" 2>/dev/null || true

echo ""
echo "============================================================"
echo "✅ Step Functions Deployment Complete!"
echo "============================================================"
echo ""
echo "📋 Resources Created:"
echo "   • State Machine: $STATE_MACHINE_ARN"
echo "   • IAM Role: $ROLE_ARN"
echo "   • SNS Topic: $SNS_TOPIC_ARN"
echo "   • Schedule: $SCHEDULE_NAME (4:05 PM America/New_York, Mon-Fri)"
echo ""
echo "🔗 AWS Console Links:"
echo "   • Step Functions: https://$AWS_REGION.console.aws.amazon.com/states/home?region=$AWS_REGION#/statemachines/view/$STATE_MACHINE_ARN"
echo "   • EventBridge Scheduler: https://$AWS_REGION.console.aws.amazon.com/scheduler/home?region=$AWS_REGION#schedules/$SCHEDULE_NAME"
echo ""
echo "🧪 To test manually:"
echo "   aws stepfunctions start-execution --state-machine-arn $STATE_MACHINE_ARN --region $AWS_REGION"
echo ""

