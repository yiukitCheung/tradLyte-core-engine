#!/usr/bin/env bash
# Create/update HTTP API Gateway routes for dev-serving-api.
#
# Usage:
#   AWS_REGION=ca-west-1 FUNCTION_NAME=dev-serving-api STAGE_NAME=v1 \
#   API_NAME=dev-serving-http-api ALLOWED_ORIGIN=https://app.tradlyte.com \
#   ./cloud/serving_layer/infrastructure/serving_api/deploy_http_api.sh

set -euo pipefail
export AWS_PAGER=""

AWS_REGION="${AWS_REGION:-ca-west-1}"
API_NAME="${API_NAME:-dev-serving-http-api}"
FUNCTION_NAME="${FUNCTION_NAME:-dev-serving-api}"
STAGE_NAME="${STAGE_NAME:-v1}"
ALLOWED_ORIGIN="${ALLOWED_ORIGIN:-*}"
THROTTLE_BURST="${THROTTLE_BURST:-50}"
THROTTLE_RATE="${THROTTLE_RATE:-25}"

echo "🚀 Deploying HTTP API Gateway for $FUNCTION_NAME"
echo "Region: $AWS_REGION"
echo "API Name: $API_NAME"

FUNCTION_ARN="$(aws lambda get-function \
  --function-name "$FUNCTION_NAME" \
  --region "$AWS_REGION" \
  --query 'Configuration.FunctionArn' \
  --output text)"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text --region "$AWS_REGION")"

API_ID="$(aws apigatewayv2 get-apis \
  --region "$AWS_REGION" \
  --query "Items[?Name=='$API_NAME'].ApiId | [0]" \
  --output text)"

if [[ -z "$API_ID" || "$API_ID" == "None" ]]; then
  API_ID="$(aws apigatewayv2 create-api \
    --name "$API_NAME" \
    --protocol-type HTTP \
    --cors-configuration "AllowOrigins=[$ALLOWED_ORIGIN],AllowHeaders=[content-type,x-api-key],AllowMethods=[GET,POST,OPTIONS]" \
    --region "$AWS_REGION" \
    --query 'ApiId' \
    --output text)"
  echo "➕ Created API: $API_ID"
else
  echo "✔ API exists: $API_ID"
  aws apigatewayv2 update-api \
    --api-id "$API_ID" \
    --cors-configuration "AllowOrigins=[$ALLOWED_ORIGIN],AllowHeaders=[content-type,x-api-key],AllowMethods=[GET,POST,OPTIONS]" \
    --region "$AWS_REGION" >/dev/null
fi

INTEGRATION_ID="$(aws apigatewayv2 get-integrations \
  --api-id "$API_ID" \
  --region "$AWS_REGION" \
  --query "Items[?IntegrationUri=='$FUNCTION_ARN'].IntegrationId | [0]" \
  --output text)"

if [[ -z "$INTEGRATION_ID" || "$INTEGRATION_ID" == "None" ]]; then
  INTEGRATION_ID="$(aws apigatewayv2 create-integration \
    --api-id "$API_ID" \
    --integration-type AWS_PROXY \
    --integration-uri "$FUNCTION_ARN" \
    --payload-format-version 2.0 \
    --timeout-in-millis 10000 \
    --region "$AWS_REGION" \
    --query 'IntegrationId' \
    --output text)"
  echo "➕ Created Lambda integration: $INTEGRATION_ID"
else
  echo "✔ Integration exists: $INTEGRATION_ID"
fi

declare -a ROUTES=(
  "GET /health"
  "GET /screener/quotes"
  "POST /backtest"
  "GET /picks/today"
  "GET /picks/today/metadata"
  "GET /picks/detail"
  "GET /picks/{scan_date}/returns"
  "GET /market/quote/{symbol}"
  "GET /market/news/{symbol}"
  "GET /market/ohlcv/{symbol}"
  "GET /market/returns/{symbol}"
)

declare -a DEPRECATED_ROUTES=(
  "GET /v1/screener/quotes"
  "GET /v1/picks/today"
  "GET /v1/picks/{scan_date}/returns"
  "GET /v1/market/quote/{symbol}"
  "GET /v1/market/ohlcv/{symbol}"
  "GET /v1/market/returns/{symbol}"
)

for ROUTE_KEY in "${ROUTES[@]}"; do
  ROUTE_ID="$(aws apigatewayv2 get-routes \
    --api-id "$API_ID" \
    --region "$AWS_REGION" \
    --query "Items[?RouteKey=='$ROUTE_KEY'].RouteId | [0]" \
    --output text)"
  if [[ -z "$ROUTE_ID" || "$ROUTE_ID" == "None" ]]; then
    aws apigatewayv2 create-route \
      --api-id "$API_ID" \
      --route-key "$ROUTE_KEY" \
      --target "integrations/$INTEGRATION_ID" \
      --region "$AWS_REGION" >/dev/null
    echo "➕ Added route: $ROUTE_KEY"
  else
    aws apigatewayv2 update-route \
      --api-id "$API_ID" \
      --route-id "$ROUTE_ID" \
      --target "integrations/$INTEGRATION_ID" \
      --region "$AWS_REGION" >/dev/null
    echo "🔄 Updated route: $ROUTE_KEY"
  fi
done

for ROUTE_KEY in "${DEPRECATED_ROUTES[@]}"; do
  ROUTE_ID="$(aws apigatewayv2 get-routes \
    --api-id "$API_ID" \
    --region "$AWS_REGION" \
    --query "Items[?RouteKey=='$ROUTE_KEY'].RouteId | [0]" \
    --output text)"
  if [[ -n "$ROUTE_ID" && "$ROUTE_ID" != "None" ]]; then
    aws apigatewayv2 delete-route \
      --api-id "$API_ID" \
      --route-id "$ROUTE_ID" \
      --region "$AWS_REGION" >/dev/null
    echo "🗑️  Removed deprecated route: $ROUTE_KEY"
  fi
done

STAGE_EXISTS="$(aws apigatewayv2 get-stages \
  --api-id "$API_ID" \
  --region "$AWS_REGION" \
  --query "Items[?StageName=='$STAGE_NAME'].StageName | [0]" \
  --output text)"

if [[ -z "$STAGE_EXISTS" || "$STAGE_EXISTS" == "None" ]]; then
  aws apigatewayv2 create-stage \
    --api-id "$API_ID" \
    --stage-name "$STAGE_NAME" \
    --auto-deploy \
    --default-route-settings "DetailedMetricsEnabled=true,ThrottlingBurstLimit=$THROTTLE_BURST,ThrottlingRateLimit=$THROTTLE_RATE" \
    --region "$AWS_REGION" >/dev/null
  echo "➕ Created stage: $STAGE_NAME"
else
  aws apigatewayv2 update-stage \
    --api-id "$API_ID" \
    --stage-name "$STAGE_NAME" \
    --default-route-settings "DetailedMetricsEnabled=true,ThrottlingBurstLimit=$THROTTLE_BURST,ThrottlingRateLimit=$THROTTLE_RATE" \
    --auto-deploy \
    --region "$AWS_REGION" >/dev/null
  echo "🔄 Updated stage: $STAGE_NAME"
fi

STATEMENT_ID="apigw-${API_ID}-${STAGE_NAME}"
aws lambda add-permission \
  --function-name "$FUNCTION_NAME" \
  --statement-id "$STATEMENT_ID" \
  --action lambda:InvokeFunction \
  --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:${AWS_REGION}:${ACCOUNT_ID}:${API_ID}/*/*/*" \
  --region "$AWS_REGION" >/dev/null 2>&1 || true

echo ""
echo "✅ HTTP API ready."
echo "Base URL: https://${API_ID}.execute-api.${AWS_REGION}.amazonaws.com/${STAGE_NAME}"
echo ""
echo "Note: HTTP API Gateway does not enforce API keys natively."
echo "MVP key auth is enforced by Lambda via SERVING_API_KEY + x-api-key header."
