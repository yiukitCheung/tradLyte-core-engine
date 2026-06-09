#!/bin/bash
# Remove overly broad ingress rules from TradLyte security groups.
#
# 1. tradlyte-vpc-default  — drop inbound 5432 from 0.0.0.0/0
# 2. tradlyte-rds          — drop dev-machine IP 75.155.41.148/32
# 3. tradlyte-rds          — drop VPC-CIDR 172.31.0.0/16 (SG-referenced rules remain)
#
# After this, RDS accepts PostgreSQL only from:
#   tradlyte-lambda-vpc, tradlyte-rds-proxy, tradlyte-batch-fargate
#
# Dev DB access: use SSM port-forwarding through a VPC-attached instance or
# Session Manager to a bastion — not a public IP rule on RDS.
#
# Usage:
#   ./harden_security_groups.sh
#   DRY_RUN=1 ./harden_security_groups.sh

set -euo pipefail
export AWS_PAGER=""

AWS_REGION="${AWS_REGION:-ca-west-1}"
DRY_RUN="${DRY_RUN:-0}"

DEFAULT_SG="${TRADLYTE_DEFAULT_SG:-sg-01774f2e460d1c778}"
RDS_SG="${TRADLYTE_RDS_SG:-sg-0f54a5a73687acb96}"
DEV_IP_CIDR="${TRADLYTE_DEV_IP_CIDR:-75.155.41.148/32}"
VPC_CIDR="${TRADLYTE_VPC_CIDR:-172.31.0.0/16}"

revoke_rule() {
  local group_id="$1"
  local rule_id="$2"
  local label="$3"
  if [ "$DRY_RUN" = "1" ]; then
    echo "[DRY_RUN] revoke $label ($rule_id)"
  else
    aws ec2 revoke-security-group-ingress \
      --group-id "$group_id" \
      --security-group-rule-ids "$rule_id" \
      --region "$AWS_REGION"
    echo "REVOKED $label ($rule_id)"
  fi
}

echo "============================================================"
echo "  Harden TradLyte security groups ($AWS_REGION)"
echo "============================================================"

# --- 1. Default SG: open PostgreSQL to the internet ---
RULE=$(aws ec2 describe-security-group-rules --region "$AWS_REGION" \
  --filters "Name=group-id,Values=$DEFAULT_SG" \
  --query "SecurityGroupRules[?IsEgress==\`false\` && FromPort==\`5432\` && CidrIpv4==\`0.0.0.0/0\`].SecurityGroupRuleId | [0]" \
  --output text)
if [ -n "$RULE" ] && [ "$RULE" != "None" ]; then
  revoke_rule "$DEFAULT_SG" "$RULE" "default SG inbound 5432 from 0.0.0.0/0"
else
  echo "SKIP  default SG open 5432 — already removed"
fi

# --- 2. RDS SG: dev machine IP ---
RULE=$(aws ec2 describe-security-group-rules --region "$AWS_REGION" \
  --filters "Name=group-id,Values=$RDS_SG" \
  --query "SecurityGroupRules[?IsEgress==\`false\` && FromPort==\`5432\` && CidrIpv4==\`${DEV_IP_CIDR}\`].SecurityGroupRuleId | [0]" \
  --output text)
if [ -n "$RULE" ] && [ "$RULE" != "None" ]; then
  revoke_rule "$RDS_SG" "$RULE" "RDS SG dev IP $DEV_IP_CIDR"
else
  echo "SKIP  RDS dev IP — already removed"
fi

# --- 3. RDS SG: whole VPC CIDR ---
RULE=$(aws ec2 describe-security-group-rules --region "$AWS_REGION" \
  --filters "Name=group-id,Values=$RDS_SG" \
  --query "SecurityGroupRules[?IsEgress==\`false\` && FromPort==\`5432\` && CidrIpv4==\`${VPC_CIDR}\`].SecurityGroupRuleId | [0]" \
  --output text)
if [ -n "$RULE" ] && [ "$RULE" != "None" ]; then
  revoke_rule "$RDS_SG" "$RULE" "RDS SG VPC CIDR $VPC_CIDR"
else
  echo "SKIP  RDS VPC CIDR — already removed"
fi

echo ""
echo "Remaining RDS inbound (SG-referenced only):"
aws ec2 describe-security-group-rules --region "$AWS_REGION" \
  --filters "Name=group-id,Values=$RDS_SG" \
  --query 'SecurityGroupRules[?IsEgress==`false`].{Port:FromPort,Peer:ReferencedGroupInfo.GroupId,Cidr:CidrIpv4}' \
  --output table

echo ""
echo "Done. Dev DB access: SSM port-forward — see SECURITY_GROUPS.md"
