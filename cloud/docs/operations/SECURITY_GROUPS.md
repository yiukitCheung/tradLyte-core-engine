# TradLyte Security Groups — Consolidated Reference

Region: `ca-west-1`. VPC: `vpc-08d9b6452ae7d1534`.

Use **Name** and **Purpose** tags (AWS SG Description cannot be changed after creation).

## Active SGs (6)

| Name tag | SG ID | Attached to | Purpose |
|----------|-------|-------------|---------|
| `tradlyte-lambda-vpc` | `sg-0ba530c61e0d9ba17` | All VPC Lambdas | HTTPS egress + PostgreSQL to RDS/Proxy |
| `tradlyte-rds` | `sg-0f54a5a73687acb96` | RDS `dev-batch-postgres` | Inbound 5432 from Lambdas, Proxy, Batch only |
| `tradlyte-rds-proxy` | `sg-0f5d696a6518b448a` | RDS Proxy + endpoint | Pooled DB connections |
| `tradlyte-batch-fargate` | `sg-04e9295d946ae4707` | Batch Fargate CE | Aggregator containers |
| `tradlyte-vpc-endpoints` | `sg-095792ac68b79fbad` | SM + Lambda VPCEs | Private API access without NAT |
| `tradlyte-vpc-default` | `sg-01774f2e460d1c778` | VPC default | Self-ref + Batch→endpoint 443 |

**Hardened (applied):** no open `5432/0.0.0.0/0` on default SG, no dev IP, no `172.31.0.0/16` on RDS.

---

## `tradlyte-rds` inbound (SG-referenced only)

| Port | Source SG | Consumer |
|------|-----------|----------|
| 5432 | `tradlyte-lambda-vpc` | planner, ingest, snapshot, scanner, serving-api |
| 5432 | `tradlyte-rds-proxy` | Proxy → DB |
| 5432 | `tradlyte-batch-fargate` | scanner-aggregator |

---

## Operations scripts

```bash
# Tag all SGs
./cloud/infrastructure/common/tag_security_groups.sh

# Merge duplicate RDS SG (one-time)
./cloud/infrastructure/common/consolidate_rds_security_group.sh

# Remove broad ingress rules (applied)
./cloud/infrastructure/common/harden_security_groups.sh
```

### Dev DB access after hardening

Direct IP rules removed. Use SSM port-forward:

```bash
aws ssm start-session \
  --target <ssm-managed-instance-id> \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["<rds-endpoint>"],"portNumber":["5432"],"localPortNumber":["5433"]}'
```

Connect locally: `psql -h 127.0.0.1 -p 5433 -U <user> -d condvest`
