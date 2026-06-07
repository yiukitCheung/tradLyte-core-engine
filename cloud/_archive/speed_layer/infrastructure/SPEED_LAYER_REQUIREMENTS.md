# Speed Layer (archived design)

A real-time path that was designed but **not deployed**. Parked for the MVP; code preserved under `cloud/_archive/speed_layer/`.

## Intended flow

```
Polygon WebSocket → ECS service → Kinesis (raw 1m)
                                      → Kinesis Analytics (Flink SQL) resample (5m/15m/30m/1h/2h/4h)
                                      → Lambda → DynamoDB (per-interval tables, TTL retention)
```

- **ECS service** maintains the Polygon WebSocket connection and pushes 1-minute candles to Kinesis.
- **Kinesis Analytics (Flink SQL)** resamples to higher intervals.
- **Lambda** writes resampled candles to per-interval DynamoDB tables with TTL-based retention.

Archived files: `kinesis_analytics/` (Flink SQL apps), `fetching/` (ECS WebSocket fetcher), `lambda_functions/` (Kinesis → DynamoDB handlers).
