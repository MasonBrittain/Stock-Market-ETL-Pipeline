# Streaming Layer (V4)

Near-real-time lane alongside the batch pipeline: a replay producer streams
tick messages through Kafka, and a Spark Structured Streaming job aggregates
them into 1-minute OHLCV bars in the warehouse.

```
Bronze Parquet ──▶ Producer ──▶ Kafka (stock-ticks) ──▶ Spark Structured Streaming
                                    │                        │
                                    ▼                        ▼
                          stock-ticks-dlq          fact_intraday_bars + alerts
                          (malformed msgs)         (1-min OHLCV, 5-min avg)
```

## Quickstart

```powershell
# 1. Land replay data (once): run the batch pipeline
python -m src.main

# 2. Start the streaming stack
docker compose -f streaming/docker-compose.streaming.yml up --build

# 3. Watch bars accumulate (separate terminal)
python streaming/monitor.py
```

Within ~2–3 minutes of startup, `fact_intraday_bars` receives its first bars
(the first window must close plus the 30-second watermark must pass).

## Message Schema

Topic `stock-ticks`, JSON, keyed by ticker:

```json
{
  "ticker": "AAPL",
  "price": 281.74,
  "volume": 66427000,
  "event_time": "2026-07-07T14:30:00.123456+00:00",
  "source": "replay",
  "producer_id": "replay-1"
}
```

`source` is `replay` (Bronze re-stream, default) or `live` (reserved for a
future live feed — the processor treats both identically).

## Processing Semantics

- **Event-time windows** — bars are built on `event_time`, not arrival time,
  with a **30-second watermark**: ticks arriving up to 30s late are still
  counted; later ones are dropped and the window finalises.
- **Append output mode** — a bar is emitted exactly once, when its window
  finalises. The `foreachBatch` sink also deduplicates against the warehouse
  on `(ticker, window_start)`, so restarts never produce duplicate bars.
- **Checkpointing** — Kafka offsets and window state persist to the
  `checkpoints` Docker volume. Killing and restarting the processor resumes
  from the last committed batch. Delete the volume to reprocess from scratch:
  `docker compose -f streaming/docker-compose.streaming.yml down -v`.
- **Dead-letter queue** — messages failing schema validation (missing fields,
  negative price, bad timestamp, unknown source) are forwarded raw to
  `stock-ticks-dlq` and never crash the job.

## Alerts

When a finalised bar's close moves more than `ALERT_THRESHOLD_PCT` (default
2%) against that ticker's 5-minute average close, a row is written to the
`alerts` table and a WARNING is logged. Tune the threshold in `.env`.

## Inspecting the DLQ

```powershell
docker compose -f streaming/docker-compose.streaming.yml exec kafka `
  kafka-console-consumer.sh --bootstrap-server localhost:9092 `
  --topic stock-ticks-dlq --from-beginning --max-messages 10
```

## Monitoring

`streaming/monitor.py` prints consumer lag, bar counts, recent throughput,
and alert totals; it exits non-zero when lag exceeds `--max-lag` (default
1000), so it can run as a scheduled health check.

Healthy looks like: `bars_last_5min` > 0 while the producer runs and
`latest_processed_at` within the last minute. Consumer lag usually shows
`n/a` — Spark Structured Streaming tracks offsets in its checkpoint rather
than committing to Kafka consumer groups, so the freshness signal to watch
is `latest_processed_at`.

## Cloud Option: Azure Event Hubs (~$11/month — off by default)

Event Hubs exposes a Kafka-compatible endpoint, so the same producer and
processor code targets it with configuration only:

1. Uncomment and run the Event Hubs section of `scripts/provision_azure.ps1`
2. Set in `.env`: `STREAM_BROKER=eventhubs` and `KAFKA_SASL_CONNECTION_STRING`
   (the namespace connection string)
3. Point `KAFKA_BOOTSTRAP` at `<namespace>.servicebus.windows.net:9093` with
   SASL_SSL — see the commented reference in the provisioning script
4. **Tear it down after the demo** — the namespace bills hourly while it exists

## Troubleshooting

- **No bars after 5 minutes** — check the producer logs
  (`docker compose ... logs producer`); the most common cause is an empty
  `data/bronze/` (run the batch pipeline first).
- **Processor restarts repeatedly on first run** — the Kafka connector JARs
  download from Maven Central on first start; slow networks can hit the
  restart policy once or twice before the cache warms. It self-heals.
- **`database is locked` (SQLite)** — stop any local process holding the
  warehouse open (a second pipeline run, a DB browser) while the processor
  is writing.
