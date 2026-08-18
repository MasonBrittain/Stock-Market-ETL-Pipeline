# Changelog

All notable changes to this project are documented here. The project evolved
across four versions, from a single-file batch script to a cloud-native
platform with a real-time streaming lane. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [4.0.0] - 2026-07-10 — Streaming

Added a near-real-time streaming lane alongside the batch pipeline. The two
lanes share one warehouse.

### Added
- Replay producer streaming Bronze Parquet to Kafka as tick messages at a
  configurable rate, simulating a live market feed
- Apache Kafka (KRaft mode, no ZooKeeper) in Docker Compose with healthchecks
- Spark Structured Streaming processor: 1-minute event-time OHLCV bars per
  ticker with a 30-second watermark and checkpointing
- Dead-letter topic for malformed messages so the stream never crashes on
  bad input
- Price-move alerting against a 5-minute rolling average (`alerts` table)
- `fact_intraday_bars` table and a consumer-lag / throughput health CLI
- Azure Event Hubs documented as a Kafka-compatible cloud opt-in
- 18 unit tests for the streaming logic (45 total), no broker required

### Verified
- Bars for all tickers with correct OHLCV per window
- Checkpoint recovery after mid-stream restart with zero duplicate records
- Malformed messages routed to the dead-letter queue without job failure

## [3.0.0] - 2026-07-07 — Cloud Migration

Moved the batch pipeline to Azure while keeping local-first development fully
working. Cloud targets are opt-in via configuration only.

### Added
- Bronze landing layer writing raw Parquet, partitioned by ticker and year
  (local filesystem or Azure Blob Storage)
- Azure SQL Database as the cloud warehouse target
- Azure Functions timer trigger (weekdays 22:30 UTC) invoking the pipeline
- CI/CD with GitHub Actions: tests on every push, deploy on merge to main
- Power BI connection guide with star-schema model and DAX measures
- Annotated Azure CLI provisioning script (free-tier resources throughout)

### Changed
- Configuration extended for storage target, log level, and lookback window
- Warehouse writes made portable across SQLite and Azure SQL (chunked inserts,
  driver-specific tuning)

## [2.0.0] - 2026-06-29 — Production-Style Batch

Reworked the single-table script into a production-style batch pipeline.

### Added
- Incremental loading: only new trading days are downloaded per ticker
- Star schema — `fact_stock_prices` with `dim_company` and `dim_date`
- `pipeline_runs` audit table and a `batch_id` traceable through every row,
  log line, and quality report
- Structured logging with rotating files
- Nine-check data quality framework with tiered severity and JSON reports
- Docker support and a V1-to-V2 migration script
- Expanded pytest suite

### Changed
- Loading is now idempotent; reruns never create duplicates

## [1.0.0] - 2026-06-08 — Initial Release

First working batch ETL pipeline.

### Added
- Extraction of historical prices from Yahoo Finance (`yfinance`)
- pandas transformation with daily return calculation
- Loading into SQLite via SQLAlchemy
- Modular extract / transform / load architecture with basic validation
