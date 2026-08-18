# Stock Market ETL Pipeline

[![CI](https://github.com/MasonBrittain/Stock-Market-ETL-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/MasonBrittain/Stock-Market-ETL-Pipeline/actions/workflows/ci.yml)

A cloud-native data platform with two lanes: a **batch ETL pipeline** that
downloads historical stock prices from Yahoo Finance into a star-schema
warehouse (SQLite locally, Azure SQL in the cloud), and a **streaming lane**
where Kafka and Spark Structured Streaming aggregate tick messages into
1-minute intraday bars in near real time. Includes a Bronze Parquet landing
layer, a 9-check data quality framework, full run auditing, scheduled
execution on Azure Functions, and CI/CD via GitHub Actions.

**Runs 100% locally with zero cloud dependencies by default.** Azure targets
are opt-in via configuration.

---

## Architecture

```
Yahoo Finance API (yfinance)
          │
          ▼
  ┌───────────────┐
  │    Extract    │  Per-ticker date windows · partial-failure handling
  └───────┬───────┘
          │  raw DataFrame (flat, per-ticker)
          ▼
  ┌───────────────┐
  │   Transform   │  Type coercion · deduplication · daily_return calc
  └───────┬───────┘
          │  clean DataFrame
          ▼
  ┌───────────────┐
  │Quality Checks │  9 checks · non-fatal collection · JSON report
  └───────┬───────┘
          │  validated DataFrame
          ▼
  ┌───────────────┐
  │     Load      │  Dedup against DB · dim FK enrichment · batch_id tag
  └───────┬───────┘
          │
          ▼
  Warehouse (SQLite local · Azure SQL cloud)
  ├── dim_company          (who)
  ├── dim_date             (when — pre-seeded 2000–2035)
  ├── fact_stock_prices    (measures + FKs + batch_id)
  └── pipeline_runs        (audit log per execution)
          │
          ▼
  reports/quality_report_{batch_id}.json
  logs/pipeline.log
```

### Cloud deployment (V3)

```
              GitHub push to main
                     │
        GitHub Actions: test → deploy
                     │
                     ▼
   Azure Functions (timer, weekdays 22:30 UTC)
                     │ runs run_pipeline()
        ┌────────────┴────────────┐
        ▼                         ▼
  Azure Blob Storage        Azure SQL Database
  bronze/ticker=X/year=Y    (serverless free tier)
  raw Parquet extracts      star schema + audit
                                  │
                                  ▼
                            Power BI Desktop
```

Extraction lands raw Parquet in Bronze *before* transformation, so any batch
can be reprocessed without re-calling the Yahoo Finance API.

### Streaming lane (V4)

```
Bronze Parquet ──▶ Replay Producer ──▶ Kafka (KRaft, Docker)
                                          │
                        ┌─────────────────┴───────┐
                        ▼                         ▼
              Spark Structured Streaming   stock-ticks-dlq
              1-min OHLCV bars, 30s        (malformed messages)
              watermark, checkpointed
                        │
                        ▼
         fact_intraday_bars + alerts (same warehouse)
```

The producer replays historical Bronze data as a simulated live feed —
deterministic, demo-able any time, and swapping in a real feed is a
producer-only change. Azure Event Hubs (Kafka-compatible endpoint) is the
documented cloud opt-in. See [docs/streaming.md](docs/streaming.md).

---

## Project Structure

```
stock-market-etl-pipeline/
├── .github/workflows/
│   ├── ci.yml              Tests + coverage on every push and PR
│   └── deploy.yml          Function App deployment on merge to main
├── src/
│   ├── config.py           Configuration (tickers, DB URL, Bronze target, logging)
│   ├── logger.py           Rotating file + console logging with batch_id
│   ├── database.py         Schema definitions, dim seeding, utility queries
│   ├── storage.py          Bronze backends: local Parquet or Azure Blob
│   ├── extract.py          Per-ticker incremental download from Yahoo Finance
│   ├── transform.py        Clean, coerce, and calculate daily returns
│   ├── quality_checks.py   9 validation checks + JSON report writer
│   ├── load.py             Incremental insert into fact_stock_prices
│   └── main.py             run_pipeline() core + CLI entry point
├── function_app/
│   ├── function_app.py     Azure Functions timer trigger (weekdays 22:30 UTC)
│   ├── host.json
│   ├── requirements.txt
│   └── local.settings.json.example
├── streaming/
│   ├── docker-compose.streaming.yml   Kafka (KRaft) + producer + Spark
│   ├── common/ticks.py     Shared tick schema, validation, alert rules
│   ├── producer/           Replays Bronze Parquet to Kafka as tick messages
│   ├── processor/          Spark job: 1-min OHLCV bars → warehouse
│   └── monitor.py          Consumer-lag + throughput health CLI
├── tests/                  45 tests — all runnable without cloud credentials
├── scripts/
│   ├── migrate_v1_to_v2.py   One-time migration for existing V1 databases
│   └── provision_azure.ps1   Annotated az CLI script — creates all resources
├── docs/
│   ├── powerbi.md          Power BI connection guide + DAX measures
│   └── streaming.md        Streaming quickstart, semantics, troubleshooting
├── data/                   SQLite database + bronze/ Parquet (local mode)
├── logs/                   Rotating log files
├── reports/                JSON quality reports (one per run)
├── .env.example            All configurable settings with defaults
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Database Schema

### fact_stock_prices

| Column | Type | Description |
|---|---|---|
| `ticker` | TEXT | Stock ticker symbol |
| `price_date` | DATETIME | Trading date |
| `open_price` | FLOAT | Opening price |
| `high_price` | FLOAT | Daily high |
| `low_price` | FLOAT | Daily low |
| `close_price` | FLOAT | Closing price |
| `adj_close_price` | FLOAT | Split/dividend-adjusted close |
| `volume` | BIGINT | Shares traded |
| `daily_return` | FLOAT | `pct_change` of close vs. prior trading day |
| `loaded_at` | DATETIME | UTC timestamp of transformation |
| `company_id` | INT | FK → dim_company |
| `date_id` | INT | FK → dim_date (YYYYMMDD) |
| `batch_id` | TEXT | UUID of the pipeline run that inserted this row |

Unique constraint: `(ticker, price_date)`

### dim_company

| Column | Type | Description |
|---|---|---|
| `company_id` | INT PK | Surrogate key |
| `ticker` | TEXT UNIQUE | Ticker symbol |
| `company_name` | TEXT | Full name from yfinance |
| `sector` | TEXT | Sector from yfinance |
| `industry` | TEXT | Industry from yfinance |
| `created_at` | DATETIME | First insert timestamp |
| `updated_at` | DATETIME | Last update timestamp |

### dim_date

Pre-seeded for 2000-01-01 through 2035-12-31.

| Column | Description |
|---|---|
| `date_id` | YYYYMMDD integer (PK) |
| `full_date` | Calendar date |
| `year`, `quarter`, `month`, `month_name` | Calendar attributes |
| `week_of_year`, `day_of_week`, `day_name` | Week attributes |
| `is_weekend` | Boolean |

### pipeline_runs

One row per pipeline execution.

| Column | Description |
|---|---|
| `batch_id` | UUID (unique per run) |
| `started_at` / `completed_at` | UTC timestamps |
| `status` | `SUCCESS` or `FAILED` |
| `tickers` | Comma-separated list processed |
| `rows_extracted` / `rows_inserted` / `rows_skipped` | Row counts |
| `quality_checks_passed` | Boolean |
| `error_message` | Null on success; exception message on failure |

---

## Setup (Local Python)

```powershell
cd stock-market-etl-pipeline
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` to configure tickers, lookback window, and log level.

---

## Setup (Docker)

```bash
cp .env.example .env
docker compose up --build
```

`data/`, `logs/`, and `reports/` are mounted as volumes — outputs persist
on your local machine after the container exits.

---

## Run the Pipeline

```powershell
# Local
python -m src.main

# Docker
docker compose up
```

Example output:

```
── ETL Pipeline V2 ─────────────────────────────────────────
  Status          : SUCCESS
  Batch ID        : 3f2a1b9c-...
  Tickers         : AAPL, MSFT, NVDA, GOOGL, AMZN
  Rows extracted  : 630
  Rows inserted   : 630
  Rows skipped    : 0
  Quality passed  : True
  Elapsed         : 18.4s
  Database        : data\stock_market.db
────────────────────────────────────────────────────────────
```

On subsequent runs, only dates after the last stored `price_date` per
ticker are downloaded. `Rows inserted` will be 0 or a small number.

---

## Demo

<!--
  Add a short screen recording or a few screenshots here showing:
    1. `python -m src.main` running end to end (console SUCCESS summary)
    2. A query against the warehouse showing loaded rows
       (e.g. SELECT * FROM fact_stock_prices ORDER BY price_date DESC LIMIT 10)
    3. The streaming stack: `docker compose -f streaming/docker-compose.streaming.yml up`
       followed by rows appearing in fact_intraday_bars
    4. Power BI dashboard connected to the warehouse (see docs/powerbi.md)

  A GIF or short MP4 embedded below is the highest-impact addition here —
  it lets a reader verify the pipeline works without cloning and running it.
-->

| Batch pipeline | Streaming pipeline |
|---|---|
| _screenshot/GIF here_ | _screenshot/GIF here_ |

---

## Cloud Deployment (Azure)

### Local vs. cloud configuration

| Setting | Local (default) | Cloud |
|---|---|---|
| `BRONZE_TARGET` | `local` → `data/bronze/` Parquet | `azure` → Blob Storage container |
| `DATABASE_URL` | `sqlite:///data/stock_market.db` | `mssql+pyodbc://...database.windows.net...` |
| Scheduling | manual / Task Scheduler / cron | Azure Functions timer (weekdays 22:30 UTC) |
| Secrets | `.env` file (gitignored) | Function App settings + GitHub secrets |

The same code runs in both modes — only configuration changes.

### Provisioning

```powershell
az login
.\scripts\provision_azure.ps1
```

The script creates every resource with inline cost annotations: resource
group, storage account + `bronze` container, Azure SQL serverless free-tier
database, Function App (Consumption plan), and the GitHub Actions service
principal. Run it section by section the first time.

For Azure SQL you also need the
[Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
installed locally (`winget install Microsoft.msodbcsql18`).

### Where each secret lives

| Secret | Location | Used by |
|---|---|---|
| SQL connection string | `.env` (local) / Function App settings (cloud) | Pipeline warehouse writes |
| Storage connection string | `.env` (local) / Function App settings (cloud) | Bronze uploads |
| Service principal JSON | GitHub repo secret `AZURE_CREDENTIALS` | Deploy workflow only |

No secret is ever committed. `.env`, `local.settings.json`, and
`function_app/src/` are gitignored.

### Monthly cost

| Resource | Tier | Cost at this workload |
|---|---|---|
| Azure SQL Database | Serverless free tier (100K vCore-s/mo, auto-pause) | $0 |
| Blob Storage | Standard LRS, < 100 MB | ~ $0.01 |
| Azure Functions | Consumption plan (1M free executions/mo) | $0 |
| Application Insights | Basic (first 5 GB/mo free) | $0 |
| **Total** | | **≈ $0–1/month** |

### Deploying

Merging to `main` triggers `deploy.yml`: tests run first, then the Function
App is packaged (pipeline `src/` copied alongside the trigger) and published.
Manual deployment: `func azure functionapp publish <app-name>` from
`function_app/` after copying `src/` in.

---

## How Incremental Loading Works

On every run, the pipeline queries `MAX(price_date)` from `fact_stock_prices`
for each ticker. If a stored date is found, yfinance is called with
`start = last_date - 2 days` (a small overlap buffer catches late corrections).
If no stored data exists yet, the pipeline falls back to `LOOKBACK_DAYS`
(default 730, configurable in `.env`).

The duplicate-prevention logic in `load.py` then filters any overlap rows
before inserting, so reruns are always safe.

---

## Data Quality Reports

After every run a JSON report is written to `reports/quality_report_{batch_id}.json`:

```json
{
  "batch_id": "...",
  "timestamp": "2026-06-28T14:00:00+00:00",
  "rows_extracted": 630,
  "rows_inserted": 630,
  "rows_skipped": 0,
  "checks": {
    "null_tickers":            { "passed": true, "failures": 0 },
    "null_dates":              { "passed": true, "failures": 0 },
    "negative_prices":         { "passed": true, "failures": 0 },
    "negative_volume":         { "passed": true, "failures": 0 },
    "duplicate_rows":          { "passed": true, "failures": 0 },
    "price_relationships":     { "passed": true, "failures": 0 },
    "invalid_ticker_format":   { "passed": true, "failures": 0 },
    "date_gaps":               { "passed": true, "details": []  },
    "column_types":            { "passed": true, "failures": 0 }
  },
  "overall_passed": true,
  "execution_time_seconds": 18.4
}
```

Critical checks (`null_tickers`, `null_dates`, `negative_prices`) abort the
pipeline. All other failed checks are logged as warnings and recorded in the
report without stopping the run.

---

## Migrating from Version 1

If you have an existing V1 database, run the migration script once:

```powershell
python scripts/migrate_v1_to_v2.py
```

This adds `company_id`, `date_id`, and `batch_id` columns to
`fact_stock_prices`, creates the new dimension and audit tables, and
back-fills keys for all existing rows. It is safe to run more than once.

---

## Tests

```powershell
pytest
```

| Test file | What it covers |
|---|---|
| `test_transform.py` | Daily return calculation, deduplication |
| `test_quality_checks.py` | Critical vs. non-critical check behaviour |
| `test_quality_report.py` | All 9 checks, JSON report writing |
| `test_incremental.py` | Last-date queries, overlap dedup, idempotent reruns |
| `test_audit.py` | SUCCESS and FAILED audit rows in pipeline_runs |
| `test_dim_date.py` | dim_date seeding, weekday/quarter correctness |
| `test_storage.py` | Bronze partitioning, local Parquet round-trip, mocked Azure uploads |
| `test_streaming_common.py` | Tick schema/validation, replay pacing, alert threshold rules |

All tests run without cloud credentials or a message broker — Azure SDK calls
are mocked and streaming logic is factored into pure functions.

---

## Roadmap

See [CHANGELOG.md](CHANGELOG.md) for the full version history (V1 → V4).

**Version 3 — Cloud Migration ✅ (current)**
- ✅ Azure Blob Storage Bronze landing zone (Parquet, partitioned)
- ✅ Azure SQL Database serverless warehouse
- ✅ Azure Functions timer orchestration
- ✅ Power BI connection guide with DAX measures
- ✅ CI/CD with GitHub Actions

**Version 3.5 — candidates**
- Azure Data Factory orchestration as an alternative to Functions
- Microsoft Fabric Warehouse migration
- Azure Key Vault for secret management
- Infrastructure as code (Bicep)

**Version 4 — Streaming ✅ (current)**
- ✅ Kafka (KRaft) in Docker with replay-based tick simulation
- ✅ Spark Structured Streaming: 1-min OHLCV bars, watermarks, checkpointing, DLQ
- ✅ Price-move alerting + consumer-lag health monitoring
- ✅ Azure Event Hubs documented as Kafka-compatible cloud opt-in

**Future**
- Live market data feed (WebSocket) into the existing producer
- Databricks / Microsoft Fabric Real-Time Intelligence
- Azure Key Vault, infrastructure as code (Bicep)

---
