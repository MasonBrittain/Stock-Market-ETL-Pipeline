# Stock Market ETL Pipeline

[![CI](https://github.com/MasonBrittain/stock-market-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/MasonBrittain/stock-market-etl-pipeline/actions/workflows/ci.yml)

A production-style batch ETL pipeline that downloads historical stock prices
from Yahoo Finance, validates data quality, and loads results into a star-schema
SQLite database with full run auditing.

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
  SQLite Database
  ├── dim_company          (who)
  ├── dim_date             (when — pre-seeded 2000–2035)
  ├── fact_stock_prices    (measures + FKs + batch_id)
  └── pipeline_runs        (audit log per execution)
          │
          ▼
  reports/quality_report_{batch_id}.json
  logs/pipeline.log
```

> **Future:** Azure Blob Storage (Bronze) → Azure SQL / Fabric (Silver/Gold)
> → Power BI connected to Azure → CI/CD with GitHub Actions

---

## Project Structure

```
stock-market-etl-pipeline/
├── src/
│   ├── config.py           Configuration (tickers, DB URL, log settings)
│   ├── logger.py           Rotating file + console logging with batch_id
│   ├── database.py         Schema definitions, dim seeding, utility queries
│   ├── extract.py          Per-ticker incremental download from Yahoo Finance
│   ├── transform.py        Clean, coerce, and calculate daily returns
│   ├── quality_checks.py   9 validation checks + JSON report writer
│   ├── load.py             Incremental insert into fact_stock_prices
│   └── main.py             Pipeline orchestrator with audit and error handling
├── tests/
│   ├── conftest.py         Shared fixtures (in-memory DB engine)
│   ├── test_transform.py
│   ├── test_quality_checks.py
│   ├── test_quality_report.py
│   ├── test_incremental.py
│   ├── test_audit.py
│   └── test_dim_date.py
├── scripts/
│   └── migrate_v1_to_v2.py   One-time migration for existing V1 databases
├── data/                   SQLite database (created on first run)
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

---

---

## Future Roadmap

**Version 3 — Cloud Migration**
- Azure Blob Storage as Bronze landing zone
- Azure SQL Database or Microsoft Fabric Warehouse for Silver/Gold
- Azure Data Factory or Azure Functions for orchestration
- Power BI connected to Azure SQL
- CI/CD pipeline with GitHub Actions

**Version 4 — Streaming**
- Azure Event Hubs (Kafka-compatible) for real-time tick ingestion
- Spark Structured Streaming or Databricks for near-real-time processing
- Monitoring and alerting with Azure Monitor

---
