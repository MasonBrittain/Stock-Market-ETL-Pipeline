"""Database schema management and utility queries for the pipeline.

This module is the single source of truth for all table definitions.
load.py and main.py import tables and helper functions from here.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    select,
)
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

metadata = MetaData()

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

pipeline_runs_table = Table(
    "pipeline_runs",
    metadata,
    Column("run_id", Integer, primary_key=True, autoincrement=True),
    Column("batch_id", String(36), unique=True, nullable=False),
    Column("started_at", DateTime, nullable=False),
    Column("completed_at", DateTime),
    Column("status", String(10), nullable=False),
    Column("tickers", String(500)),
    Column("rows_extracted", Integer),
    Column("rows_inserted", Integer),
    Column("rows_skipped", Integer),
    Column("quality_checks_passed", Boolean),
    Column("error_message", Text),
)

dim_company_table = Table(
    "dim_company",
    metadata,
    Column("company_id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(10), unique=True, nullable=False),
    Column("company_name", String(255)),
    Column("sector", String(100)),
    Column("industry", String(100)),
    Column("created_at", DateTime, nullable=False),
    Column("updated_at", DateTime, nullable=False),
)

dim_date_table = Table(
    "dim_date",
    metadata,
    Column("date_id", Integer, primary_key=True),  # YYYYMMDD integer
    Column("full_date", DateTime, unique=True, nullable=False),
    Column("year", Integer),
    Column("quarter", Integer),
    Column("month", Integer),
    Column("month_name", String(20)),
    Column("week_of_year", Integer),
    Column("day_of_week", Integer),  # 0 = Monday
    Column("day_name", String(20)),
    Column("is_weekend", Boolean),
)

fact_stock_prices_table = Table(
    "fact_stock_prices",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("price_date", DateTime, nullable=False),
    Column("open_price", Float),
    Column("high_price", Float),
    Column("low_price", Float),
    Column("close_price", Float),
    Column("adj_close_price", Float),
    Column("volume", BigInteger),
    Column("daily_return", Float),
    Column("loaded_at", DateTime, nullable=False),
    Column("company_id", Integer),
    Column("date_id", Integer),
    Column("batch_id", String(36)),
    UniqueConstraint("ticker", "price_date", name="uq_stock_ticker_price_date"),
)

# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------


def create_db_engine(database_url: str) -> Engine:
    """Create and return a SQLAlchemy engine.

    For Azure SQL (mssql+pyodbc), fast_executemany batches inserts at the
    driver level — without it, executemany sends one round-trip per row.
    """
    if database_url.startswith("mssql"):
        return create_engine(database_url, fast_executemany=True)
    return create_engine(database_url)


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------


def initialize_schema(engine: Engine) -> None:
    """Create all tables that do not yet exist, then seed dim_date."""
    metadata.create_all(engine)
    logger.info("Database schema initialised")
    _seed_dim_date(engine)


def _seed_dim_date(engine: Engine) -> None:
    """Populate dim_date for 2000-01-01 through 2035-12-31 when the table is empty."""
    with engine.connect() as conn:
        existing_count = conn.execute(
            select(func.count()).select_from(dim_date_table)
        ).scalar_one()

    if existing_count > 0:
        return

    logger.info("Seeding dim_date (2000-01-01 → 2035-12-31)…")
    start = date(2000, 1, 1)
    end = date(2035, 12, 31)
    rows: list[dict[str, Any]] = []
    current = start
    while current <= end:
        ts = pd.Timestamp(current)
        rows.append(
            {
                "date_id": int(current.strftime("%Y%m%d")),
                "full_date": datetime(current.year, current.month, current.day),
                "year": current.year,
                "quarter": int(ts.quarter),
                "month": current.month,
                "month_name": ts.strftime("%B"),
                "week_of_year": int(ts.isocalendar().week),
                "day_of_week": current.weekday(),
                "day_name": ts.strftime("%A"),
                "is_weekend": current.weekday() >= 5,
            }
        )
        current += timedelta(days=1)

    pd.DataFrame(rows).to_sql("dim_date", engine, if_exists="append", index=False)
    logger.info("dim_date seeded with %d rows", len(rows))


# ---------------------------------------------------------------------------
# Incremental loading helpers
# ---------------------------------------------------------------------------


def get_last_stored_dates(engine: Engine, tickers: list[str]) -> dict[str, date]:
    """Return the most recent price_date stored per ticker.

    Only tickers that already have rows in the database are included.
    Tickers with no stored data are absent from the result, triggering a
    full LOOKBACK_DAYS download in extract.py.
    """
    if not tickers:
        return {}

    query = (
        select(
            fact_stock_prices_table.c.ticker,
            func.max(fact_stock_prices_table.c.price_date).label("last_date"),
        )
        .where(fact_stock_prices_table.c.ticker.in_(tickers))
        .group_by(fact_stock_prices_table.c.ticker)
    )

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    return {
        row[0]: pd.Timestamp(row[1]).date()
        for row in rows
        if row[1] is not None
    }


# ---------------------------------------------------------------------------
# Dimension helpers
# ---------------------------------------------------------------------------


def upsert_dim_company(
    engine: Engine,
    tickers: list[str],
    company_info: dict[str, dict[str, str | None]],
) -> dict[str, int]:
    """Ensure each ticker exists in dim_company and return a ticker → company_id map.

    company_info maps ticker → {company_name, sector, industry} (values may be None).
    Rows that already exist are not overwritten on the core identity fields,
    but company_name/sector/industry are updated if they were previously null.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    with engine.begin() as conn:
        for ticker in tickers:
            info = company_info.get(ticker, {})
            existing_row = conn.execute(
                select(
                    dim_company_table.c.company_id,
                    dim_company_table.c.company_name,
                ).where(dim_company_table.c.ticker == ticker)
            ).fetchone()

            if existing_row is None:
                conn.execute(
                    dim_company_table.insert().values(
                        ticker=ticker,
                        company_name=info.get("company_name"),
                        sector=info.get("sector"),
                        industry=info.get("industry"),
                        created_at=now,
                        updated_at=now,
                    )
                )
            elif existing_row[1] is None and info.get("company_name"):
                conn.execute(
                    dim_company_table.update()
                    .where(dim_company_table.c.ticker == ticker)
                    .values(
                        company_name=info.get("company_name"),
                        sector=info.get("sector"),
                        industry=info.get("industry"),
                        updated_at=now,
                    )
                )

        id_rows = conn.execute(
            select(
                dim_company_table.c.ticker,
                dim_company_table.c.company_id,
            ).where(dim_company_table.c.ticker.in_(tickers))
        ).fetchall()

    return {row[0]: row[1] for row in id_rows}


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------


def write_audit_row(engine: Engine, audit: dict[str, Any]) -> None:
    """Insert one row into pipeline_runs."""
    with engine.begin() as conn:
        conn.execute(pipeline_runs_table.insert().values(**audit))
    logger.info(
        "Audit row written — batch_id=%s status=%s",
        audit["batch_id"],
        audit["status"],
    )
