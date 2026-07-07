"""Load transformed stock price data into the fact_stock_prices table.

V2 change: accepts a shared Engine (created once in main.py) and enriches each
row with company_id, date_id, and batch_id before inserting.  Duplicate rows
(same ticker + price_date already stored) are silently skipped.
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine

from src.database import fact_stock_prices_table

logger = logging.getLogger(__name__)


def _get_existing_keys(
    engine: Engine,
    tickers: list[str],
) -> set[tuple[str, pd.Timestamp]]:
    """Read (ticker, price_date) pairs already stored for the given tickers."""
    if not tickers:
        return set()

    query = select(
        fact_stock_prices_table.c.ticker,
        fact_stock_prices_table.c.price_date,
    ).where(fact_stock_prices_table.c.ticker.in_(tickers))

    existing = pd.read_sql(query, engine)
    if existing.empty:
        return set()

    existing["price_date"] = pd.to_datetime(existing["price_date"]).dt.normalize()
    return set(existing.itertuples(index=False, name=None))


def load_stock_data(
    stock_data: pd.DataFrame,
    engine: Engine,
    company_id_map: dict[str, int],
    batch_id: str,
) -> tuple[int, int]:
    """Append only new ticker/date records to fact_stock_prices.

    Args:
        stock_data: Transformed DataFrame from transform_stock_data().
        engine: Shared SQLAlchemy engine (schema must already be initialised).
        company_id_map: Maps ticker symbol → dim_company.company_id.
        batch_id: UUID for this pipeline run, written to every inserted row.

    Returns:
        (rows_inserted, rows_skipped) counts.
    """
    if stock_data.empty:
        logger.info("No rows to load — DataFrame is empty")
        return 0, 0

    rows = stock_data.copy()

    # Enrich with dimension foreign keys
    rows["company_id"] = rows["ticker"].map(company_id_map)
    rows["date_id"] = pd.to_datetime(rows["price_date"]).dt.strftime("%Y%m%d").astype(int)
    rows["batch_id"] = batch_id

    rows["price_date"] = pd.to_datetime(rows["price_date"]).dt.normalize()
    rows = rows.drop_duplicates(subset=["ticker", "price_date"], keep="last")

    existing = _get_existing_keys(
        engine,
        rows["ticker"].dropna().unique().tolist(),
    )

    is_new = [
        (row.ticker, row.price_date) not in existing
        for row in rows[["ticker", "price_date"]].itertuples(index=False)
    ]
    new_rows = rows.loc[is_new]
    skipped = len(rows) - len(new_rows)

    if new_rows.empty:
        logger.info("All %d rows already exist in the database — nothing to insert", skipped)
        return 0, skipped

    # Plain executemany in chunks. method="multi" builds one giant statement
    # that exceeds pyodbc's 2100-parameter limit on Azure SQL; executemany
    # (with fast_executemany enabled on mssql engines) is fast and portable.
    new_rows.to_sql(
        "fact_stock_prices",
        engine,
        if_exists="append",
        index=False,
        chunksize=500,
    )
    logger.info("Inserted %d rows | skipped %d existing rows", len(new_rows), skipped)
    return len(new_rows), skipped
