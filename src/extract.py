"""Extract historical daily stock prices from Yahoo Finance.

V2 change: each ticker is downloaded individually using a date window
derived from the last stored date in the database (see database.get_last_stored_dates).
Tickers that fail to download are logged and skipped rather than aborting the pipeline.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

import pandas as pd
import yfinance as yf

if TYPE_CHECKING:
    from src.storage import BronzeStorage

logger = logging.getLogger(__name__)


def fetch_company_info(tickers: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Fetch company metadata from yfinance for dim_company population.

    Returns a dict mapping ticker → {company_name, sector, industry}.
    Missing or rate-limited tickers return an empty inner dict gracefully.
    """
    info: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        try:
            raw_info = yf.Ticker(ticker).info
            info[ticker] = {
                "company_name": raw_info.get("longName"),
                "sector": raw_info.get("sector"),
                "industry": raw_info.get("industry"),
            }
            logger.debug("Fetched company info for %s", ticker)
        except Exception:
            logger.warning("Could not fetch company info for %s — will store nulls", ticker)
            info[ticker] = {}
    return info


def extract_stock_data(
    tickers: Sequence[str],
    start_dates: dict[str, date],
    lookback_days: int,
    interval: str = "1d",
    bronze_storage: "BronzeStorage | None" = None,
    batch_id: str = "",
) -> tuple[pd.DataFrame, list[str]]:
    """Download price history for each ticker from its last stored date forward.

    Args:
        tickers: Ticker symbols to download.
        start_dates: Maps ticker → last stored date from the database.
            Tickers absent from this dict are downloaded from scratch using lookback_days.
        lookback_days: Calendar days to look back for tickers with no stored data.
        interval: yfinance interval string (default "1d").
        bronze_storage: Optional Bronze-layer writer. When provided, each
            ticker's raw extract is landed as Parquet before transformation.
        batch_id: Pipeline run UUID, used in Bronze file names.

    Returns:
        A tuple of (combined DataFrame with all downloaded rows, list of tickers that failed).
        Partial failures are logged but do not raise — the pipeline continues with the
        tickers that succeeded.
    """
    normalized = [t.strip().upper() for t in tickers if t.strip()]
    if not normalized:
        raise ValueError("At least one ticker symbol is required.")

    today = date.today()
    frames: list[pd.DataFrame] = []
    failed: list[str] = []

    for ticker in normalized:
        last_stored = start_dates.get(ticker)
        if last_stored is not None:
            # 2-day overlap buffer catches any late-arriving corrections
            download_start = last_stored - timedelta(days=2)
        else:
            download_start = today - timedelta(days=lookback_days)

        logger.info(
            "Downloading %s | %s → %s", ticker, download_start.isoformat(), today.isoformat()
        )
        try:
            raw = yf.download(
                tickers=ticker,
                start=download_start.strftime("%Y-%m-%d"),
                end=today.strftime("%Y-%m-%d"),
                interval=interval,
                auto_adjust=False,
                progress=False,
            )
        except Exception:
            logger.exception("Download failed for %s — skipping ticker", ticker)
            failed.append(ticker)
            continue

        if raw is None or raw.empty:
            logger.warning("No data returned for %s — skipping ticker", ticker)
            failed.append(ticker)
            continue

        raw = raw.copy()
        # yfinance >= 0.2.x returns MultiIndex columns even for single-ticker
        # downloads, e.g. ("Open", "AAPL"). Flatten to field names only so that
        # adding the Ticker column doesn't corrupt the MultiIndex structure.
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        # Land the raw extract in the Bronze layer before any transformation.
        # A Bronze failure must not lose the already-downloaded data.
        if bronze_storage is not None:
            try:
                bronze_storage.write_bronze(raw, ticker, batch_id)
            except Exception:
                logger.exception("Bronze write failed for %s — continuing", ticker)

        raw["Ticker"] = ticker
        frames.append(raw)
        logger.info("Downloaded %d rows for %s", len(raw), ticker)

    if not frames:
        raise ValueError(
            "No data was extracted for any ticker. "
            "Check ticker symbols, date ranges, and internet connection."
        )

    combined = pd.concat(frames)
    logger.info(
        "Extraction complete — %d total rows | %d tickers succeeded | %d failed",
        len(combined),
        len(normalized) - len(failed),
        len(failed),
    )
    if failed:
        logger.warning("Failed tickers: %s", ", ".join(failed))

    return combined, failed
