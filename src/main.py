"""Command-line entry point for the stock market ETL pipeline (Version 2)."""

from __future__ import annotations

import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Insert the project root so 'src' is importable when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import (
    AZURE_STORAGE_ACCOUNT_URL,
    AZURE_STORAGE_CONNECTION_STRING,
    AZURE_STORAGE_CONTAINER,
    BRONZE_LOCAL_DIR,
    BRONZE_TARGET,
    DATABASE_URL,
    INTERVAL,
    LOOKBACK_DAYS,
    LOG_DIR,
    LOG_LEVEL,
    REPORTS_DIR,
    TICKERS,
    get_database_location,
)
from src.database import (
    create_db_engine,
    get_last_stored_dates,
    initialize_schema,
    upsert_dim_company,
    write_audit_row,
)
from src.extract import extract_stock_data, fetch_company_info
from src.load import load_stock_data
from src.logger import configure_logging
from src.quality_checks import run_quality_checks, write_quality_report
from src.storage import create_bronze_storage
from src.transform import transform_stock_data


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-naive datetime for DB storage."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def main() -> None:
    """Orchestrate extraction, transformation, validation, and loading."""
    batch_id = str(uuid.uuid4())
    started_at = _utc_now()
    pipeline_start = time.monotonic()

    configure_logging(batch_id=batch_id, log_dir=LOG_DIR, log_level=LOG_LEVEL)
    logger = logging.getLogger(__name__)

    logger.info(
        "Pipeline starting — batch_id=%s | tickers=%s | interval=%s | lookback=%dd",
        batch_id,
        ", ".join(TICKERS),
        INTERVAL,
        LOOKBACK_DAYS,
    )

    engine = create_db_engine(DATABASE_URL)
    quality_results: dict = {}
    rows_extracted = 0
    rows_inserted = 0
    rows_skipped = 0
    failed_tickers: list[str] = []

    try:
        # ── Schema setup ──────────────────────────────────────────────────────
        initialize_schema(engine)

        # ── Incremental date windows ──────────────────────────────────────────
        start_dates = get_last_stored_dates(engine, TICKERS)
        if start_dates:
            logger.info(
                "Resuming from stored dates: %s",
                {t: str(d) for t, d in start_dates.items()},
            )
        else:
            logger.info("No stored data found — performing initial load")

        # ── Bronze storage backend ────────────────────────────────────────────
        bronze_storage = create_bronze_storage(
            target=BRONZE_TARGET,
            local_dir=BRONZE_LOCAL_DIR,
            azure_container=AZURE_STORAGE_CONTAINER,
            azure_connection_string=AZURE_STORAGE_CONNECTION_STRING,
            azure_account_url=AZURE_STORAGE_ACCOUNT_URL,
        )
        logger.info("Bronze target: %s", BRONZE_TARGET)

        # ── Extract (raw data lands in Bronze per ticker) ─────────────────────
        raw_data, failed_tickers = extract_stock_data(
            tickers=TICKERS,
            start_dates=start_dates,
            lookback_days=LOOKBACK_DAYS,
            interval=INTERVAL,
            bronze_storage=bronze_storage,
            batch_id=batch_id,
        )

        # ── Transform ─────────────────────────────────────────────────────────
        clean_data = transform_stock_data(raw_data)
        rows_extracted = len(clean_data)

        # ── Quality checks ────────────────────────────────────────────────────
        quality_results = run_quality_checks(clean_data)

        # ── Dimension population ──────────────────────────────────────────────
        succeeded_tickers = [t for t in TICKERS if t not in failed_tickers]
        company_info = fetch_company_info(succeeded_tickers)
        company_id_map = upsert_dim_company(engine, succeeded_tickers, company_info)

        # ── Load ──────────────────────────────────────────────────────────────
        rows_inserted, rows_skipped = load_stock_data(
            stock_data=clean_data,
            engine=engine,
            company_id_map=company_id_map,
            batch_id=batch_id,
        )

        execution_time = time.monotonic() - pipeline_start

        # ── Quality report ────────────────────────────────────────────────────
        write_quality_report(
            quality_results=quality_results,
            batch_id=batch_id,
            reports_dir=REPORTS_DIR,
            rows_extracted=rows_extracted,
            rows_inserted=rows_inserted,
            rows_skipped=rows_skipped,
            execution_time_seconds=execution_time,
        )

        # ── Audit row ─────────────────────────────────────────────────────────
        write_audit_row(
            engine,
            {
                "batch_id": batch_id,
                "started_at": started_at,
                "completed_at": _utc_now(),
                "status": "SUCCESS",
                "tickers": ", ".join(TICKERS),
                "rows_extracted": rows_extracted,
                "rows_inserted": rows_inserted,
                "rows_skipped": rows_skipped,
                "quality_checks_passed": quality_results.get("overall_passed", False),
                "error_message": None,
            },
        )

        logger.info(
            "Pipeline complete — extracted=%d | inserted=%d | skipped=%d | "
            "failed_tickers=%s | elapsed=%.1fs | db=%s",
            rows_extracted,
            rows_inserted,
            rows_skipped,
            failed_tickers or "none",
            execution_time,
            get_database_location(),
        )

        print("\n── ETL Pipeline V2 ─────────────────────────────────────────")
        print(f"  Status          : SUCCESS")
        print(f"  Batch ID        : {batch_id}")
        print(f"  Tickers         : {', '.join(TICKERS)}")
        if failed_tickers:
            print(f"  Failed tickers  : {', '.join(failed_tickers)}")
        print(f"  Rows extracted  : {rows_extracted}")
        print(f"  Rows inserted   : {rows_inserted}")
        print(f"  Rows skipped    : {rows_skipped}")
        print(f"  Quality passed  : {quality_results.get('overall_passed', False)}")
        print(f"  Elapsed         : {execution_time:.1f}s")
        print(f"  Database        : {get_database_location()}")
        print("────────────────────────────────────────────────────────────\n")

    except Exception as exc:
        execution_time = time.monotonic() - pipeline_start
        logger.exception("Pipeline FAILED after %.1fs", execution_time)

        try:
            write_audit_row(
                engine,
                {
                    "batch_id": batch_id,
                    "started_at": started_at,
                    "completed_at": _utc_now(),
                    "status": "FAILED",
                    "tickers": ", ".join(TICKERS),
                    "rows_extracted": rows_extracted,
                    "rows_inserted": rows_inserted,
                    "rows_skipped": rows_skipped,
                    "quality_checks_passed": quality_results.get("overall_passed"),
                    "error_message": str(exc),
                },
            )
        except Exception:
            logger.exception("Could not write failure audit row")

        print("\n── ETL Pipeline V2 — FAILED ─────────────────────────────────")
        print(f"  Batch ID : {batch_id}")
        print(f"  Error    : {exc}")
        print("─────────────────────────────────────────────────────────────\n")

        sys.exit(1)

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
