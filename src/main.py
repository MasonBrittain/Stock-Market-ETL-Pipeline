"""Entry points for the stock market ETL pipeline (Version 3).

run_pipeline() is the importable core — called by both the CLI (main())
and the Azure Functions timer trigger. It never calls sys.exit; it returns
a result dict and records SUCCESS/FAILED in the pipeline_runs audit table.
"""

from __future__ import annotations

import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def run_pipeline() -> dict[str, Any]:
    """Run the full ETL pipeline and return a result summary.

    Returns a dict with keys: status ("SUCCESS" | "FAILED"), batch_id,
    rows_extracted, rows_inserted, rows_skipped, failed_tickers,
    quality_passed, execution_time_seconds, and error (None on success).
    Never raises and never exits — callers decide how to surface failure.
    """
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
        initialize_schema(engine)

        start_dates = get_last_stored_dates(engine, TICKERS)
        if start_dates:
            logger.info(
                "Resuming from stored dates: %s",
                {t: str(d) for t, d in start_dates.items()},
            )
        else:
            logger.info("No stored data found — performing initial load")

        bronze_storage = create_bronze_storage(
            target=BRONZE_TARGET,
            local_dir=BRONZE_LOCAL_DIR,
            azure_container=AZURE_STORAGE_CONTAINER,
            azure_connection_string=AZURE_STORAGE_CONNECTION_STRING,
            azure_account_url=AZURE_STORAGE_ACCOUNT_URL,
        )
        logger.info("Bronze target: %s", BRONZE_TARGET)

        raw_data, failed_tickers = extract_stock_data(
            tickers=TICKERS,
            start_dates=start_dates,
            lookback_days=LOOKBACK_DAYS,
            interval=INTERVAL,
            bronze_storage=bronze_storage,
            batch_id=batch_id,
        )

        clean_data = transform_stock_data(raw_data)
        rows_extracted = len(clean_data)

        quality_results = run_quality_checks(clean_data)

        succeeded_tickers = [t for t in TICKERS if t not in failed_tickers]
        company_info = fetch_company_info(succeeded_tickers)
        company_id_map = upsert_dim_company(engine, succeeded_tickers, company_info)

        rows_inserted, rows_skipped = load_stock_data(
            stock_data=clean_data,
            engine=engine,
            company_id_map=company_id_map,
            batch_id=batch_id,
        )

        execution_time = time.monotonic() - pipeline_start

        write_quality_report(
            quality_results=quality_results,
            batch_id=batch_id,
            reports_dir=REPORTS_DIR,
            rows_extracted=rows_extracted,
            rows_inserted=rows_inserted,
            rows_skipped=rows_skipped,
            execution_time_seconds=execution_time,
        )

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

        return {
            "status": "SUCCESS",
            "batch_id": batch_id,
            "rows_extracted": rows_extracted,
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
            "failed_tickers": failed_tickers,
            "quality_passed": quality_results.get("overall_passed", False),
            "execution_time_seconds": round(execution_time, 1),
            "error": None,
        }

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

        return {
            "status": "FAILED",
            "batch_id": batch_id,
            "rows_extracted": rows_extracted,
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
            "failed_tickers": failed_tickers,
            "quality_passed": quality_results.get("overall_passed"),
            "execution_time_seconds": round(execution_time, 1),
            "error": str(exc),
        }

    finally:
        engine.dispose()


def main() -> None:
    """CLI entry point: run the pipeline, print a summary, exit non-zero on failure."""
    result = run_pipeline()

    # ASCII-only console output: Windows consoles default to cp1252 when
    # stdout is piped, and non-ASCII box characters raise UnicodeEncodeError.
    divider = "-" * 60
    if result["status"] == "SUCCESS":
        print(f"\n{divider}")
        print("  ETL Pipeline V3 : SUCCESS")
        print(f"  Batch ID        : {result['batch_id']}")
        print(f"  Tickers         : {', '.join(TICKERS)}")
        if result["failed_tickers"]:
            print(f"  Failed tickers  : {', '.join(result['failed_tickers'])}")
        print(f"  Rows extracted  : {result['rows_extracted']}")
        print(f"  Rows inserted   : {result['rows_inserted']}")
        print(f"  Rows skipped    : {result['rows_skipped']}")
        print(f"  Quality passed  : {result['quality_passed']}")
        print(f"  Elapsed         : {result['execution_time_seconds']}s")
        print(f"  Database        : {get_database_location()}")
        print(f"{divider}\n")
    else:
        print(f"\n{divider}")
        print("  ETL Pipeline V3 : FAILED")
        print(f"  Batch ID : {result['batch_id']}")
        print(f"  Error    : {result['error']}")
        print(f"{divider}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
