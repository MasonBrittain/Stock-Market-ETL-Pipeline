"""Azure Functions entry point: scheduled execution of the stock ETL pipeline.

Runs weekdays at 22:30 UTC — after the US market close (16:00 ET) with
buffer for Yahoo Finance to finalise daily bars. Configuration comes from
Function App settings (the cloud equivalent of .env).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import azure.functions as func

# The pipeline package is deployed alongside this file (see deploy workflow);
# make the repo root importable so `from src...` resolves.
sys.path.insert(0, str(Path(__file__).resolve().parent))

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 30 22 * * 1-5",  # sec min hour day month day-of-week (Mon–Fri)
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def stock_etl_timer(timer: func.TimerRequest) -> None:
    """Run the ETL pipeline on schedule; raise on failure so the run shows as failed."""
    if timer.past_due:
        logging.warning("Timer is past due — running catch-up execution")

    from src.main import run_pipeline

    result = run_pipeline()

    logging.info("Pipeline result: %s", result)
    if result["status"] != "SUCCESS":
        # Raising marks the invocation as failed in Application Insights,
        # which is what drives the Functions failure metrics and alerts.
        raise RuntimeError(f"Pipeline failed: {result['error']}")
