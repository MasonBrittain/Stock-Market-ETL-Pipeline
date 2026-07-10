"""Replay producer: streams Bronze Parquet rows to Kafka as tick messages.

Replays the project's own historical extracts at a configurable rate to
simulate a live market feed — the same pattern used to load-test streaming
systems. Loops over the data continuously until stopped.

Environment:
    KAFKA_BOOTSTRAP   broker address        (default kafka:9092)
    BRONZE_DIR        parquet root          (default /data/bronze)
    REPLAY_SPEED      ticks per second      (default 10)
    PRODUCER_ID       identifier in msgs    (default replay-1)
"""

from __future__ import annotations

import glob
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.ticks import TICK_TOPIC, build_tick, tick_interval_seconds

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | producer | %(message)s",
)
logger = logging.getLogger(__name__)

_running = True


def _handle_shutdown(signum: int, frame: object) -> None:
    global _running
    logger.info("Shutdown signal received — finishing current tick")
    _running = False


def load_replay_rows(bronze_dir: str) -> pd.DataFrame:
    """Read all Bronze Parquet files into one frame sorted by date.

    Each file is one ticker's extract; the ticker is recovered from the
    partition path (ticker=XXXX). Returns columns: ticker, price, volume.
    """
    pattern = os.path.join(bronze_dir, "ticker=*", "year=*", "*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No Bronze Parquet found under {bronze_dir}. "
            "Run the batch pipeline first to land replay data."
        )

    frames = []
    for file in files:
        ticker = Path(file).parts[-3].removeprefix("ticker=")
        df = pd.read_parquet(file).reset_index()
        df["ticker"] = ticker
        frames.append(df[["ticker", "Close", "Volume", "Date"]])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["Close"]).sort_values("Date")
    combined = combined.rename(columns={"Close": "price", "Volume": "volume"})
    logger.info("Loaded %d replay rows from %d files", len(combined), len(files))
    return combined[["ticker", "price", "volume"]]


def create_producer(bootstrap: str) -> KafkaProducer:
    """Connect to Kafka with retry/backoff — the broker may still be starting."""
    delay = 2.0
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
            )
        except Exception:
            logger.warning("Broker not ready at %s — retrying in %.0fs", bootstrap, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


def run() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    bronze_dir = os.getenv("BRONZE_DIR", "/data/bronze")
    replay_speed = float(os.getenv("REPLAY_SPEED", "10"))
    producer_id = os.getenv("PRODUCER_ID", "replay-1")
    interval = tick_interval_seconds(replay_speed)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    rows = load_replay_rows(bronze_dir)
    producer = create_producer(bootstrap)
    logger.info(
        "Replaying %d rows to %s at %.1f ticks/sec (looping)",
        len(rows),
        TICK_TOPIC,
        replay_speed,
    )

    sent = 0
    while _running:
        for row in rows.itertuples(index=False):
            if not _running:
                break
            tick = build_tick(
                ticker=row.ticker,
                price=row.price,
                volume=row.volume,
                producer_id=producer_id,
                source="replay",
            )
            producer.send(TICK_TOPIC, key=tick["ticker"], value=tick)
            sent += 1
            if sent % 500 == 0:
                logger.info("Sent %d ticks", sent)
            time.sleep(interval)

    producer.flush()
    producer.close()
    logger.info("Producer stopped after %d ticks", sent)


if __name__ == "__main__":
    run()
