"""Spark Structured Streaming job: ticks → 1-minute OHLCV bars → warehouse.

Two streaming queries run from the same Kafka source:

1. DLQ query   — malformed messages are forwarded raw to stock-ticks-dlq
2. Bars query  — valid ticks are aggregated into 1-minute event-time windows
                 (30-second watermark) and written to fact_intraday_bars via
                 foreachBatch, which also computes the 5-minute rolling
                 average close and evaluates price-move alerts.

Checkpointing to /checkpoints makes restarts resume without data loss;
inserts are deduplicated against the warehouse for idempotency.

Environment:
    KAFKA_BOOTSTRAP       broker address       (default kafka:9092)
    DATABASE_URL          warehouse            (default sqlite:////data/stock_market.db)
    ALERT_THRESHOLD_PCT   alert trigger        (default 2.0)
    CHECKPOINT_DIR        checkpoint root      (default /checkpoints)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.ticks import DLQ_TOPIC, TICK_TOPIC, price_move_pct, should_alert

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | processor | %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/stock_market.db")
ALERT_THRESHOLD_PCT = float(os.getenv("ALERT_THRESHOLD_PCT", "2.0"))
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")

TICK_SCHEMA = StructType(
    [
        StructField("ticker", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", LongType()),
        StructField("event_time", StringType()),
        StructField("source", StringType()),
        StructField("producer_id", StringType()),
    ]
)


def build_session() -> SparkSession:
    return (
        SparkSession.builder.appName("stock-etl-intraday-bars")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_ticks(spark: SparkSession) -> DataFrame:
    """Raw Kafka stream with the JSON payload parsed alongside the raw value."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TICK_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("raw", F.col("value").cast("string"))
        .withColumn("tick", F.from_json(F.col("raw"), TICK_SCHEMA))
    )


def is_valid() -> F.Column:
    """Validity predicate mirroring streaming.common.ticks.validate_tick."""
    t = F.col("tick")
    return (
        t.isNotNull()
        & t["ticker"].isNotNull()
        & (F.length(F.trim(t["ticker"])) > 0)
        & t["price"].isNotNull()
        & (t["price"] > 0)
        & t["volume"].isNotNull()
        & (t["volume"] >= 0)
        & t["event_time"].isNotNull()
        & F.to_timestamp(t["event_time"]).isNotNull()
        & t["source"].isin("live", "replay")
    )


def aggregate_bars(parsed: DataFrame) -> DataFrame:
    """Valid ticks → 1-minute OHLCV bars per ticker with a 30s watermark."""
    ticks = (
        parsed.filter(is_valid())
        .select(
            F.col("tick.ticker").alias("ticker"),
            F.col("tick.price").alias("price"),
            F.col("tick.volume").alias("volume"),
            F.to_timestamp(F.col("tick.event_time")).alias("event_time"),
        )
        .withWatermark("event_time", "30 seconds")
    )
    return ticks.groupBy(
        F.window("event_time", "1 minute").alias("w"),
        F.col("ticker"),
    ).agg(
        F.expr("min_by(price, event_time)").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.expr("max_by(price, event_time)").alias("close"),
        F.sum("volume").alias("volume"),
        F.count("*").alias("tick_count"),
    ).select(
        "ticker",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "tick_count",
    )


def write_bars_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: dedupe, enrich with rolling average, insert, alert."""
    bars = batch_df.toPandas()
    if bars.empty:
        return

    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URL)
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with engine.begin() as conn:
            inserted = 0
            alerts = 0
            for row in bars.itertuples(index=False):
                exists = conn.execute(
                    text(
                        "SELECT 1 FROM fact_intraday_bars "
                        "WHERE ticker = :t AND window_start = :ws"
                    ),
                    {"t": row.ticker, "ws": row.window_start},
                ).fetchone()
                if exists:
                    continue

                ref = conn.execute(
                    text(
                        "SELECT AVG(close) FROM fact_intraday_bars "
                        "WHERE ticker = :t AND window_start >= :cutoff"
                    ),
                    {
                        "t": row.ticker,
                        "cutoff": row.window_start - timedelta(minutes=5),
                    },
                ).scalar()
                avg_5min = (
                    (ref * 1.0 + row.close) / 2 if ref is not None else row.close
                )

                conn.execute(
                    text(
                        "INSERT INTO fact_intraday_bars (ticker, window_start, "
                        "window_end, open, high, low, close, volume, tick_count, "
                        "avg_close_5min, processed_at) VALUES (:t, :ws, :we, :o, "
                        ":h, :l, :c, :v, :n, :a, :p)"
                    ),
                    {
                        "t": row.ticker,
                        "ws": row.window_start,
                        "we": row.window_end,
                        "o": row.open,
                        "h": row.high,
                        "l": row.low,
                        "c": row.close,
                        "v": int(row.volume),
                        "n": int(row.tick_count),
                        "a": avg_5min,
                        "p": now,
                    },
                )
                inserted += 1

                if ref is not None and should_alert(row.close, ref, ALERT_THRESHOLD_PCT):
                    move = price_move_pct(row.close, ref)
                    conn.execute(
                        text(
                            "INSERT INTO alerts (ticker, window_start, close, "
                            "reference_avg, move_pct, threshold_pct, created_at) "
                            "VALUES (:t, :ws, :c, :r, :m, :th, :ca)"
                        ),
                        {
                            "t": row.ticker,
                            "ws": row.window_start,
                            "c": row.close,
                            "r": ref,
                            "m": move,
                            "th": ALERT_THRESHOLD_PCT,
                            "ca": now,
                        },
                    )
                    alerts += 1
                    logger.warning(
                        "ALERT %s moved %.2f%% vs 5-min avg in window %s",
                        row.ticker,
                        move,
                        row.window_start,
                    )

        logger.info(
            "batch=%d | bars_in=%d | inserted=%d | skipped=%d | alerts=%d",
            batch_id,
            len(bars),
            inserted,
            len(bars) - inserted,
            alerts,
        )
    finally:
        engine.dispose()


def ensure_tables() -> None:
    """Create warehouse tables if missing (idempotent, shared definitions)."""
    from sqlalchemy import create_engine

    from src.database import metadata

    engine = create_engine(DATABASE_URL)
    metadata.create_all(
        engine,
        tables=[
            metadata.tables["fact_intraday_bars"],
            metadata.tables["alerts"],
        ],
    )
    engine.dispose()
    logger.info("Warehouse tables verified")


def main() -> None:
    ensure_tables()
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    parsed = read_ticks(spark)

    dlq_query = (
        parsed.filter(~is_valid())
        .select(F.col("raw").alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .start()
    )
    logger.info("DLQ query started → %s", DLQ_TOPIC)

    bars_query = (
        aggregate_bars(parsed)
        .writeStream.outputMode("append")
        .foreachBatch(write_bars_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bars")
        .trigger(processingTime="30 seconds")
        .start()
    )
    logger.info("Bars query started → fact_intraday_bars (30s trigger)")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
