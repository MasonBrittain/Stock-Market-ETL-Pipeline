"""Streaming health monitor: consumer lag, bar throughput, and alert counts.

Run from the host while the streaming stack is up:

    python streaming/monitor.py

Exits non-zero when consumer lag exceeds --max-lag (default 1000 messages),
making it suitable for scheduled health checks (cron / Task Scheduler).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def get_consumer_lag(bootstrap: str, topic: str) -> int | None:
    """Total messages behind across partitions for the Spark consumer group.

    Returns None when the group has not committed offsets yet (job warming up).
    """
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        # kafka-python 3.x renamed the admin group APIs; support both.
        if hasattr(admin, "list_groups"):
            groups = [g[0] for g in admin.list_groups()]
            fetch_offsets = admin.list_group_offsets
        else:
            groups = [g[0] for g in admin.list_consumer_groups()]
            fetch_offsets = admin.list_consumer_group_offsets

        spark_groups = [g for g in groups if g.startswith("spark-kafka-source")]
        if not spark_groups:
            return None

        consumer = KafkaConsumer(bootstrap_servers=bootstrap)
        try:
            partitions = [
                TopicPartition(topic, p)
                for p in consumer.partitions_for_topic(topic) or []
            ]
            if not partitions:
                return None
            end_offsets = consumer.end_offsets(partitions)

            total_lag = 0
            for group in spark_groups:
                committed = fetch_offsets(group)
                for tp, meta in committed.items():
                    if tp.topic == topic and meta.offset >= 0:
                        total_lag += max(0, end_offsets.get(tp, 0) - meta.offset)
            return total_lag
        finally:
            consumer.close()
    finally:
        admin.close()


def get_warehouse_stats(database_url: str) -> dict:
    """Bar and alert counts plus recent throughput from the warehouse."""
    from sqlalchemy import create_engine, text

    engine = create_engine(database_url)
    try:
        with engine.connect() as conn:
            bars = conn.execute(
                text("SELECT COUNT(*) FROM fact_intraday_bars")
            ).scalar()
            cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5)
            recent = conn.execute(
                text("SELECT COUNT(*) FROM fact_intraday_bars WHERE processed_at >= :c"),
                {"c": cutoff},
            ).scalar()
            alerts = conn.execute(text("SELECT COUNT(*) FROM alerts")).scalar()
            latest = conn.execute(
                text("SELECT MAX(processed_at) FROM fact_intraday_bars")
            ).scalar()
        return {
            "total_bars": bars,
            "bars_last_5min": recent,
            "total_alerts": alerts,
            "latest_processed_at": latest,
        }
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming pipeline health check")
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--database-url", default="sqlite:///data/stock_market.db")
    parser.add_argument("--max-lag", type=int, default=1000)
    args = parser.parse_args()

    print("--- Streaming Health ------------------------------------")

    try:
        lag = get_consumer_lag(args.bootstrap, "stock-ticks")
        lag_display = "n/a (group not committed yet)" if lag is None else str(lag)
        print(f"  Consumer lag (stock-ticks) : {lag_display}")
    except Exception as exc:
        print(f"  Consumer lag               : UNAVAILABLE ({exc})")
        lag = None

    try:
        stats = get_warehouse_stats(args.database_url)
        print(f"  Bars total                 : {stats['total_bars']}")
        print(f"  Bars last 5 min            : {stats['bars_last_5min']}")
        print(f"  Alerts total               : {stats['total_alerts']}")
        print(f"  Latest processed_at (UTC)  : {stats['latest_processed_at']}")
    except Exception as exc:
        print(f"  Warehouse stats            : UNAVAILABLE ({exc})")

    print("----------------------------------------------------------")

    if lag is not None and lag > args.max_lag:
        print(f"UNHEALTHY: lag {lag} exceeds threshold {args.max_lag}")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
