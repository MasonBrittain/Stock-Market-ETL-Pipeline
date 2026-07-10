"""Tick message schema, validation, pacing, and alert rules.

Pure functions only — no Kafka or Spark imports — so both the producer and
processor share one definition and the logic is unit-testable without brokers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

TICK_TOPIC = "stock-ticks"
DLQ_TOPIC = "stock-ticks-dlq"

REQUIRED_FIELDS = ("ticker", "price", "volume", "event_time", "source", "producer_id")
VALID_SOURCES = ("live", "replay")


def build_tick(
    ticker: str,
    price: float,
    volume: int,
    producer_id: str,
    source: str = "replay",
    event_time: datetime | None = None,
) -> dict[str, Any]:
    """Construct a tick message conforming to the stream schema."""
    ts = event_time or datetime.now(timezone.utc)
    return {
        "ticker": ticker.strip().upper(),
        "price": float(price),
        "volume": int(volume),
        "event_time": ts.isoformat(),
        "source": source,
        "producer_id": producer_id,
    }


def validate_tick(message: dict[str, Any]) -> tuple[bool, str]:
    """Check one parsed tick message; return (is_valid, reason)."""
    missing = [f for f in REQUIRED_FIELDS if f not in message]
    if missing:
        return False, f"missing fields: {', '.join(missing)}"

    if not isinstance(message["ticker"], str) or not message["ticker"].strip():
        return False, "ticker must be a non-empty string"

    try:
        price = float(message["price"])
    except (TypeError, ValueError):
        return False, "price is not numeric"
    if price <= 0:
        return False, "price must be positive"

    try:
        volume = int(message["volume"])
    except (TypeError, ValueError):
        return False, "volume is not numeric"
    if volume < 0:
        return False, "volume must be non-negative"

    try:
        datetime.fromisoformat(str(message["event_time"]))
    except ValueError:
        return False, "event_time is not ISO-8601"

    if message["source"] not in VALID_SOURCES:
        return False, f"source must be one of {VALID_SOURCES}"

    return True, "ok"


def tick_interval_seconds(replay_speed: float) -> float:
    """Seconds to sleep between replayed ticks for a target ticks/second rate."""
    if replay_speed <= 0:
        raise ValueError("REPLAY_SPEED must be positive")
    return 1.0 / replay_speed


def price_move_pct(close: float, reference: float) -> float:
    """Percentage move of close vs a reference price (e.g. 5-min average)."""
    if reference == 0:
        return 0.0
    return (close - reference) / reference * 100.0


def should_alert(close: float, reference: float, threshold_pct: float) -> bool:
    """True when the absolute move vs reference exceeds the alert threshold."""
    return abs(price_move_pct(close, reference)) > threshold_pct
