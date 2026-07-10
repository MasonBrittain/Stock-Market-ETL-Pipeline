"""Unit tests for the shared streaming tick logic (no broker required)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from streaming.common.ticks import (
    build_tick,
    price_move_pct,
    should_alert,
    tick_interval_seconds,
    validate_tick,
)


def _valid_tick() -> dict:
    return build_tick(
        ticker="aapl",
        price=150.25,
        volume=1000,
        producer_id="test-1",
        event_time=datetime(2026, 7, 7, 14, 30, tzinfo=timezone.utc),
    )


class TestBuildTick:
    def test_normalises_ticker_to_uppercase(self) -> None:
        assert _valid_tick()["ticker"] == "AAPL"

    def test_event_time_is_iso8601(self) -> None:
        tick = _valid_tick()
        parsed = datetime.fromisoformat(tick["event_time"])
        assert parsed.tzinfo is not None

    def test_defaults_to_replay_source(self) -> None:
        assert _valid_tick()["source"] == "replay"


class TestValidateTick:
    def test_valid_tick_passes(self) -> None:
        ok, reason = validate_tick(_valid_tick())
        assert ok, reason

    def test_missing_field_fails(self) -> None:
        tick = _valid_tick()
        del tick["price"]
        ok, reason = validate_tick(tick)
        assert not ok
        assert "missing" in reason

    def test_negative_price_fails(self) -> None:
        tick = _valid_tick()
        tick["price"] = -1.0
        ok, reason = validate_tick(tick)
        assert not ok
        assert "positive" in reason

    def test_non_numeric_price_fails(self) -> None:
        tick = _valid_tick()
        tick["price"] = "not-a-number"
        ok, _ = validate_tick(tick)
        assert not ok

    def test_bad_event_time_fails(self) -> None:
        tick = _valid_tick()
        tick["event_time"] = "yesterday"
        ok, reason = validate_tick(tick)
        assert not ok
        assert "ISO-8601" in reason

    def test_unknown_source_fails(self) -> None:
        tick = _valid_tick()
        tick["source"] = "manual"
        ok, _ = validate_tick(tick)
        assert not ok

    def test_blank_ticker_fails(self) -> None:
        tick = _valid_tick()
        tick["ticker"] = "   "
        ok, _ = validate_tick(tick)
        assert not ok


class TestPacing:
    def test_ten_ticks_per_second(self) -> None:
        assert tick_interval_seconds(10) == pytest.approx(0.1)

    def test_fractional_rate(self) -> None:
        assert tick_interval_seconds(0.5) == pytest.approx(2.0)

    def test_zero_rate_rejected(self) -> None:
        with pytest.raises(ValueError):
            tick_interval_seconds(0)


class TestAlerts:
    def test_move_pct(self) -> None:
        assert price_move_pct(103.0, 100.0) == pytest.approx(3.0)
        assert price_move_pct(97.0, 100.0) == pytest.approx(-3.0)

    def test_zero_reference_is_safe(self) -> None:
        assert price_move_pct(100.0, 0.0) == 0.0

    def test_alert_above_threshold(self) -> None:
        assert should_alert(close=103.0, reference=100.0, threshold_pct=2.0)

    def test_no_alert_below_threshold(self) -> None:
        assert not should_alert(close=101.0, reference=100.0, threshold_pct=2.0)

    def test_downward_move_alerts_too(self) -> None:
        assert should_alert(close=97.0, reference=100.0, threshold_pct=2.0)
