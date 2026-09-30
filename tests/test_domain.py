from datetime import UTC

import pytest

from app.domain import idempotency_key, parse_utc, validate_event


def sample_event():
    return {
        "event_id": "evt-1",
        "key": "merchant-301",
        "event_time": "2026-09-30T12:00:05Z",
        "event_type": "payment_status",
        "schema_version": "1.0",
        "payload": {
            "payment_id": "pay-1",
            "merchant_id": "merchant-301",
            "amount": 1000,
            "currency": "PYG",
            "status": "CONFIRMED",
            "fraud_score": 0.91,
        },
    }


def test_parse_utc_timezone_aware():
    dt = parse_utc("2026-09-30T12:00:05Z")
    assert dt.tzinfo == UTC


def test_contract_valid_and_versioned():
    ok, reason = validate_event(sample_event())
    assert ok is True and reason == "ok"


def test_contract_rejects_key_mismatch():
    event = sample_event()
    event["key"] = "other"
    assert validate_event(event)[1] == "key_merchant_mismatch"


def test_idempotency_key_stable():
    row = {"merchant_id": "m-1", "window_start": "2026-09-30T12:00:00Z"}
    assert idempotency_key(row) == "m-1|2026-09-30T12:00:00Z"


def test_invalid_timestamp_rejected():
    with pytest.raises(ValueError):
        parse_utc("not-a-date")
