from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

REQUIRED_TOP_LEVEL = {"event_id", "key", "event_time", "event_type", "schema_version", "payload"}
REQUIRED_PAYLOAD = {"payment_id", "merchant_id", "amount", "currency", "status", "fraud_score"}


def parse_utc(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("event_time vacío o inválido")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"event_time ISO-8601 inválido: {value!r}") from exc
    if dt.utcoffset() is None:
        raise ValueError("event_time debe incluir zona horaria")
    return dt.astimezone(UTC)


def validate_event(event: dict[str, Any]) -> tuple[bool, str]:
    missing = sorted(REQUIRED_TOP_LEVEL - set(event))
    if missing:
        return False, f"missing_top_level:{','.join(missing)}"
    if event.get("schema_version") != "1.0":
        return False, "unsupported_schema_version"
    if event.get("event_type") != "payment_status":
        return False, "unsupported_event_type"
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return False, "payload_not_object"
    missing_payload = sorted(REQUIRED_PAYLOAD - set(payload))
    if missing_payload:
        return False, f"missing_payload:{','.join(missing_payload)}"
    try:
        parse_utc(str(event["event_time"]))
        amount = int(payload["amount"])
        score = float(payload["fraud_score"])
    except (ValueError, TypeError):
        return False, "invalid_types"
    if event["key"] != payload["merchant_id"]:
        return False, "key_merchant_mismatch"
    if amount < 0 or not 0 <= score <= 1:
        return False, "invalid_ranges"
    if payload["status"] not in {"CONFIRMED", "PENDING", "REJECTED"}:
        return False, "invalid_status"
    return True, "ok"


def idempotency_key(result: dict[str, Any]) -> str:
    return f"{result['merchant_id']}|{result['window_start']}"
