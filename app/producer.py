from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def event(
    event_id: str,
    merchant: str,
    when: datetime,
    amount: int,
    score: float,
    status: str = "CONFIRMED",
):
    return {
        "event_id": event_id,
        "key": merchant,
        "event_time": when.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "event_type": "payment_status",
        "schema_version": "1.0",
        "payload": {
            "payment_id": event_id.replace("evt", "pay"),
            "merchant_id": merchant,
            "amount": amount,
            "currency": "PYG",
            "status": status,
            "fraud_score": score,
        },
    }


def demo_events() -> list[dict]:
    base = floor_minute(datetime.now(UTC)) - timedelta(minutes=2)
    e1 = event(
        "evt-001",
        "merchant-301",
        base + timedelta(seconds=5),
        120_000,
        0.92,
    )
    e2 = event(
        "evt-002",
        "merchant-301",
        base + timedelta(seconds=42),
        80_000,
        0.41,
    )
    e3 = event(
        "evt-003",
        "merchant-777",
        base + timedelta(seconds=15),
        200_000,
        0.88,
    )
    e4 = event(
        "evt-004",
        "merchant-301",
        base + timedelta(minutes=1, seconds=10),
        150_000,
        0.73,
    )
    duplicate = dict(e2)
    out_of_order = event(
        "evt-005",
        "merchant-301",
        base + timedelta(seconds=25),
        50_000,
        0.81,
    )
    pending = event(
        "evt-006",
        "merchant-301",
        base + timedelta(minutes=1, seconds=20),
        90_000,
        0.55,
        "PENDING",
    )
    invalid = event(
        "evt-bad",
        "merchant-999",
        base + timedelta(seconds=30),
        10_000,
        0.2,
    )
    invalid.pop("schema_version")
    return [e1, e2, e3, e4, duplicate, out_of_order, pending, invalid]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("INPUT_TOPIC", "payments.raw"),
    )
    parser.add_argument("--scenario", default="demo", choices=["demo"])
    parser.add_argument("--delay", type=float, default=0.7)
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    events = demo_events()

    for idx, row in enumerate(events, start=1):
        payload = json.dumps(row, ensure_ascii=False).encode("utf-8")
        producer.produce(
            args.topic,
            key=row.get("key", "invalid").encode(),
            value=payload,
        )
        producer.flush()
        print(
            f"PRODUCED {idx}/{len(events)} "
            f"key={row.get('key')} "
            f"event_id={row.get('event_id')} "
            f"event_time={row.get('event_time')}"
        )
        time.sleep(args.delay)

    print("SCENARIO_COMPLETE")


if __name__ == "__main__":
    main()
