from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer

from domain import idempotency_key, parse_utc, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("integrador")
INVALID = "invalid"


class ParseValidateDoFn(beam.DoFn):
    def process(self, element: tuple[bytes, bytes]):
        raw_key, raw_value = element
        try:
            event = json.loads(raw_value.decode("utf-8"))
        except Exception as exc:
            yield beam.pvalue.TaggedOutput(INVALID, {"reason": "invalid_json", "error": str(exc)})
            return
        ok, reason = validate_event(event)
        if not ok:
            yield beam.pvalue.TaggedOutput(INVALID, {"reason": reason, "event": event})
            return
        if raw_key and raw_key.decode("utf-8") != event["key"]:
            yield beam.pvalue.TaggedOutput(INVALID, {"reason": "kafka_key_mismatch", "event": event})
            return
        yield beam.window.TimestampedValue(event, parse_utc(event["event_time"]).timestamp())


class DeduplicateDoFn(beam.DoFn):
    SEEN = SetStateSpec("seen", StrUtf8Coder())
    EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

    def __init__(self, allowed_lateness: int = 120):
        self.allowed_lateness = allowed_lateness

    def process(
        self,
        element: tuple[str, dict[str, Any]],
        seen=beam.DoFn.StateParam(SEEN),
        window=beam.DoFn.WindowParam,
        expiry=beam.DoFn.TimerParam(EXPIRY),
    ):
        merchant_id, event = element
        event_id = str(event["event_id"])
        current = set(seen.read())
        if event_id in current:
            LOG.info("DUPLICATE_DROPPED merchant=%s event_id=%s", merchant_id, event_id)
            return
        seen.add(event_id)
        expiry.set(window.end + self.allowed_lateness)
        yield merchant_id, event

    @on_timer(EXPIRY)
    def expire(self, seen=beam.DoFn.StateParam(SEEN)):
        seen.clear()


class PaymentStatsFn(beam.CombineFn):
    def create_accumulator(self):
        return 0, 0, 0

    def add_input(self, acc, event):
        amount_sum, count, high_risk = acc
        payload = event["payload"]
        return (
            amount_sum + int(payload["amount"]),
            count + 1,
            high_risk + int(float(payload["fraud_score"]) >= 0.80),
        )

    def merge_accumulators(self, accs):
        a = c = h = 0
        for amount_sum, count, high_risk in accs:
            a += amount_sum
            c += count
            h += high_risk
        return a, c, h

    def extract_output(self, acc):
        amount_sum, count, high_risk = acc
        return {"confirmed_amount": amount_sum, "confirmed_count": count, "high_risk_count": high_risk}


class AttachPaneMetadata(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        merchant_id, metrics = element
        timing = {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}.get(int(pane.timing), "UNKNOWN")
        result = {
            "merchant_id": merchant_id,
            "window_start": window.start.to_rfc3339(),
            "window_end": window.end.to_rfc3339(),
            **metrics,
            "pane_timing": timing,
            "pane_index": pane.index,
        }
        result["idempotency_key"] = idempotency_key(result)
        yield result


class SQLiteUpsertDoFn(beam.DoFn):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def setup(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_metrics (
              idempotency_key TEXT PRIMARY KEY,
              merchant_id TEXT NOT NULL,
              window_start TEXT NOT NULL,
              window_end TEXT NOT NULL,
              confirmed_amount INTEGER NOT NULL,
              confirmed_count INTEGER NOT NULL,
              high_risk_count INTEGER NOT NULL,
              pane_timing TEXT NOT NULL,
              pane_index INTEGER NOT NULL,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.commit()

    def process(self, result: dict[str, Any]):
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT INTO payment_metrics (
              idempotency_key, merchant_id, window_start, window_end,
              confirmed_amount, confirmed_count, high_risk_count, pane_timing, pane_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET
              window_end=excluded.window_end,
              confirmed_amount=excluded.confirmed_amount,
              confirmed_count=excluded.confirmed_count,
              high_risk_count=excluded.high_risk_count,
              pane_timing=excluded.pane_timing,
              pane_index=excluded.pane_index,
              updated_at=CURRENT_TIMESTAMP
            WHERE excluded.pane_index >= payment_metrics.pane_index
            """,
            (
                result["idempotency_key"], result["merchant_id"], result["window_start"], result["window_end"],
                result["confirmed_amount"], result["confirmed_count"], result["high_risk_count"],
                result["pane_timing"], result["pane_index"],
            ),
        )
        self.conn.commit()
        LOG.info("UPSERT_RESULT %s", json.dumps(result, ensure_ascii=False))
        yield result

    def teardown(self):
        if self.conn is not None:
            self.conn.close()


def log_invalid(row):
    LOG.warning("INVALID_EVENT %s", json.dumps(row, ensure_ascii=False))
    return row


def build_pipeline(pipeline: beam.Pipeline, bootstrap: str, topic: str, db_path: str):
    read = pipeline | "KafkaRead" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": bootstrap,
            "group.id": "fpuna-integrador-v1",
            "auto.offset.reset": "earliest",
        },
        topics=[topic],
        commit_offset_in_finalize=True,
    )

    parsed = read | "ParseValidate" >> beam.ParDo(ParseValidateDoFn()).with_outputs(INVALID, main="valid")
    _ = parsed.invalid | "LogInvalid" >> beam.Map(log_invalid)

    valid = (
        parsed.valid
        | "ConfirmedOnly" >> beam.Filter(lambda e: e["payload"]["status"] == "CONFIRMED")
        | "FixedMinuteWindow" >> beam.WindowInto(
            beam.window.FixedWindows(60),
            trigger=beam.trigger.AfterWatermark(
                early=beam.trigger.Repeatedly(beam.trigger.AfterProcessingTime(30)),
                late=beam.trigger.AfterCount(1),
            ),
            accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=120,
        )
        | "KeyByMerchant" >> beam.Map(lambda e: (e["key"], e))
        | "Deduplicate" >> beam.ParDo(DeduplicateDoFn(120))
        | "AggregateStats" >> beam.CombinePerKey(PaymentStatsFn())
        | "PaneMetadata" >> beam.ParDo(AttachPaneMetadata())
        | "IdempotentSQLiteSink" >> beam.ParDo(SQLiteUpsertDoFn(db_path))
    )
    return valid


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    topic = os.getenv("INPUT_TOPIC", "payments.raw")
    db_path = os.getenv("OUTPUT_DB", "/data/metrics.db")
    options = PipelineOptions(["--streaming", "--direct_num_workers=1", "--allow_unsafe_triggers"])
    options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=options) as pipeline:
        build_pipeline(pipeline, bootstrap, topic, db_path)


if __name__ == "__main__":
    main()
