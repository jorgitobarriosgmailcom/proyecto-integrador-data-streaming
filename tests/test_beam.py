from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.userstate import SetStateSpec
from apache_beam.utils.timestamp import Timestamp

from app.pipeline import DeduplicateDoFn, PaymentStatsFn


def test_duplicate_is_removed_per_key_and_window():
    event_ts = Timestamp.from_rfc3339("2026-09-30T12:00:05Z")
    events = [
        beam.window.TimestampedValue(
            ("m-1", {"event_id": "same", "payload": {"amount": 10, "fraud_score": 0.9}}),
            event_ts,
        ),
        beam.window.TimestampedValue(
            ("m-1", {"event_id": "same", "payload": {"amount": 10, "fraud_score": 0.9}}),
            event_ts,
        ),
        beam.window.TimestampedValue(
            ("m-2", {"event_id": "same", "payload": {"amount": 20, "fraud_score": 0.1}}),
            event_ts,
        ),
    ]

    with BeamTestPipeline() as p:
        out = (
            p
            | beam.Create(events)
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(DeduplicateDoFn(120))
            | beam.CombinePerKey(PaymentStatsFn())
        )
        assert_that(
            out,
            equal_to(
                [
                    (
                        "m-1",
                        {
                            "confirmed_amount": 10,
                            "confirmed_count": 1,
                            "high_risk_count": 1,
                        },
                    ),
                    (
                        "m-2",
                        {
                            "confirmed_amount": 20,
                            "confirmed_count": 1,
                            "high_risk_count": 0,
                        },
                    ),
                ]
            ),
        )


def test_teststream_out_of_order_stays_in_event_time_window():
    start = Timestamp.from_rfc3339("2026-09-30T12:00:00Z")
    later = Timestamp.from_rfc3339("2026-09-30T12:01:10Z")
    old = Timestamp.from_rfc3339("2026-09-30T12:00:25Z")
    stream = (
        BeamTestStream()
        .advance_watermark_to(start)
        .add_elements([beam.window.TimestampedValue(("m-1", 100), later)])
        .add_elements([beam.window.TimestampedValue(("m-1", 50), old)])
        .advance_watermark_to_infinity()
    )
    opts = PipelineOptions(streaming=True)
    with BeamTestPipeline(options=opts) as p:
        out = (
            p
            | stream
            | beam.WindowInto(beam.window.FixedWindows(60), allowed_lateness=120)
            | beam.CombinePerKey(sum)
        )
        assert_that(out, equal_to([("m-1", 50), ("m-1", 100)]))


def test_state_spec_exists_for_dedup():
    assert isinstance(DeduplicateDoFn.SEEN, SetStateSpec)


def test_state_spec_exists_for_dedup():
    assert isinstance(DeduplicateDoFn.SEEN, SetStateSpec)
