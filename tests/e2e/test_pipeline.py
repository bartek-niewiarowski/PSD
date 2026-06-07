"""End-to-end tests of the Kafka -> Flink -> Kafka anomaly detection pipeline.

Each test uses a unique card_id because the Flink detector keys its state by
card_id and state survives between tests within a session.
"""

import uuid

import pytest

from helpers import TOKYO, WARSAW, make_tx, poll_alerts, send_tx, warmup


def _unique_card() -> str:
    return f"card-e2e-{uuid.uuid4().hex[:12]}"


@pytest.mark.timeout(120)
def test_limit_exceeded_detected(kafka_producer):
    card_id = _unique_card()

    warmup(kafka_producer, card_id, n=6, amount=50.0)

    send_tx(
        kafka_producer,
        make_tx(card_id, amount=9999.0, available_limit=100.0),
    )
    kafka_producer.flush(10)

    alerts = poll_alerts(card_id, timeout_sec=60)

    types = {alert.get("anomaly_type") for alert in alerts}
    assert "LIMIT_EXCEEDED" in types, f"Expected LIMIT_EXCEEDED, got alerts: {alerts}"

    limit_alert = next(a for a in alerts if a.get("anomaly_type") == "LIMIT_EXCEEDED")
    assert limit_alert.get("card_id") == card_id
    assert "transaction" in limit_alert


@pytest.mark.timeout(120)
def test_statistical_amount_anomaly_detected(kafka_producer):
    card_id = _unique_card()

    # Small variance around 50 so stddev > 0 and the z-score branch is active.
    for amount in (48.0, 50.0, 52.0, 49.0, 51.0, 50.0):
        send_tx(kafka_producer, make_tx(card_id, amount=amount, available_limit=1_000_000.0))
    kafka_producer.flush(10)

    # A huge amount (still below the limit) yields a very large z-score.
    send_tx(
        kafka_producer,
        make_tx(card_id, amount=5000.0, available_limit=1_000_000.0),
    )
    kafka_producer.flush(10)

    alerts = poll_alerts(card_id, timeout_sec=60)

    types = {alert.get("anomaly_type") for alert in alerts}
    assert "STATISTICAL_AMOUNT_ANOMALY" in types, (
        f"Expected STATISTICAL_AMOUNT_ANOMALY, got alerts: {alerts}"
    )


@pytest.mark.timeout(120)
def test_new_location_detected(kafka_producer):
    card_id = _unique_card()

    # Build up history from a single, stable location (Warsaw).
    warmup(kafka_producer, card_id, n=6, amount=50.0, location=WARSAW)

    # A transaction from a previously unseen location (Tokyo), normal amount.
    send_tx(
        kafka_producer,
        make_tx(card_id, amount=50.0, available_limit=1_000_000.0, location=dict(TOKYO)),
    )
    kafka_producer.flush(10)

    alerts = poll_alerts(card_id, timeout_sec=60)

    types = {alert.get("anomaly_type") for alert in alerts}
    assert "NEW_LOCATION" in types, f"Expected NEW_LOCATION, got alerts: {alerts}"
