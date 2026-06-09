"""Helpers for building transactions and polling alerts during e2e tests."""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from confluent_kafka import Consumer, Producer

TRANSACTIONS_TOPIC = "transactions"
ALERTS_TOPIC = "alerts"

WARSAW = {"lat": 52.2297, "lon": 21.0122}
TOKYO = {"lat": 35.6895, "lon": 139.6917}


def make_tx(card_id: str, **overrides) -> Dict:
    """Build a transaction matching the simulator schema (simulator/app.py)."""
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "card_id": card_id,
        "user_id": "user-e2e-000001",
        "location": dict(WARSAW),
        "amount": 50.0,
        "available_limit": 5000.0,
        "currency": "PLN",
        "is_anomaly": False,
        "anomaly_type": None,
    }
    transaction.update(overrides)
    return transaction


def send_tx(producer: Producer, transaction: Dict) -> None:
    producer.produce(
        topic=TRANSACTIONS_TOPIC,
        key=transaction["card_id"],
        value=json.dumps(transaction, ensure_ascii=False),
    )
    producer.poll(0)


def warmup(producer: Producer, card_id: str, n: int = 6, amount: float = 50.0,
           location: Optional[Dict] = None) -> None:
    """Send n normal transactions so the Flink detector builds up state.

    The detector requires count >= 5 before statistical / new-location
    detection kicks in (see AnomalyDetectorJob.processElement).
    """
    for _ in range(n):
        send_tx(
            producer,
            make_tx(
                card_id,
                amount=amount,
                location=dict(location) if location else dict(WARSAW),
            ),
        )
    producer.flush(10)


def poll_alerts(card_id: str, timeout_sec: int = 30,
                bootstrap_servers: str = "localhost:9092") -> List[Dict]:
    """Read alerts from the alerts topic, returning those for the given card.

    Uses a unique consumer group per call and reads from the earliest offset
    so the test is independent of previously committed offsets.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"e2e-alert-consumer-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([ALERTS_TOPIC])

    matched: List[Dict] = []
    deadline = time.time() + timeout_sec

    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                alert = json.loads(msg.value().decode("utf-8"))
            except Exception:  # noqa: BLE001 - ignore malformed messages
                continue
            if alert.get("card_id") == card_id:
                matched.append(alert)
    finally:
        consumer.close()

    return matched
