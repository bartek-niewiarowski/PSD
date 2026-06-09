"""End-to-end test of alert persistence to MongoDB.

This mirrors what alert-app/app.py does: it consumes alerts produced by Flink
on the `alerts` topic and upserts them into MongoDB (keyed by `alarm_id`).
The test reproduces that persistence step and verifies the document round-trips.
"""

import os
import uuid

import pytest
from pymongo import MongoClient

from helpers import make_tx, poll_alerts, send_tx, warmup

MONGO_URI = os.environ.get("E2E_MONGO_URI", "mongodb://admin:admin123@localhost:27017")
MONGO_DB = "anomaly_detection"
MONGO_COLLECTION = "alerts"


def _save_alert_to_mongo(collection, alert: dict) -> None:
    """Upsert an alert by alarm_id, matching alert-app save_alert_to_mongo."""
    if not alert.get("alarm_id"):
        return
    collection.update_one(
        {"alarm_id": alert["alarm_id"]},
        {"$set": alert},
        upsert=True,
    )


@pytest.fixture
def mongo_collection():
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]
    yield collection
    client.close()


@pytest.mark.timeout(120)
def test_alert_persisted_to_mongo(kafka_producer, mongo_collection):
    card_id = f"card-e2e-{uuid.uuid4().hex[:12]}"

    warmup(kafka_producer, card_id, n=6, amount=50.0)
    send_tx(
        kafka_producer,
        make_tx(card_id, amount=9999.0, available_limit=100.0),
    )
    kafka_producer.flush(10)

    alerts = poll_alerts(card_id, timeout_sec=60)
    assert alerts, f"No alerts produced for card {card_id}"

    for alert in alerts:
        _save_alert_to_mongo(mongo_collection, alert)

    alarm_ids = [a["alarm_id"] for a in alerts if a.get("alarm_id")]
    assert alarm_ids, "Alerts have no alarm_id"

    stored = mongo_collection.find_one({"alarm_id": alarm_ids[0]}, {"_id": 0})
    assert stored is not None, "Alert was not persisted to MongoDB"
    assert stored.get("card_id") == card_id
    assert stored.get("anomaly_type") == "LIMIT_EXCEEDED"
