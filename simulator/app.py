import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List

from confluent_kafka import Producer
from faker import Faker


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

CARD_COUNT = 10_000
USER_COUNT = 6_000

TRANSACTIONS_PER_SECOND = 20
ANOMALY_PROBABILITY = 0.03

fake = Faker("pl_PL")


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Kafka delivery failed: {err}")
    else:
        print(f"[OK] sent to {msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def generate_users() -> List[str]:
    return [f"user-{i:06d}" for i in range(1, USER_COUNT + 1)]


def generate_cards(users: List[str]) -> List[Dict]:
    cards = []

    for i in range(1, CARD_COUNT + 1):
        user_id = random.choice(users)

        # Bazowa lokalizacja użytkownika — okolice dużych miast w Polsce
        base_locations = [
            (52.2297, 21.0122),  # Warszawa
            (50.0647, 19.9450),  # Kraków
            (51.1079, 17.0385),  # Wrocław
            (54.3520, 18.6466),  # Gdańsk
            (52.4064, 16.9252),  # Poznań
            (50.2649, 19.0238),  # Katowice
        ]

        lat, lon = random.choice(base_locations)

        cards.append({
            "card_id": f"card-{i:06d}",
            "user_id": user_id,
            "base_lat": lat,
            "base_lon": lon,
            "available_limit": round(random.uniform(1000, 20000), 2),
            "typical_amount": round(random.uniform(20, 300), 2),
        })

    return cards


def normal_location(card: Dict) -> Dict:
    return {
        "lat": round(card["base_lat"] + random.uniform(-0.05, 0.05), 6),
        "lon": round(card["base_lon"] + random.uniform(-0.05, 0.05), 6),
    }


def distant_location() -> Dict:
    # Losowa lokalizacja daleko od typowych lokalizacji w Polsce
    suspicious_locations = [
        (40.7128, -74.0060),   # Nowy Jork
        (35.6895, 139.6917),   # Tokio
        (25.2048, 55.2708),    # Dubaj
        (-23.5505, -46.6333),  # Sao Paulo
        (51.5072, -0.1276),    # Londyn
    ]

    lat, lon = random.choice(suspicious_locations)

    return {
        "lat": round(lat + random.uniform(-0.03, 0.03), 6),
        "lon": round(lon + random.uniform(-0.03, 0.03), 6),
    }


def generate_normal_transaction(card: Dict) -> Dict:
    amount = max(1, random.gauss(card["typical_amount"], card["typical_amount"] * 0.3))
    amount = round(amount, 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "location": normal_location(card),
        "amount": amount,
        "available_limit": card["available_limit"],
        "currency": "PLN",
        "is_anomaly": False,
        "anomaly_type": None,
    }


def generate_anomaly_transaction(card: Dict) -> Dict:
    anomaly_type = random.choice([
        "HIGH_AMOUNT",
        "DISTANT_LOCATION",
        "LIMIT_EXCEEDED",
        "FREQUENT_TRANSACTION",
    ])

    transaction = generate_normal_transaction(card)
    transaction["is_anomaly"] = True
    transaction["anomaly_type"] = anomaly_type

    if anomaly_type == "HIGH_AMOUNT":
        transaction["amount"] = round(card["typical_amount"] * random.uniform(8, 20), 2)

    elif anomaly_type == "DISTANT_LOCATION":
        transaction["location"] = distant_location()

    elif anomaly_type == "LIMIT_EXCEEDED":
        transaction["amount"] = round(card["available_limit"] * random.uniform(1.05, 1.5), 2)

    elif anomaly_type == "FREQUENT_TRANSACTION":
        transaction["amount"] = round(random.uniform(5, 80), 2)

    return transaction


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "transaction-simulator",
    })

    users = generate_users()
    cards = generate_cards(users)

    print(f"Generated {len(users)} users")
    print(f"Generated {len(cards)} cards")
    print(f"Sending transactions to Kafka topic: {KAFKA_TOPIC}")

    delay = 1 / TRANSACTIONS_PER_SECOND

    while True:
        card = random.choice(cards)

        if random.random() < ANOMALY_PROBABILITY:
            transaction = generate_anomaly_transaction(card)
        else:
            transaction = generate_normal_transaction(card)

        producer.produce(
            topic=KAFKA_TOPIC,
            key=transaction["card_id"],
            value=json.dumps(transaction, ensure_ascii=False),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(delay)


if __name__ == "__main__":
    main()