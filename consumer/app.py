import json
import signal
import sys
from datetime import datetime
from typing import Optional

from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"
KAFKA_GROUP_ID = "transaction-test-consumer"

SAVE_TO_FILE = True
OUTPUT_FILE = "received_transactions.jsonl"

running = True


def stop_handler(sig, frame):
    global running
    print("\nStopping consumer...")
    running = False


def parse_message(value: bytes) -> Optional[dict]:
    try:
        return json.loads(value.decode("utf-8"))
    except Exception as e:
        print(f"[ERROR] Cannot parse message: {e}")
        return None


def print_transaction(transaction: dict):
    anomaly = transaction.get("is_anomaly", False)
    anomaly_type = transaction.get("anomaly_type")

    prefix = "[ANOMALY]" if anomaly else "[NORMAL]"

    print(
        f"{prefix} "
        f"id={transaction.get('transaction_id')} | "
        f"card={transaction.get('card_id')} | "
        f"user={transaction.get('user_id')} | "
        f"amount={transaction.get('amount')} {transaction.get('currency')} | "
        f"limit={transaction.get('available_limit')} | "
        f"location={transaction.get('location')} | "
        f"type={anomaly_type}"
    )


def save_transaction(transaction: dict):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as file:
        file.write(json.dumps(transaction, ensure_ascii=False) + "\n")


def main():
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([KAFKA_TOPIC])

    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Consumer group: {KAFKA_GROUP_ID}")

    if SAVE_TO_FILE:
        print(f"Saving messages to: {OUTPUT_FILE}")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            transaction = parse_message(msg.value())

            if transaction is None:
                continue

            print_transaction(transaction)

            if SAVE_TO_FILE:
                save_transaction(transaction)

    except KafkaException as e:
        print(f"[KAFKA ERROR] {e}")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()