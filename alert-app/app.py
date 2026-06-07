import json
import os
import time
from datetime import datetime

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaException
from pymongo import DESCENDING, MongoClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "alerts")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "alert-dashboard-consumer")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "anomaly_detection")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "alerts")


st.set_page_config(
    page_title="Anomaly Dashboard",
    page_icon="🚨",
    layout="wide",
)

@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


def create_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([KAFKA_TOPIC])
    return consumer

def save_alert_to_mongo(alert: dict):
    collection = get_mongo_collection()

    if not alert.get("alarm_id"):
        return

    collection.update_one(
        {"alarm_id": alert.get("alarm_id")},
        {"$set": alert},
        upsert=True,
    )

def read_alerts_from_kafka(max_messages=100):
    consumer = create_consumer()
    saved_count = 0

    try:
        start_time = time.time()

        while saved_count < max_messages and time.time() - start_time < 5:
            msg = consumer.poll(timeout=0.5)

            if msg is None:
                continue

            if msg.error():
                st.warning(f"Błąd Kafki: {msg.error()}")
                continue

            try:
                alert = json.loads(msg.value().decode("utf-8"))
                save_alert_to_mongo(alert)
                saved_count += 1
            except Exception as e:
                st.warning(f"Nie udało się sparsować wiadomości: {e}")

    finally:
        consumer.close()

    return saved_count

def read_alerts_from_mongo(limit=500):
    collection = get_mongo_collection()

    documents = list(
        collection
        .find({}, {"_id": 0})
        .sort("created_at", DESCENDING)
        .limit(limit)
    )

    return documents


def normalize_alert(alert):
    transaction = alert.get("transaction", {})
    anomaly = alert.get("anomaly", {})

    return {
        "alarm_id": alert.get("alarm_id"),
        "created_at": alert.get("created_at"),
        "anomaly_type": alert.get("anomaly_type") or anomaly.get("type"),
        "reason": alert.get("reason") or anomaly.get("reason"),
        "card_id": alert.get("card_id"),
        "user_id": alert.get("user_id"),
        "transaction_id": alert.get("transaction_id"),
        "amount": transaction.get("amount"),
        "available_limit": transaction.get("available_limit"),
        "currency": transaction.get("currency", "PLN"),
        "lat": transaction.get("location", {}).get("lat"),
        "lon": transaction.get("location", {}).get("lon"),
        "source": alert.get("source"),
    }


def main():
    st.title("Dashboard anomalii transakcji kartowych")

    st.sidebar.header("Ustawienia")
    kafka_read_limit = st.sidebar.slider("Ile nowych alertów czytać z Kafki", 10, 500, 100)
    mongo_limit = st.sidebar.slider("Ile alarmów pokazać z MongoDB", 50, 2000, 500)
    auto_refresh = st.sidebar.checkbox("Automatyczne odświeżanie", value=True)
    refresh_seconds = st.sidebar.slider("Odświeżanie co ile sekund", 2, 30, 5)

    if st.sidebar.button("Odśwież teraz"):
        st.rerun()

    saved_count = read_alerts_from_kafka(max_messages=kafka_read_limit)
    alerts = read_alerts_from_mongo(limit=mongo_limit)

    if not alerts:
        st.info("Brak zapisanych alarmów w MongoDB.")
        if auto_refresh:
            time.sleep(refresh_seconds)
            st.rerun()
        return

    rows = [normalize_alert(alert) for alert in alerts]
    df = pd.DataFrame(rows)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Liczba alarmów", len(df))

    with col2:
        st.metric("Liczba kart z alarmami", df["card_id"].nunique())

    with col3:
        st.metric("Liczba użytkowników z alarmami", df["user_id"].nunique())
    
    with col4:
        st.metric("Typy anomalii", df["anomaly_type"].nunique())

    st.subheader("Typy anomalii")

    anomaly_counts = df["anomaly_type"].fillna("UNKNOWN").value_counts()
    st.bar_chart(anomaly_counts)

    st.subheader("Alarmy w czasie")

    time_df = df.copy()
    time_df["created_at"] = pd.to_datetime(
    time_df["created_at"],
    errors="coerce",
    utc=True
    )
    time_df = time_df.dropna(subset=["created_at"])

    if not time_df.empty:
        time_df["minute"] = time_df["created_at"].dt.floor("min")
        alarms_over_time = time_df.groupby("minute").size()
        st.line_chart(alarms_over_time)
    else:
        st.info("Brak poprawnych dat do wykresu czasowego.")

    st.subheader("Najczęściej alarmowane karty")

    top_cards = df["card_id"].value_counts().head(10)
    st.bar_chart(top_cards)

    st.subheader("Kwoty transakcji anormalnych")

    amount_df = df.dropna(subset=["amount"])

    if not amount_df.empty:
        st.bar_chart(amount_df.set_index("alarm_id")["amount"])
    else:
        st.info("Brak kwot do wizualizacji.")

    st.subheader("Mapa lokalizacji alarmów")

    map_df = df.dropna(subset=["lat", "lon"])

    if not map_df.empty:
        st.map(map_df[["lat", "lon"]])
    else:
        st.info("Brak danych GPS do pokazania na mapie.")

    st.subheader("Ostatnie alarmy z MongoDB")

    display_columns = [
        "created_at",
        "anomaly_type",
        "reason",
        "card_id",
        "user_id",
        "amount",
        "available_limit",
        "currency",
        "lat",
        "lon",
        "source",
    ]

    existing_columns = [col for col in display_columns if col in df.columns]

    st.dataframe(
        df[existing_columns].sort_values(by="created_at", ascending=False),
        use_container_width=True,
    )

    st.subheader("Surowe dane z MongoDB")

    with st.expander("Pokaż JSON"):
        st.json(alerts)

    st.caption(f"Ostatnie odświeżenie: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()