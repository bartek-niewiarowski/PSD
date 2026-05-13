import json
import time
from datetime import datetime

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "alerts"
KAFKA_GROUP_ID = "alert-dashboard-consumer"


st.set_page_config(
    page_title="Anomaly Dashboard",
    page_icon="🚨",
    layout="wide",
)


def create_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def read_alerts(max_messages=100):
    consumer = create_consumer()
    alerts = []

    try:
        start_time = time.time()

        while len(alerts) < max_messages and time.time() - start_time < 5:
            msg = consumer.poll(timeout=0.5)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                alert = json.loads(msg.value().decode("utf-8"))
                alerts.append(alert)
            except Exception as e:
                st.warning(f"Nie udało się sparsować wiadomości: {e}")

    finally:
        consumer.close()

    return alerts


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
    max_messages = st.sidebar.slider("Liczba odczytywanych alarmów", 10, 500, 100)
    auto_refresh = st.sidebar.checkbox("Automatyczne odświeżanie", value=True)
    refresh_seconds = st.sidebar.slider("Odświeżanie co ile sekund", 2, 30, 5)

    if st.sidebar.button("Odśwież teraz"):
        st.rerun()

    alerts = read_alerts(max_messages=max_messages)

    if not alerts:
        st.info("Brak alarmów w topiku Kafka `alerts`.")
        if auto_refresh:
            time.sleep(refresh_seconds)
            st.rerun()
        return

    rows = [normalize_alert(alert) for alert in alerts]
    df = pd.DataFrame(rows)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Liczba alarmów", len(df))

    with col2:
        st.metric("Liczba kart z alarmami", df["card_id"].nunique())

    with col3:
        st.metric("Liczba użytkowników z alarmami", df["user_id"].nunique())

    st.subheader("Typy anomalii")

    if "anomaly_type" in df.columns:
        anomaly_counts = df["anomaly_type"].value_counts()
        st.bar_chart(anomaly_counts)

    st.subheader("Ostatnie alarmy")

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
    ]

    existing_columns = [col for col in display_columns if col in df.columns]
    st.dataframe(
        df[existing_columns].sort_values(by="created_at", ascending=False),
        use_container_width=True,
    )

    st.subheader("Mapa lokalizacji alarmów")

    map_df = df.dropna(subset=["lat", "lon"])

    if not map_df.empty:
        st.map(map_df[["lat", "lon"]])
    else:
        st.info("Brak danych GPS do pokazania na mapie.")

    st.subheader("Surowe dane alarmów")

    with st.expander("Pokaż JSON"):
        st.json(alerts)

    st.caption(f"Ostatnie odświeżenie: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()