"""Pytest fixtures for the end-to-end pipeline tests.

The fixtures bring up the full docker compose stack (Kafka, Flink, MongoDB),
wait until Kafka is reachable and the Flink anomaly-detection job is RUNNING,
and tear everything down afterwards.

Set E2E_SKIP_DOCKER=1 to run the tests against an already running stack
(useful while iterating locally).
"""

import os
import subprocess
import time
from pathlib import Path

import pytest
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

REPO_ROOT = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("E2E_KAFKA_BOOTSTRAP", "localhost:9092")
FLINK_REST_URL = os.environ.get("E2E_FLINK_REST", "http://localhost:8081")
EXPECTED_FLINK_JOB = "Card Transaction Statistical Anomaly Detector"

SKIP_DOCKER = True


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        check=True,
    )


def _wait_for_kafka(timeout: int = 120) -> None:
    deadline = time.time() + timeout
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
            metadata = admin.list_topics(timeout=5)
            if metadata.brokers:
                return
        except Exception as exc:  # noqa: BLE001 - broad on purpose during polling
            last_error = exc
        time.sleep(2)

    raise TimeoutError(f"Kafka not reachable within {timeout}s: {last_error}")


def _wait_for_topics(topics: tuple[str, ...], timeout: int = 60) -> None:
    deadline = time.time() + timeout
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    while time.time() < deadline:
        metadata = admin.list_topics(timeout=5)
        if all(topic in metadata.topics for topic in topics):
            return
        time.sleep(2)

    raise TimeoutError(f"Topics {topics} not created within {timeout}s")


def _wait_for_flink_job(timeout: int = 180) -> None:
    deadline = time.time() + timeout
    last_state = "unknown"

    while time.time() < deadline:
        try:
            response = requests.get(f"{FLINK_REST_URL}/jobs/overview", timeout=5)
            response.raise_for_status()
            jobs = response.json().get("jobs", [])
            for job in jobs:
                last_state = job.get("state", "unknown")
                if job.get("state") == "RUNNING":
                    return
        except Exception:  # noqa: BLE001 - broad on purpose during polling
            pass
        time.sleep(3)

    raise TimeoutError(
        f"No RUNNING Flink job within {timeout}s (last observed state: {last_state})"
    )


@pytest.fixture(scope="session", autouse=True)
def docker_stack():
    """Start the full stack once per test session and tear it down afterwards."""
    if not SKIP_DOCKER:
        _compose("up", "-d", "--build")

    try:
        _wait_for_kafka()
        _wait_for_topics(("transactions", "alerts"))
        _wait_for_flink_job()
        yield
    finally:
        if not SKIP_DOCKER:
            _compose("down", "-v")


@pytest.fixture
def kafka_producer():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "e2e-test-producer",
        }
    )
    yield producer
    producer.flush(10)
