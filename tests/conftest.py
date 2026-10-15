import os
import socket
import uuid

import pytest

from bbo_tap.config import EXAMPLE

REDPANDA = "localhost:9092"
PUBSUB_EMULATOR = "localhost:8085"
PUBSUB_PROJECT = "bbo-tap-test"


def pytest_addoption(parser):
    parser.addoption("--live", action="store_true", help="run the 30 s live cycle against the real exchanges")
    parser.addoption("--live-config", default=str(EXAMPLE), help="config whose symbols the live cycle uses")
    parser.addoption("--live-duration", default="30", help="seconds the live cycle runs")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--live"):
        skip = pytest.mark.skip(reason="live exchanges: run with --live")
        for item in items:
            if "live" in item.keywords:
                item.add_marker(skip)


class FakeSink:
    """Collects what the connection would produce."""

    def __init__(self) -> None:
        self.records: list[tuple[str, int | None, str, int, int, str]] = []

    def send(self, exchange: str, partition: int | None, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None:
        self.records.append((exchange, partition, conn_id, seq, ts_recv_ns, payload))


@pytest.fixture
def fake_sink():
    return FakeSink()


@pytest.fixture(scope="session")
def redpanda():
    """Broker address, or skip when no Redpanda is listening (fail instead with REQUIRE_REDPANDA=1, as in CI)."""
    host, port = REDPANDA.split(":")
    try:
        socket.create_connection((host, int(port)), timeout=1).close()
    except OSError:
        if os.environ.get("REQUIRE_REDPANDA") == "1":
            pytest.fail(f"no Redpanda at {REDPANDA}, and REQUIRE_REDPANDA=1")
        pytest.skip(f"no Redpanda at {REDPANDA}")
    return REDPANDA


@pytest.fixture
def topics(redpanda):
    """Fresh raw/ref topic pair per test, deleted afterwards, so tests never touch raw.ws."""
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient({"bootstrap.servers": redpanda})
    suffix = uuid.uuid4().hex[:8]
    raw, ref = f"test.raw.{suffix}", f"test.ref.{suffix}"
    futures = admin.create_topics(
        [
            NewTopic(raw, num_partitions=5, replication_factor=1),
            NewTopic(ref, num_partitions=1, replication_factor=1, config={"cleanup.policy": "compact"}),
        ]
    )
    for f in futures.values():
        f.result(10)
    yield raw, ref
    for f in admin.delete_topics([raw, ref]).values():
        f.result(10)


@pytest.fixture(scope="session")
def pubsub_emulator():
    """Emulator address, or skip when none is listening (fail instead with REQUIRE_PUBSUB=1, as in CI)."""
    host, port = PUBSUB_EMULATOR.split(":")
    try:
        socket.create_connection((host, int(port)), timeout=1).close()
    except OSError:
        if os.environ.get("REQUIRE_PUBSUB") == "1":
            pytest.fail(f"no Pub/Sub emulator at {PUBSUB_EMULATOR}, and REQUIRE_PUBSUB=1")
        pytest.skip(f"no Pub/Sub emulator at {PUBSUB_EMULATOR}")
    return PUBSUB_EMULATOR


@pytest.fixture
def pubsub_topics(pubsub_emulator, monkeypatch):
    """Fresh raw/ref topic pair per test, each with a subscription (ordered on raw), deleted afterwards.

    Yields (raw topic, ref topic, raw subscription path, ref subscription path).
    """
    from google.cloud import pubsub_v1

    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", pubsub_emulator)
    publisher, subscriber = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
    suffix = uuid.uuid4().hex[:8]
    raw, ref = f"test-raw-{suffix}", f"test-ref-{suffix}"
    subs = []
    for topic, ordered in ((raw, True), (ref, False)):
        topic_path = publisher.topic_path(PUBSUB_PROJECT, topic)
        sub_path = subscriber.subscription_path(PUBSUB_PROJECT, topic)
        publisher.create_topic(name=topic_path)
        subscriber.create_subscription(
            request={"name": sub_path, "topic": topic_path, "enable_message_ordering": ordered}
        )
        subs.append(sub_path)
    yield raw, ref, subs[0], subs[1]
    for topic, sub in zip((raw, ref), subs, strict=True):
        subscriber.delete_subscription(subscription=sub)
        publisher.delete_topic(topic=publisher.topic_path(PUBSUB_PROJECT, topic))
    subscriber.close()
