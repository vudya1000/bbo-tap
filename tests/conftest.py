import os
import socket
import uuid

import pytest

from bbo_tap.config import EXAMPLE

REDPANDA = "localhost:9092"


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
        self.records: list[tuple[str, int, str, int, int, str]] = []

    def send(self, exchange: str, partition: int, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None:
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
