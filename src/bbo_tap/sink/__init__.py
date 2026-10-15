"""Sinks: where records go. One per backend, chosen by the config's `redpanda:` or `pubsub:` section.

Every sink writes the same record (`encode_record`) and keeps the same rules:
delivery is at least once (duplicates are allowed, frames are snapshots), loss
is never silent (a failed delivery raises SinkError, so the process exits
non-zero), and a full local queue makes `send` wait instead of dropping.
"""

import json
from typing import Protocol

from ..config import Config, Kafka


class SinkError(RuntimeError):
    pass


class Sink(Protocol):
    sent: int  # records handed to the client, for the heartbeat

    def check(self, timeout: float = 10.0) -> None:
        """Fail unless the backend is reachable and both topics exist."""

    def publish_symbols(self, timeout: float = 10.0) -> None:
        """Write the symbol table, one record per unified symbol, and wait for it."""

    def send(self, exchange: str, partition: int | None, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None:
        """Hand one frame to the client. Blocks (on purpose) while the local queue is full."""

    async def run(self) -> None:
        """Watch deliveries until one fails; then raise SinkError."""

    def close(self, timeout: float = 10.0) -> None:
        """Deliver what is queued; raise SinkError if anything is left or failed."""


_encode = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False).encode


def encode_record(exchange: str, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> str:
    """The raw record, as JSON."""
    return _encode({"exchange": exchange, "conn_id": conn_id, "seq": seq, "ts_recv_ns": ts_recv_ns, "payload": payload})


def encode_symbols(row: dict[str, str]) -> str:
    """One symbol-table row, {venue: venue symbol}, as JSON."""
    return _encode(row)


def make_sink(cfg: Config) -> Sink:
    if isinstance(cfg.sink, Kafka):
        from .kafka import KafkaSink

        return KafkaSink(cfg, cfg.sink)
    try:
        from .pubsub import PubSubSink
    except ImportError as e:
        raise SinkError(f"Pub/Sub support is not installed ({e}); install bbo-tap[pubsub]") from e
    return PubSubSink(cfg, cfg.sink)
