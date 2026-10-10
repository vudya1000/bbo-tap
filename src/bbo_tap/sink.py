"""Redpanda sink: wraps each frame in the record envelope and produces it.

Delivery is at least once: duplicates are allowed (frames are snapshots), loss
is not silent. A full local queue makes `send` wait, and any delivery failure
or fatal client error raises SinkError so the process exits non-zero.
"""

import asyncio
import json
import logging
import time

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from . import metrics
from .config import Config

log = logging.getLogger(__name__)

POLL_INTERVAL = 0.1  # seconds between delivery-error checks
FULL_QUEUE_WAIT = 0.05  # seconds `send` waits for room when the local queue is full


class SinkError(RuntimeError):
    pass


class Sink:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._error: KafkaError | None = None
        self.sent = 0  # records handed to the producer
        self._encode = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False).encode
        self._producer = Producer(
            {
                "bootstrap.servers": ",".join(cfg.brokers),
                "client.id": "bbo-tap",
                # Every frame is a full top-of-book snapshot, so a duplicate (a retry
                # after a lost ack) is harmless: no idempotence, no waiting for replicas.
                "acks": 1,
                "enable.idempotence": False,
                "compression.type": "zstd",
                # Bigger batches, better compression, fewer requests. Delay doesn't
                # matter: state is reconstructed by ts_recv_ns, not arrival time.
                "linger.ms": 50,
                # Redpanda may be unreachable this long before records fail and we exit.
                "message.timeout.ms": 30_000,
                # Room for those 30 s at full rate. The message cap is raised from its
                # 100k default so the byte limit is the one that applies.
                "queue.buffering.max.kbytes": 512 * 1024,
                "queue.buffering.max.messages": 2_000_000,
                # Successful deliveries cost no Python callback.
                "delivery.report.only.error": True,
                "on_delivery": self._on_delivery,
                "error_cb": self._on_error,
                # librdkafka's own log (e.g. a line per failed connect) goes through
                # Python logging instead of raw stderr.
                "logger": logging.getLogger("librdkafka"),
            }
        )
        metrics.QUEUE.set_function(lambda: len(self._producer))

    def check(self, timeout: float = 10.0) -> None:
        """Fail unless both topics exist and raw.ws has every configured partition."""
        try:
            topics = self._producer.list_topics(timeout=timeout).topics
        except KafkaException as e:
            raise SinkError(f"Redpanda {','.join(self._cfg.brokers)} unreachable: {e}") from e
        for name in (self._cfg.topic, self._cfg.symbols_topic):
            if name not in topics or topics[name].error is not None:
                raise SinkError(f"topic {name} missing, run scripts/create_topics.sh")
        have = len(topics[self._cfg.topic].partitions)
        need = max(v.partition for v in self._cfg.venues.values()) + 1
        if have < need:
            raise SinkError(f"topic {self._cfg.topic} has {have} partitions, config needs {need}")

    def publish_symbols(self, timeout: float = 10.0) -> None:
        """Write the symbol table to symbols_topic, one record per unified symbol, and wait for it."""
        for unified, row in self._cfg.symbols.items():
            self._producer.produce(self._cfg.symbols_topic, self._encode(row), key=unified)
        self._flush(timeout)
        log.info("published %d symbols to %s", len(self._cfg.symbols), self._cfg.symbols_topic)

    def send(self, exchange: str, partition: int, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None:
        """Produce one frame. Blocks (on purpose) while the local queue is full."""
        value = self._encode(
            {
                "exchange": exchange,
                "conn_id": conn_id,
                "seq": seq,
                "ts_recv_ns": ts_recv_ns,
                "payload": payload,
            }
        )
        blocked_since = None
        while True:
            if self._error is not None:
                raise SinkError(f"delivery failed: {self._error}")
            try:
                self._producer.produce(
                    self._cfg.topic,
                    value,
                    key=exchange,
                    partition=partition,
                    timestamp=ts_recv_ns // 1_000_000,
                )
                self.sent += 1
                metrics.RECORDS.inc()
                if blocked_since is not None:
                    metrics.SEND_BLOCKED.inc(time.monotonic() - blocked_since)
                return
            except BufferError:
                blocked_since = blocked_since or time.monotonic()
                self._producer.poll(FULL_QUEUE_WAIT)

    async def run(self) -> None:
        """Serve delivery reports until one fails; then raise."""
        while True:
            self._producer.poll(0)
            if self._error is not None:
                raise SinkError(f"delivery failed: {self._error}")
            await asyncio.sleep(POLL_INTERVAL)

    def close(self, timeout: float = 10.0) -> None:
        """Deliver what is queued; raise if anything is left or failed."""
        self._flush(timeout)

    def _flush(self, timeout: float) -> None:
        left = self._producer.flush(timeout)
        if self._error is not None:
            raise SinkError(f"delivery failed: {self._error}")
        if left:
            raise SinkError(f"{left} records not delivered within {timeout} s")

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None and self._error is None:
            self._error = err

    def _on_error(self, err: KafkaError) -> None:
        # Non-fatal errors (e.g. all brokers down) are retried by librdkafka until
        # message.timeout.ms fails the records; only fatal ones stop us here.
        if err.fatal():
            self._error = self._error or err
        elif err.code() in (KafkaError._TRANSPORT, KafkaError._RESOLVE):
            # One per failed (re)connect, several a second; _ALL_BROKERS_DOWN reports the state every 10 s.
            log.debug("redpanda: %s", err)
        else:
            log.warning("redpanda: %s", err)
