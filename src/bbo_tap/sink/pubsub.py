"""Google Cloud Pub/Sub sink: publishes each record to the raw topic with ordering key = exchange.

The ordering key is Pub/Sub's counterpart of one Kafka partition per venue: a
subscription with message ordering enabled receives each venue's frames in
receive order. The record itself is the same JSON as with Redpanda.

A full local queue makes `send` wait, and any failed publish (after retrying
for PUBLISH_TIMEOUT) raises SinkError so the process exits non-zero.
"""

import asyncio
import logging
import os
import threading
import time

import google.auth
from google.api_core import exceptions as gexc
from google.api_core.retry import Retry, if_exception_type
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.exceptions import FlowControlLimitError, PublishToPausedOrderingKeyException
from google.cloud.pubsub_v1.publisher.futures import Future
from google.cloud.pubsub_v1.types import BatchSettings, LimitExceededBehavior, PublisherOptions, PublishFlowControl

from .. import metrics
from ..config import Config, PubSub
from . import SinkError, encode_record, encode_symbols

log = logging.getLogger(__name__)

POLL_INTERVAL = 0.1  # seconds between delivery-error checks
FULL_QUEUE_WAIT = 0.05  # seconds `send` waits for room when the local queue is full
PUBLISH_TIMEOUT = 30.0  # Pub/Sub may be unreachable this long before a publish fails and we exit
ATTEMPT_TIMEOUT = 10.0  # one publish request
# Room for those 30 s at full rate (~1.8k frames/s with 25 symbols on five venues).
QUEUE_MESSAGES = 100_000
QUEUE_BYTES = 128 * 1024 * 1024

# The publish errors the client library itself retries by default.
_RETRY = Retry(
    predicate=if_exception_type(
        gexc.Aborted,
        gexc.Cancelled,
        gexc.DeadlineExceeded,
        gexc.InternalServerError,
        gexc.ResourceExhausted,
        gexc.ServiceUnavailable,
        gexc.Unknown,
    ),
    initial=0.1,
    maximum=5.0,
    multiplier=2.0,
    timeout=PUBLISH_TIMEOUT,
)


class PubSubSink:
    def __init__(self, cfg: Config, settings: PubSub):
        self._cfg = cfg
        self._error: BaseException | None = None
        self.sent = 0  # records handed to the publisher
        self._done = 0  # records whose publish finished, either way
        self._done_lock = threading.Lock()  # done-callbacks run on the client's threads
        try:
            self._publisher = pubsub_v1.PublisherClient(
                # Bigger batches, fewer requests (each is billed at least 1 KB). Delay doesn't
                # matter: state is reconstructed by ts_recv_ns, not arrival time.
                batch_settings=BatchSettings(max_bytes=1_000_000, max_latency=0.05, max_messages=1000),
                publisher_options=PublisherOptions(
                    enable_message_ordering=True,
                    flow_control=PublishFlowControl(
                        message_limit=QUEUE_MESSAGES,
                        byte_limit=QUEUE_BYTES,
                        limit_exceeded_behavior=LimitExceededBehavior.ERROR,
                    ),
                    retry=_RETRY,
                    timeout=ATTEMPT_TIMEOUT,
                ),
                client_options={"api_endpoint": settings.endpoint} if settings.endpoint else None,
            )
        except DefaultCredentialsError as e:
            raise SinkError(f"no Google credentials: {e}") from e
        project = _project(settings)
        self._topic = self._publisher.topic_path(project, settings.topic)
        self._symbols_topic = self._publisher.topic_path(project, settings.symbols_topic)
        metrics.QUEUE.set_function(self._pending)

    def check(self, timeout: float = 10.0) -> None:
        """Fail unless both topics exist and can be read (roles/pubsub.viewer)."""
        retry = Retry(timeout=timeout)
        for topic in (self._topic, self._symbols_topic):
            try:
                self._publisher.get_topic(topic=topic, retry=retry, timeout=timeout)
            except gexc.NotFound as e:
                raise SinkError(f"topic {topic} missing") from e
            except gexc.PermissionDenied as e:
                raise SinkError(f"no access to {topic}: needs roles/pubsub.viewer and roles/pubsub.publisher") from e
            except (gexc.GoogleAPIError, DefaultCredentialsError) as e:
                raise SinkError(f"Pub/Sub unreachable: {e}") from e

    def publish_symbols(self, timeout: float = 10.0) -> None:
        """Write the symbol table to symbols_topic, one message per unified symbol, and wait for it.

        Message data is the {venue: venue symbol} row; attribute `symbol` is the unified symbol.
        """
        futures = [
            self._publisher.publish(self._symbols_topic, encode_symbols(row).encode(), symbol=unified)
            for unified, row in self._cfg.symbols.items()
        ]
        deadline = time.monotonic() + timeout
        try:
            for f in futures:
                f.result(timeout=max(0.0, deadline - time.monotonic()))
        except TimeoutError as e:
            raise SinkError(f"symbols not delivered within {timeout} s") from e
        except Exception as e:
            raise SinkError(f"delivery failed: {e!r}") from e
        log.info("published %d symbols to %s", len(futures), self._symbols_topic)

    def send(self, exchange: str, partition: int | None, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None:
        """Publish one frame. Blocks (on purpose) while the local queue is full."""
        data = encode_record(exchange, conn_id, seq, ts_recv_ns, payload).encode()
        blocked_since = None
        while True:
            if self._error is not None:
                raise SinkError(f"delivery failed: {self._error!r}")
            try:
                future = self._publisher.publish(self._topic, data, ordering_key=exchange)
            except FlowControlLimitError:
                blocked_since = blocked_since or time.monotonic()
                time.sleep(FULL_QUEUE_WAIT)
                continue
            except PublishToPausedOrderingKeyException as e:
                # An earlier publish for this venue failed; its done-callback may not have run yet.
                raise SinkError(f"delivery failed: {self._error or e!r}") from e
            future.add_done_callback(self._on_done)
            self.sent += 1
            metrics.RECORDS.inc()
            if blocked_since is not None:
                metrics.SEND_BLOCKED.inc(time.monotonic() - blocked_since)
            return

    async def run(self) -> None:
        """Watch publish results until one fails; then raise."""
        while True:
            if self._error is not None:
                raise SinkError(f"delivery failed: {self._error!r}")
            await asyncio.sleep(POLL_INTERVAL)

    def close(self, timeout: float = 10.0) -> None:
        """Deliver what is queued; raise if anything is left or failed."""
        deadline = time.monotonic() + timeout
        while self._pending() and self._error is None and time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL)
        if self._error is not None:
            raise SinkError(f"delivery failed: {self._error!r}")
        if left := self._pending():
            raise SinkError(f"{left} records not delivered within {timeout} s")
        self._publisher.stop()

    def _pending(self) -> int:
        return self.sent - self._done

    def _on_done(self, future: Future) -> None:
        with self._done_lock:
            self._done += 1
        if (e := future.exception()) is not None and self._error is None:
            self._error = e


def _project(settings: PubSub) -> str:
    """pubsub.project, else GOOGLE_CLOUD_PROJECT, else the credentials' project (on GKE: the cluster's)."""
    if settings.project:
        return settings.project
    if project := os.environ.get("GOOGLE_CLOUD_PROJECT"):
        return project
    try:
        _, project = google.auth.default()
    except DefaultCredentialsError as e:
        raise SinkError(f"no Pub/Sub project: set pubsub.project or GOOGLE_CLOUD_PROJECT ({e})") from e
    if not project:
        raise SinkError("no Pub/Sub project: set pubsub.project or GOOGLE_CLOUD_PROJECT")
    return str(project)
