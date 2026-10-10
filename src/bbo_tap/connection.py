"""One websocket connection, from open to close.

Open, subscribe, keep an application ping on a fixed timer, and send every
received frame to the sink. Returns why the connection ended; reconnecting and
backoff are the runner's job. Sink errors are not caught: they end the process.
"""

import asyncio
import contextlib
import functools
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

import websockets
from websockets.exceptions import ConnectionClosed

from . import metrics
from .exchanges import Exchange

log = logging.getLogger(__name__)

CONNECT_TIMEOUT = 10.0
PROTOCOL_PING_INTERVAL = 20.0  # websocket-level ping, handled by the library
PROTOCOL_PING_TIMEOUT = 20.0
CLOSE_TIMEOUT = 5.0
IDLE_TIMEOUT = 30.0  # no frame of any kind for this long -> reconnect
ROTATE_AFTER = 12 * 3600.0  # proactive reconnect
STATS_INTERVAL = 60.0  # log frames/s per connection this often


class FrameSink(Protocol):
    """Where received frames go (the Redpanda Sink in production)."""

    def send(self, exchange: str, partition: int, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None: ...


@dataclass(frozen=True)
class Outcome:
    reason: str  # "rotate" | "idle" | "closed" | "connect_failed"
    detail: str
    connected_for: float  # seconds the socket was open; 0 if it never opened


async def run(exchange: Exchange, partition: int, shard: int, symbols: list[str], sink: FrameSink) -> Outcome:
    subscribes = exchange.subscribe_messages(symbols)  # built before connecting, so a bug fails loudly
    try:
        ws = await websockets.connect(
            exchange.url,
            additional_headers=exchange.headers,
            open_timeout=CONNECT_TIMEOUT,
            ping_interval=PROTOCOL_PING_INTERVAL,
            ping_timeout=PROTOCOL_PING_TIMEOUT,
            close_timeout=CLOSE_TIMEOUT,
        )
    except (OSError, TimeoutError, websockets.WebSocketException) as e:
        return Outcome("connect_failed", repr(e), 0.0)

    opened = time.monotonic()
    conn_id = f"{exchange.name}-{shard}-{time.time_ns() // 1_000_000}"
    log.info("%s open, %d symbols", conn_id, len(symbols))
    frames = metrics.FRAMES.labels(exchange.name, shard)
    last_frame_ts = metrics.LAST_FRAME.labels(exchange.name, shard)
    connected = metrics.CONNECTED.labels(exchange.name, shard)
    connected.set(1)
    last_frame = opened
    idle = False
    seq = 0

    async def subscribe() -> None:
        for i, msg in enumerate(subscribes):
            if i and exchange.subscribe_interval:
                await asyncio.sleep(exchange.subscribe_interval)
            await ws.send(msg)

    async def ping(interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            message = exchange.ping_message()
            if message is not None:
                await ws.send(message)

    async def watchdog() -> None:
        nonlocal idle
        # Sleep until the deadline the last frame set; every frame pushes it out.
        while (left := last_frame + IDLE_TIMEOUT - time.monotonic()) > 0:  # noqa: ASYNC110 (a deadline, not an event)
            await asyncio.sleep(left)
        idle = True
        await ws.close()

    async def stats() -> None:
        last = 0
        while True:
            await asyncio.sleep(STATS_INTERVAL)
            log.info("%s %.0f frames/s", conn_id, (seq - last) / STATS_INTERVAL)
            last = seq

    helpers: list[Callable[[], Awaitable[None]]] = [subscribe, watchdog, stats]
    if exchange.ping_interval:
        helpers.append(functools.partial(ping, exchange.ping_interval))
    tasks = [asyncio.create_task(_quiet(h)) for h in helpers]

    shutdown = False
    try:
        async with asyncio.timeout(ROTATE_AFTER):
            while True:
                frame = await ws.recv(decode=True)  # binary frames (KuCoin) decoded as UTF-8
                ts_recv_ns = time.time_ns()
                last_frame = time.monotonic()
                sink.send(exchange.name, partition, conn_id, seq, ts_recv_ns, frame)
                seq += 1
                frames.inc()
                last_frame_ts.set(ts_recv_ns / 1e9)
    except TimeoutError:
        reason, detail = "rotate", ""
    except ConnectionClosed as e:
        reason, detail = ("idle" if idle else "closed"), str(e)
    except asyncio.CancelledError:
        log.info("%s closing: shutdown (%d frames)", conn_id, seq)
        shutdown = True
        raise
    finally:
        connected.set(0)
        for t in tasks:
            t.cancel()
        if shutdown:
            # No close handshake on the way out: some venues (Binance) never
            # answer it, which would hold every stop for CLOSE_TIMEOUT.
            ws.transport.abort()
        else:
            await ws.close()

    connected_for = time.monotonic() - opened
    log.info("%s ended: %s %s (%d frames, %.0f s)", conn_id, reason, detail, seq, connected_for)
    return Outcome(reason, detail, connected_for)


async def _quiet(helper: Callable[[], Awaitable[None]]) -> None:
    """Run a helper; a closed socket just ends it, the read loop reports why."""
    with contextlib.suppress(ConnectionClosed):
        await helper()
