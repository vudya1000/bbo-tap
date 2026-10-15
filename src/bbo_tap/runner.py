"""Runner: shard each venue's symbols, keep every shard connected, stop cleanly.

Each shard has its own loop: connect, stream until the connection ends, back
off, repeat. A sink failure or an unexpected error in any loop stops the whole
process; SIGTERM/SIGINT closes the sockets and flushes the sink first.
"""

import asyncio
import logging
import random
import signal
from collections.abc import Callable
from pathlib import Path

from . import connection, metrics
from .config import Config
from .exchanges import EXCHANGES, Exchange
from .sink import make_sink

log = logging.getLogger(__name__)

BACKOFF_BASE = 1.0  # seconds; doubles per consecutive failure
BACKOFF_MAX = 60.0
BACKOFF_RESET_AFTER = 60.0  # a connection up this long clears the failure count
CONNECT_SPACING = 1.0  # min seconds between connection attempts per venue
HEARTBEAT_INTERVAL = 10.0  # seconds between heartbeat checks


def shards(symbols: tuple[str, ...], size: int) -> list[list[str]]:
    ordered = sorted(symbols)
    return [ordered[i : i + size] for i in range(0, len(ordered), size)]


def backoff(failures: int) -> float:
    """Full jitter: uniform over [0, min(max, base * 2^(failures-1))]."""
    return random.uniform(0, min(BACKOFF_MAX, BACKOFF_BASE * 2 ** min(failures - 1, 16)))


class Pacer:
    """Spaces connection attempts of one venue at least CONNECT_SPACING apart."""

    def __init__(self) -> None:
        self._next = 0.0

    async def wait(self) -> None:
        now = asyncio.get_running_loop().time()
        slot = max(now, self._next)
        self._next = slot + CONNECT_SPACING
        await asyncio.sleep(slot - now)


async def supervise(
    exchange: Exchange, partition: int | None, shard: int, symbols: list[str], sink: connection.FrameSink, pacer: Pacer
) -> None:
    failures = 0
    while True:
        await pacer.wait()
        out = await connection.run(exchange, partition, shard, symbols, sink)
        metrics.DISCONNECTS.labels(exchange.name, out.reason).inc()
        if out.reason == "rotate":
            failures = 0
            continue
        if out.connected_for >= BACKOFF_RESET_AFTER:
            failures = 0
        failures += 1
        delay = backoff(failures)
        log.warning(
            "%s-%d %s: %s; reconnect in %.1f s (failure %d)",
            exchange.name,
            shard,
            out.reason,
            out.detail,
            delay,
            failures,
        )
        await asyncio.sleep(delay)


async def heartbeat(sent: Callable[[], int], path: Path) -> None:
    """Touch `path` whenever records went out since the last check (`sent` counts them).

    The file goes stale when the process is alive but nothing is flowing
    (every connection down or backing off), which a container healthcheck can see.
    """
    last = -1
    while True:
        if (now := sent()) != last:
            await asyncio.to_thread(path.touch)
            last = now
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def run(cfg: Config, heartbeat_path: Path | None = None) -> None:
    sink = make_sink(cfg)
    sink.check()
    sink.publish_symbols()

    tasks = [asyncio.create_task(sink.run(), name="sink")]
    if heartbeat_path is not None:
        tasks.append(asyncio.create_task(heartbeat(lambda: sink.sent, heartbeat_path), name="heartbeat"))
    for name, venue in cfg.venues.items():
        exchange = EXCHANGES[name]()
        pacer = Pacer()
        groups = shards(venue.symbols, exchange.max_symbols_per_conn)
        # Every series exists from the start at 0, so rate() and absence alerts work before the first event.
        for reason in metrics.REASONS:
            metrics.DISCONNECTS.labels(name, reason)
        for shard in range(len(groups)):
            metrics.FRAMES.labels(name, shard)
            metrics.CONNECTED.labels(name, shard).set(0)
        for shard, symbols in enumerate(groups):
            tasks.append(
                asyncio.create_task(
                    supervise(exchange, venue.partition, shard, symbols, sink, pacer), name=f"{name}-{shard}"
                )
            )
        where = f"partition {venue.partition}" if venue.partition is not None else f"ordering key {name}"
        log.info("%s: %d symbols in %d connection(s), %s", name, len(venue.symbols), len(groups), where)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    stopper = asyncio.create_task(stop.wait())

    # Every task runs forever, so the first one to finish is either a failure or the stop signal.
    try:
        done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in (*tasks, stopper):
            t.cancel()
        await asyncio.gather(*tasks, stopper, return_exceptions=True)
    for t in done:
        if t is not stopper:
            t.result()  # raises the failure
            raise RuntimeError(f"task {t.get_name()} ended without an error")

    log.info("stopping: flushing the sink")
    sink.close()
    log.info("stopped")
