"""connection.run against a local websocket server standing in for a venue."""

import asyncio
import base64
import hashlib
import json
import time
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any, ClassVar

import pytest
import websockets
from prometheus_client import REGISTRY

from bbo_tap import connection
from bbo_tap.exchanges import Exchange


class Fake(Exchange):
    name = "fake"
    url = ""  # set per test
    headers: ClassVar[dict[str, str]] = {"X-Test": "1"}
    ping_interval = 0.3
    subscribe_interval = 0.1

    def subscribe_messages(self, symbols):
        return [json.dumps({"sub": s}) for s in symbols]

    def ping_message(self):
        return "ping"


@dataclass
class Received:
    """What the fake venue got from us."""

    headers: Any = None
    subs: list[tuple[float, str]] = field(default_factory=list)  # (monotonic time, message)
    pings: list[float] = field(default_factory=list)


@pytest.fixture
async def venue():
    """Start a fake venue in a given mode; returns (exchange, log of what the server got)."""
    servers = []

    async def start(mode):
        got = Received()

        async def handler(ws):
            got.headers = ws.request.headers
            await ws.send(b'{"welcome":true}')  # binary, like KuCoin
            async for m in ws:
                if m == "ping":
                    got.pings.append(time.monotonic())
                    continue
                got.subs.append((time.monotonic(), m))
                await ws.send(json.dumps({"ack": json.loads(m)["sub"]}))
                if mode == "close" and len(got.subs) == 2:
                    await ws.close(1000, "bye")
                    return
                if mode == "stream":
                    for i in range(3):
                        await ws.send(json.dumps({"d": i}))
            # mode "silent": nothing after the acks

        server = await websockets.serve(handler, "127.0.0.1", 0)
        servers.append(server)
        port = server.sockets[0].getsockname()[1]
        return type("F", (Fake,), {"url": f"ws://127.0.0.1:{port}"})(), got

    yield start
    for s in servers:
        s.close()
        await s.wait_closed()


@pytest.fixture
def fast(monkeypatch):
    monkeypatch.setattr(connection, "IDLE_TIMEOUT", 30.0)
    monkeypatch.setattr(connection, "ROTATE_AFTER", 60.0)
    return lambda **kw: [monkeypatch.setattr(connection, k, v) for k, v in kw.items()]


async def test_rotate_streams_everything_in_order(venue, fast, fake_sink):
    fast(ROTATE_AFTER=1.5)
    ex, got = await venue("stream")
    labels = {"exchange": "fake", "shard": "7"}
    frames_before = REGISTRY.get_sample_value("bbo_tap_frames_total", labels) or 0.0
    out = await connection.run(ex, 3, 7, ["A", "B", "C"], fake_sink)

    assert out.reason == "rotate"
    assert 1.4 < out.connected_for < 2.5
    assert got.headers["X-Test"] == "1"
    assert [m for _, m in got.subs] == ['{"sub": "A"}', '{"sub": "B"}', '{"sub": "C"}']
    gaps = [b - a for (a, _), (b, _) in pairwise(got.subs)]
    assert all(g >= 0.09 for g in gaps), gaps
    assert 3 <= len(got.pings) <= 6  # every 0.3 s for ~1.5 s

    recs = fake_sink.records
    assert len(recs) == 1 + 3 * 4  # welcome + per subscribe: ack and 3 data
    assert [r[3] for r in recs] == list(range(len(recs)))  # seq
    assert recs[0][5] == '{"welcome":true}'  # binary decoded to text
    assert all(isinstance(r[5], str) for r in recs)
    exchange, partition, conn_id = recs[0][:3]
    assert (exchange, partition) == ("fake", 3)
    assert conn_id.startswith("fake-7-")
    assert all(r[2] == conn_id for r in recs)
    ts = [r[4] for r in recs]
    assert ts == sorted(ts)
    assert abs(ts[0] / 1e9 - time.time()) < 5

    assert REGISTRY.get_sample_value("bbo_tap_frames_total", labels) == frames_before + len(recs)
    assert REGISTRY.get_sample_value("bbo_tap_last_frame_timestamp_seconds", labels) == ts[-1] / 1e9
    assert REGISTRY.get_sample_value("bbo_tap_connected", labels) == 0  # cleared on close


async def test_idle(venue, fast, fake_sink):
    fast(IDLE_TIMEOUT=1.0)
    ex, got = await venue("silent")  # pings are sent but never answered
    t = time.monotonic()
    out = await connection.run(ex, 0, 0, ["A"], fake_sink)
    assert out.reason == "idle"
    assert 1.0 <= time.monotonic() - t < 3.5
    assert got.pings  # app pings alone don't count as received frames


async def test_server_close(venue, fast, fake_sink):
    ex, _ = await venue("close")
    out = await connection.run(ex, 0, 0, ["A", "B", "C"], fake_sink)
    assert out.reason == "closed"
    assert "1000" in out.detail
    assert "bye" in out.detail


async def test_connect_failed(fake_sink):
    ex = type("F", (Fake,), {"url": "ws://127.0.0.1:1"})()
    out = await connection.run(ex, 0, 0, ["A"], fake_sink)
    assert out.reason == "connect_failed"
    assert out.connected_for == 0.0
    assert fake_sink.records == []


async def test_new_conn_id_per_connect(venue, fast, fake_sink):
    fast(ROTATE_AFTER=0.3)
    ex, _ = await venue("stream")
    await connection.run(ex, 0, 0, ["A"], fake_sink)
    await asyncio.sleep(0.01)
    await connection.run(ex, 0, 0, ["A"], fake_sink)
    conn_ids = list(dict.fromkeys(r[2] for r in fake_sink.records))
    assert len(conn_ids) == 2
    assert next(r[3] for r in fake_sink.records if r[2] == conn_ids[1]) == 0  # seq restarts


async def test_sink_error_propagates(venue, fast):
    ex, _ = await venue("stream")

    class BoomError(Exception):
        pass

    class FailingSink:
        def send(self, exchange: str, partition: int, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None:
            raise BoomError

    with pytest.raises(BoomError):
        await connection.run(ex, 0, 0, ["A"], FailingSink())


async def test_cancel_closes_socket(venue, fast, fake_sink):
    ex, _ = await venue("stream")
    task = asyncio.create_task(connection.run(ex, 0, 0, ["A"], fake_sink))
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_stats_logged(venue, fast, fake_sink, caplog, monkeypatch):
    monkeypatch.setattr(connection, "STATS_INTERVAL", 0.5)
    fast(ROTATE_AFTER=1.2)
    ex, _ = await venue("stream")
    with caplog.at_level("INFO", logger="bbo_tap.connection"):
        await connection.run(ex, 0, 0, ["A", "B"], fake_sink)
    rates = [r.getMessage() for r in caplog.records if "frames/s" in r.getMessage()]
    assert len(rates) == 2
    assert rates[0].startswith("fake-0-")


@pytest.fixture
async def deaf_venue():
    """Raw websocket server that streams but never answers a close frame (like Binance)."""

    async def handle(reader, writer):
        request = await reader.readuntil(b"\r\n\r\n")
        key = next(
            line.split(b":", 1)[1].strip()
            for line in request.split(b"\r\n")
            if line.lower().startswith(b"sec-websocket-key")
        )
        accept = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
        writer.write(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n"
        )
        try:
            while True:
                writer.write(b"\x81\x01x")  # unmasked text frame "x"; incoming frames are ignored
                await writer.drain()
                await asyncio.sleep(0.01)
        except (ConnectionError, asyncio.CancelledError):
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    yield type("F", (Fake,), {"url": f"ws://127.0.0.1:{port}", "ping_interval": None})()
    server.close()


async def test_rotate_waits_for_close_handshake(deaf_venue, fast, fake_sink, monkeypatch):
    """Control case: a normal close to a deaf venue waits CLOSE_TIMEOUT."""
    monkeypatch.setattr(connection, "CLOSE_TIMEOUT", 1.0)
    fast(ROTATE_AFTER=0.3)
    t = time.monotonic()
    out = await connection.run(deaf_venue, 0, 0, [], fake_sink)
    assert out.reason == "rotate"
    assert time.monotonic() - t >= 1.2


async def test_shutdown_does_not_wait_for_close_handshake(deaf_venue, fast, fake_sink, monkeypatch):
    monkeypatch.setattr(connection, "CLOSE_TIMEOUT", 1.0)
    task = asyncio.create_task(connection.run(deaf_venue, 0, 0, [], fake_sink))
    await asyncio.sleep(0.3)
    assert fake_sink.records  # streaming
    t = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.monotonic() - t < 0.2
