"""Pub/Sub sink and the whole runner against the Pub/Sub emulator (localhost:8085), on throwaway topics."""

import asyncio
import json
import os
import signal
import time

import pytest
import websockets
from google.cloud import pubsub_v1
from google.pubsub_v1.types import PubsubMessage

from bbo_tap import runner
from bbo_tap.config import parse
from bbo_tap.exchanges import Exchange
from bbo_tap.sink import SinkError, make_sink
from bbo_tap.sink.pubsub import PubSubSink
from tests.conftest import PUBSUB_PROJECT

pytestmark = pytest.mark.pubsub

VENUES = ("binance", "bybit", "okx", "gate", "kucoin")
TABLE = {
    "BTC-USDT": {
        "binance": "BTCUSDT",
        "bybit": "BTCUSDT",
        "okx": "BTC-USDT-SWAP",
        "gate": "BTC_USDT",
        "kucoin": "XBTUSDTM",
    },
    "ETH-USDT": {"binance": "ETHUSDT", "okx": "ETH-USDT-SWAP"},
}


def make_cfg(raw, ref, venues=VENUES):
    return parse(
        {
            "pubsub": {"project": PUBSUB_PROJECT, "topic": raw, "symbols_topic": ref},
            "exchanges": {name: {"enabled": name in venues} for name in VENUES},
            "symbols": TABLE,
        }
    )


def pull_all(subscription, want, timeout=30.0):
    """Pull and ack until `want` messages arrived, in delivery order."""
    subscriber = pubsub_v1.SubscriberClient()
    out: list[PubsubMessage] = []
    deadline = time.monotonic() + timeout
    while len(out) < want and time.monotonic() < deadline:
        resp = subscriber.pull(subscription=subscription, max_messages=1000, timeout=5)
        if resp.received_messages:
            subscriber.acknowledge(subscription=subscription, ack_ids=[m.ack_id for m in resp.received_messages])
            out += [m.message for m in resp.received_messages]
    subscriber.close()
    return out


def test_make_sink_picks_pubsub(pubsub_topics):
    raw, ref, _, _ = pubsub_topics
    assert isinstance(make_sink(make_cfg(raw, ref)), PubSubSink)


def test_check_passes_on_good_topics(pubsub_topics):
    raw, ref, _, _ = pubsub_topics
    make_sink(make_cfg(raw, ref)).check()


def test_check_missing_topic(pubsub_topics):
    _, ref, _, _ = pubsub_topics
    with pytest.raises(SinkError, match="test-missing missing"):
        make_sink(make_cfg("test-missing", ref)).check()


def test_check_unreachable(monkeypatch):
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:1")
    with pytest.raises(SinkError, match="unreachable"):
        make_sink(make_cfg("raw", "ref")).check(timeout=2)


def test_round_trip(pubsub_topics):
    raw, ref, raw_sub, ref_sub = pubsub_topics
    cfg = make_cfg(raw, ref)
    sink = make_sink(cfg)
    sink.publish_symbols()
    sent = {}
    for i in range(5000):
        name = VENUES[i % 5]
        payload = json.dumps({"i": i, "q": 'quote"inside', "u": "ü"})
        sink.send(name, None, f"{name}-0-1", i // 5, 1_791_000_000_000_000_000 + i, payload)
        sent[(name, i // 5)] = payload
    sink.close()

    msgs = pull_all(raw_sub, 5000)
    assert len(msgs) == 5000
    order: dict[str, list[int]] = {}
    for m in msgs:
        r = json.loads(m.data)
        assert m.ordering_key == r["exchange"]
        assert r["payload"] == sent[(r["exchange"], r["seq"])]
        assert r["conn_id"] == f"{r['exchange']}-0-1"
        order.setdefault(r["exchange"], []).append(r["seq"])
    # The ordered subscription delivers each venue's frames in the order they were sent.
    assert order == {name: list(range(1000)) for name in VENUES}

    rows = {m.attributes["symbol"]: json.loads(m.data) for m in pull_all(ref_sub, len(TABLE))}
    assert rows == TABLE


async def test_failed_publish_stops_the_sink(pubsub_topics):
    """Loss is never silent: a publish that fails (topic deleted under us) makes run() raise."""
    raw, ref, _, _ = pubsub_topics
    sink = make_sink(make_cfg(raw, ref))
    sink.check()
    pubsub_v1.PublisherClient().delete_topic(topic=f"projects/{PUBSUB_PROJECT}/topics/{raw}")
    sink.send("okx", None, "okx-0-1", 0, 1, "x")
    with pytest.raises(SinkError, match="delivery failed"):
        await asyncio.wait_for(sink.run(), timeout=10)
    # The fixture deletes the topic again; recreate it so that works.
    pubsub_v1.PublisherClient().create_topic(name=f"projects/{PUBSUB_PROJECT}/topics/{raw}")


async def test_runner_end_to_end(pubsub_topics, monkeypatch):
    """Two fake venues stream through the real runner and Pub/Sub sink; SIGTERM stops it cleanly."""

    async def handler(ws):
        async for m in ws:
            if m == "ping":
                await ws.send("pong")
                continue
            for i in range(50):
                await ws.send(json.dumps({"sym": json.loads(m)["sub"], "i": i}))
                await asyncio.sleep(0.005)

    server = await websockets.serve(handler, "127.0.0.1", 0)
    url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"

    def fake(name):
        return type(
            name,
            (Exchange,),
            {
                "name": name,
                "url": url,
                "ping_interval": 0.2,
                "max_symbols_per_conn": 1,
                "subscribe_messages": lambda self, symbols: [json.dumps({"sub": s}) for s in symbols],
                "ping_message": lambda self: "ping",
            },
        )

    monkeypatch.setattr(runner, "EXCHANGES", {"okx": fake("okx"), "binance": fake("binance")})
    monkeypatch.setattr(runner, "CONNECT_SPACING", 0.05)
    raw, ref, raw_sub, _ = pubsub_topics
    cfg = make_cfg(raw, ref, venues=("okx", "binance"))

    async def stop_soon():
        await asyncio.sleep(1.5)
        os.kill(os.getpid(), signal.SIGTERM)

    stopper = asyncio.create_task(stop_soon())
    await runner.run(cfg)  # returns only after a clean stop and flush
    await stopper
    server.close()

    # 4 connections x (50 data frames + pongs): pull until nothing more arrives.
    msgs = await asyncio.to_thread(pull_all, raw_sub, 200)
    msgs += await asyncio.to_thread(pull_all, raw_sub, 10_000, 3.0)
    records = [json.loads(m.data) for m in msgs]
    assert {m.ordering_key for m in msgs} == {"okx", "binance"}
    assert len({r["conn_id"] for r in records}) == 4
    assert sum('"i": 49' in r["payload"] for r in records) == 4  # every connection streamed to the end
    assert any(r["payload"] == "pong" for r in records)

    # No frame lost: per connection, seq covers 0..max.
    seqs: dict[str, set[int]] = {}
    for r in records:
        seqs.setdefault(r["conn_id"], set()).add(r["seq"])
    assert all(s == set(range(max(s) + 1)) for s in seqs.values())
