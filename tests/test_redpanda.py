"""Sink and the whole runner against a real Redpanda (localhost:9092), on throwaway topics."""

import asyncio
import dataclasses
import json
import os
import signal
import uuid

import pytest
import websockets
from confluent_kafka import Consumer, Message, TopicPartition

from bbo_tap import runner
from bbo_tap.config import parse
from bbo_tap.exchanges import Exchange
from bbo_tap.sink import SinkError
from bbo_tap.sink.kafka import KafkaSink

pytestmark = pytest.mark.redpanda

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


def make_cfg(brokers, raw, ref, venues=("binance", "bybit", "okx", "gate", "kucoin")):
    return parse(
        {
            "redpanda": {"brokers": [brokers], "topic": raw, "symbols_topic": ref},
            "exchanges": {
                name: {"enabled": name in venues, "partition": i}
                for i, name in enumerate(["binance", "bybit", "okx", "gate", "kucoin"])
            },
            "symbols": TABLE,
        }
    )


def read_all(brokers, topic, partitions):
    c = Consumer({"bootstrap.servers": brokers, "group.id": str(uuid.uuid4()), "enable.auto.commit": False})
    ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in range(partitions)}
    c.assign([TopicPartition(topic, p, 0) for p in range(partitions)])
    out: list[Message] = []
    want = sum(ends.values())
    while len(out) < want:
        out += [m for m in c.consume(10_000, 2.0) if not m.error()]
    c.close()
    return out


def _sink(cfg):
    return KafkaSink(cfg, cfg.sink)


def test_check_passes_on_good_topics(redpanda, topics):
    _sink(make_cfg(redpanda, *topics)).check()


def test_check_missing_topic(redpanda, topics):
    cfg = make_cfg(redpanda, "test.missing." + uuid.uuid4().hex[:6], topics[1])
    with pytest.raises(SinkError, match="missing"):
        _sink(cfg).check()


def test_check_too_few_partitions(redpanda, topics):
    cfg = make_cfg(redpanda, *topics)
    venues = dict(cfg.venues)
    venues["kucoin"] = dataclasses.replace(venues["kucoin"], partition=7)
    with pytest.raises(SinkError, match="has 5 partitions, config needs 8"):
        _sink(dataclasses.replace(cfg, venues=venues)).check()


def test_check_unreachable():
    cfg = make_cfg("localhost:1", "raw", "ref")
    with pytest.raises(SinkError, match="unreachable"):
        _sink(cfg).check(timeout=2)


def test_round_trip(redpanda, topics):
    raw, ref = topics
    cfg = make_cfg(redpanda, raw, ref)
    sink = _sink(cfg)
    sink.publish_symbols()
    sent = {}
    for i in range(5000):
        name = list(cfg.venues)[i % 5]
        payload = json.dumps({"i": i, "q": 'quote"inside', "u": "ü"})
        sink.send(name, cfg.venues[name].partition, f"{name}-0-1", i // 5, 1_791_000_000_000_000_000 + i, payload)
        sent[(name, i // 5)] = payload
    sink.close()

    msgs = read_all(redpanda, raw, 5)
    assert len(msgs) == 5000
    for m in msgs:
        r = json.loads(m.value())
        assert m.key().decode() == r["exchange"]
        assert m.partition() == cfg.venues[r["exchange"]].partition
        assert m.timestamp()[1] == r["ts_recv_ns"] // 1_000_000
        assert r["payload"] == sent[(r["exchange"], r["seq"])]
        assert r["conn_id"] == f"{r['exchange']}-0-1"

    rows = {m.key().decode(): json.loads(m.value()) for m in read_all(redpanda, ref, 1)}
    assert rows == TABLE


async def test_runner_end_to_end(redpanda, topics, monkeypatch):
    """Two fake venues stream through the real runner and sink; SIGTERM stops it cleanly."""

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
    raw, ref = topics
    cfg = make_cfg(redpanda, raw, ref, venues=("okx", "binance"))

    async def stop_soon():
        await asyncio.sleep(1.5)
        os.kill(os.getpid(), signal.SIGTERM)

    stopper = asyncio.create_task(stop_soon())
    await runner.run(cfg)  # returns only after a clean stop and flush
    await stopper
    server.close()

    msgs = read_all(redpanda, raw, 5)
    by_exchange: dict[str, set[int]] = {}
    for m in msgs:
        by_exchange.setdefault(m.key().decode(), set()).add(m.partition())
    assert by_exchange == {"binance": {0}, "okx": {2}}
    conns = {json.loads(m.value())["conn_id"] for m in msgs}
    assert len(conns) == 4  # one connection per symbol (max_symbols_per_conn=1), 2 symbols each
    data = [json.loads(m.value())["payload"] for m in msgs]
    assert sum('"i": 49' in p for p in data) == 4  # every connection streamed to the end
    assert any(p == "pong" for p in data)

    # No frame lost: per connection, seq covers 0..max (duplicates would be fine, gaps are not).
    seqs: dict[str, set[int]] = {}
    for m in msgs:
        r = json.loads(m.value())
        seqs.setdefault(r["conn_id"], set()).add(r["seq"])
    assert all(s == set(range(max(s) + 1)) for s in seqs.values())
