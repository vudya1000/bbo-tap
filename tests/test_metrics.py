import socket
import urllib.request

import pytest
from prometheus_client import REGISTRY

from bbo_tap import connection, metrics, runner
from bbo_tap.connection import Outcome
from bbo_tap.exchanges import Okx


def value(name, **labels):
    return REGISTRY.get_sample_value(name, {k: str(v) for k, v in labels.items()}) or 0.0


def test_serve_exposes_metrics():
    port = metrics.serve(0, "127.0.0.1")
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    assert "bbo_tap_records_produced_total" in body
    assert "bbo_tap_producer_queue_messages" in body


def test_serve_port_taken():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen()
        with pytest.raises(OSError, match="in use"):
            metrics.serve(s.getsockname()[1], "127.0.0.1")


async def test_supervise_counts_disconnects(monkeypatch, fake_sink):
    outcomes = iter([Outcome("connect_failed", "", 0.0), Outcome("idle", "", 5.0), Outcome("idle", "", 5.0)])

    class _DoneError(Exception):
        pass

    async def fake_run(*args):
        try:
            return next(outcomes)
        except StopIteration:
            raise _DoneError from None

    monkeypatch.setattr(connection, "run", fake_run)
    monkeypatch.setattr(runner, "backoff", lambda failures: 0.0)
    monkeypatch.setattr(runner, "CONNECT_SPACING", 0.0)
    before = {r: value("bbo_tap_disconnects_total", exchange="okx", reason=r) for r in ("connect_failed", "idle")}
    with pytest.raises(_DoneError):
        await runner.supervise(Okx(), 0, 0, ["A"], fake_sink, runner.Pacer())
    assert value("bbo_tap_disconnects_total", exchange="okx", reason="connect_failed") == before["connect_failed"] + 1
    assert value("bbo_tap_disconnects_total", exchange="okx", reason="idle") == before["idle"] + 2
