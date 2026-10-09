import asyncio
import random
from itertools import chain, pairwise

import pytest

from bbo_tap import connection, runner
from bbo_tap.connection import Outcome
from bbo_tap.exchanges import Okx


def test_shards_sorted_and_chunked():
    symbols = tuple(f"S{i:02d}" for i in range(60, 0, -1))
    groups = runner.shards(symbols, 25)
    assert [len(g) for g in groups] == [25, 25, 10]
    assert list(chain.from_iterable(groups)) == sorted(symbols)


@pytest.mark.parametrize(("failures", "cap"), [(1, 1), (2, 2), (3, 4), (4, 8), (6, 32), (7, 60), (100, 60)])
def test_backoff_full_jitter_bounds(failures, cap):
    random.seed(1)
    xs = [runner.backoff(failures) for _ in range(5000)]
    assert min(xs) >= 0
    assert max(xs) <= cap
    assert max(xs) > cap * 0.95
    assert min(xs) < cap * 0.05


async def test_pacer_spaces_attempts(monkeypatch):
    monkeypatch.setattr(runner, "CONNECT_SPACING", 0.1)
    pacer, loop = runner.Pacer(), asyncio.get_running_loop()
    start, times = loop.time(), []

    async def attempt():
        await pacer.wait()
        times.append(loop.time() - start)

    await asyncio.gather(*(attempt() for _ in range(4)))
    gaps = [b - a for a, b in pairwise(sorted(times))]
    assert all(g >= 0.095 for g in gaps), gaps


class _DoneError(Exception):
    pass


async def test_supervise_failure_count(monkeypatch, fake_sink):
    """Short failures escalate; 60 s connected or a rotate clears the count; rotate skips backoff."""
    outcomes = iter(
        [
            Outcome("connect_failed", "", 0.0),
            Outcome("closed", "", 5.0),
            Outcome("idle", "", 120.0),  # was up > 60 s: count resets before this failure
            Outcome("rotate", "", 43200.0),  # no backoff, count reset
            Outcome("closed", "", 1.0),
        ]
    )
    calls: list[tuple[object, ...]] = []
    backoffs: list[int] = []

    async def fake_run(*args):
        calls.append(args)
        try:
            return next(outcomes)
        except StopIteration:
            raise _DoneError from None

    def fake_backoff(failures):
        backoffs.append(failures)
        return 0.0

    monkeypatch.setattr(connection, "run", fake_run)
    monkeypatch.setattr(runner, "backoff", fake_backoff)
    monkeypatch.setattr(runner, "CONNECT_SPACING", 0.0)

    exchange = Okx()
    with pytest.raises(_DoneError):
        await runner.supervise(exchange, 3, 1, ["A"], fake_sink, runner.Pacer())
    assert backoffs == [1, 2, 1, 1]
    assert len(calls) == 6
    assert calls[0] == (exchange, 3, 1, ["A"], fake_sink)


async def test_heartbeat_touches_only_while_records_flow(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "HEARTBEAT_INTERVAL", 0.05)
    sent = 0
    path = tmp_path / "alive"
    task = asyncio.create_task(runner.heartbeat(lambda: sent, path))
    await asyncio.sleep(0.02)
    assert path.exists()  # touched at start
    first = path.stat().st_mtime_ns
    await asyncio.sleep(0.2)
    assert path.stat().st_mtime_ns == first  # nothing sent: stale
    sent += 1
    await asyncio.sleep(0.1)
    assert path.stat().st_mtime_ns > first
    task.cancel()
