"""30 s full cycle against the real venues (pytest --live [--live-config config.yaml]).

All venues run at once through connection.run, with frames kept in memory
instead of Redpanda. Each test then applies its venue's "30 s check" from
tests/formats/<exchange>/README.md. Run it before deploying and whenever the
symbol list changes: it is the only check that catches a symbol a venue acks
but never streams (Binance, KuCoin).

Quiet coins can go 30 s without a book change, and Binance, Gate and KuCoin
only send on change. Where the ack proves the symbol (Bybit, OKX and Gate
reject unknown ones), silence is just reported; on Binance and KuCoin data is
the only proof, so a silent symbol fails: rerun with a longer --live-duration.
"""

import asyncio
import statistics
import time
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pytest

from bbo_tap import connection
from bbo_tap.config import load
from bbo_tap.exchanges import EXCHANGES
from tests.frames import classify

pytestmark = pytest.mark.live

ACK_WITHIN = 5.0  # after its subscribe message was due
ACK_PROVES_SYMBOL = {"bybit", "okx", "gate"}  # they reject unknown symbols; Binance and KuCoin ack anything
PRICE_AGREE = 0.03  # each venue's mid within 3 % of the cross-venue median
CONNECT_SLACK = 5.0  # handshake


@dataclass
class Capture:
    symbols: list[str]
    start: float = 0.0
    frames: list[tuple[float, str]] = field(default_factory=list)  # (seconds since start, text)
    outcome: connection.Outcome | None = None

    def send(self, exchange: str, partition: int, conn_id: str, seq: int, ts_recv_ns: int, payload: str) -> None:
        self.frames.append((time.monotonic() - self.start, payload))


@dataclass
class LiveRun:
    caps: dict[str, Capture]  # venue -> what it sent
    table: dict[str, dict[str, str]]  # unified -> venue -> native symbol


@pytest.fixture(scope="session")
def duration(request):
    return float(request.config.getoption("--live-duration"))


@pytest.fixture(scope="session")
def live(request, duration):
    cfg = load(request.config.getoption("--live-config"))
    caps = {name: Capture(list(v.symbols[: EXCHANGES[name].max_symbols_per_conn])) for name, v in cfg.venues.items()}

    async def one(name, cap):
        cap.start = time.monotonic()
        cap.outcome = await connection.run(EXCHANGES[name](), 0, 0, cap.symbols, cap)

    async def all_venues():
        rotate = connection.ROTATE_AFTER
        connection.ROTATE_AFTER = duration
        try:
            await asyncio.gather(*(one(n, c) for n, c in caps.items()))
        finally:
            connection.ROTATE_AFTER = rotate

    asyncio.run(all_venues())
    return LiveRun(caps, cfg.symbols)


@pytest.mark.parametrize("name", list(EXCHANGES))
def test_cycle(live, duration, name):
    if name not in live.caps:
        pytest.skip(f"{name} not enabled in the config")
    cap, ex = live.caps[name], EXCHANGES[name]
    frames = [(t, classify(name, text)) for t, text in cap.frames]
    problems = []

    # 5. No close before the end.
    if cap.outcome.reason != "rotate":
        problems.append(f"ended early: {cap.outcome.reason} {cap.outcome.detail}")

    # 6. Every frame is a known kind; no errors.
    for (t, f), (_, text) in zip(frames, cap.frames, strict=True):
        if f.kind in ("unknown", "error"):
            problems.append(f"{f.kind} frame at {t:.1f}s: {text[:200]}")

    # 1. KuCoin: welcome first.
    if name == "kucoin" and (not frames or frames[0][1].kind != "welcome"):
        problems.append("first frame is not welcome")

    # 2. Acks: Binance one for the batch, OKX/Bybit/Gate one per symbol, KuCoin one per request id.
    acks = [(t, f) for t, f in frames if f.kind == "ack"]
    ack_at: dict[str, float] = {}
    if name == "binance":
        if len(acks) != 1:
            problems.append(f"{len(acks)} acks, want 1")
        ack_at = {s: acks[0][0] for s in cap.symbols} if acks else {}
    else:
        id_to_symbol = {str(i + 1): s for i, s in enumerate(cap.symbols)}
        per_symbol: defaultdict[str | None, list[float]] = defaultdict(list)
        for t, f in acks:
            per_symbol[f.symbol or (id_to_symbol.get(f.ref) if f.ref else None)].append(t)
        for s in cap.symbols:
            if len(per_symbol[s]) != 1:
                problems.append(f"{s}: {len(per_symbol[s])} acks, want 1")
            elif per_symbol[s][0] > CONNECT_SLACK + ex.subscribe_interval * cap.symbols.index(s) + ACK_WITHIN:
                problems.append(f"{s}: ack at {per_symbol[s][0]:.1f}s, too late")
            if per_symbol[s]:
                ack_at[s] = per_symbol[s][0]

    # 3. Data for every symbol (any time: quiet books send nothing until they change).
    first_data: dict[str, float] = {}
    for t, f in frames:
        if f.kind == "data" and f.symbol:
            first_data.setdefault(f.symbol, t)
            if name == "gate" and not (isinstance(f.msg["result"]["B"], str) and isinstance(f.msg["result"]["A"], str)):
                problems.append("gate sizes are not strings: X-Gate-Size-Decimal not applied")
                break
    silent = [s for s in cap.symbols if s not in first_data]
    if silent and name in ACK_PROVES_SYMBOL:
        print(f"\n{name}: acked but no book change in {duration:.0f}s: {silent}")
    elif silent:
        problems += [
            f"{s}: no data in {duration:.0f}s (wrong symbol, or quiet: try a longer --live-duration)" for s in silent
        ]
    unexpected = set(first_data) - set(cap.symbols)
    if unexpected:
        problems.append(f"data for symbols not subscribed: {sorted(unexpected)}")

    # 4. Pongs: one per app ping sent (none for Binance).
    pongs = sum(1 for _, f in frames if f.kind == "pong")
    if ex.ping_interval:
        pings = int((duration - CONNECT_SLACK) // ex.ping_interval)
        if pongs < pings:
            problems.append(f"{pongs} pongs, want ≥ {pings}")
    elif pongs:
        problems.append(f"{pongs} pongs but no app ping is sent")

    counts: defaultdict[str, int] = defaultdict(int)
    for _, f in frames:
        counts[f.kind] += 1
    print(
        f"\n{name}: {dict(counts)}; first data per symbol: "
        + ", ".join(f"{s} {first_data.get(s, -1):.1f}s" for s in cap.symbols)
    )
    assert not problems, "\n".join(problems)


def _mid(name: str, m: Mapping[str, Any]) -> float | None:
    """Mid price from a data frame; None if a side is empty."""
    if name == "binance":
        bid, ask = m["data"]["b"], m["data"]["a"]
    elif name == "bybit":
        bid, ask = (m["data"]["b"] or [[None]])[0][0], (m["data"]["a"] or [[None]])[0][0]
    elif name == "okx":
        book = m["data"][0]
        bid, ask = (book["bids"] or [[None]])[0][0], (book["asks"] or [[None]])[0][0]
    elif name == "gate":
        bid, ask = m["result"]["b"], m["result"]["a"]
    else:
        bid, ask = (m["d"]["b"] or [[None]])[0][0], (m["d"]["a"] or [[None]])[0][0]
    if not bid or not ask:
        return None
    return (float(bid) + float(ask)) / 2


def test_prices_agree_across_venues(live):
    """Same unified symbol = same coin: catches a ticker that means different tokens on different venues."""
    mids: defaultdict[str, dict[str, float]] = defaultdict(dict)  # unified -> venue -> last mid
    native_to_unified = {
        (venue, native): unified for unified, row in live.table.items() for venue, native in row.items()
    }
    for name, cap in live.caps.items():
        for _, text in cap.frames:
            f = classify(name, text)
            if f.kind == "data" and (mid := _mid(name, f.msg)) is not None:
                mids[native_to_unified[name, f.symbol]][name] = mid
    problems = []
    for unified, by_venue in sorted(mids.items()):
        median = statistics.median(by_venue.values())
        off = {v: m for v, m in by_venue.items() if abs(m / median - 1) > PRICE_AGREE}
        print(
            f"{unified:12} median {median:<12.6g} "
            + " ".join(f"{v}:{m / median - 1:+.2%}" for v, m in by_venue.items())
        )
        if off:
            problems.append(f"{unified}: {off} vs median {median}")
    assert not problems, "\n".join(problems)
