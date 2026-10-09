"""The captured formats in tests/formats/ classify as documented.

Keeps the classifier used by the live check honest against real frames.
"""

import json
from pathlib import Path

import pytest

from tests.frames import classify

FORMATS = Path(__file__).parent / "formats"

CASES = {
    "binance/subscribe_ack.json": ("ack", None),
    "binance/data.json": ("data", "BTCUSDT"),
    "binance/error.json": ("error", None),
    "bybit/subscribe_ack.json": ("ack", None),  # captured before req_id = symbol
    "bybit/subscribe_error.json": ("error", None),
    "bybit/error.json": ("error", None),
    "bybit/pong.json": ("pong", None),
    "bybit/data.json": ("data", "BTCUSDT"),
    "okx/subscribe_ack.json": ("ack", "BTC-USDT-SWAP"),
    "okx/subscribe_error.json": ("error", None),
    "okx/error.json": ("error", None),
    "okx/pong.txt": ("pong", None),
    "okx/data.json": ("data", "BTC-USDT-SWAP"),
    "gate/subscribe_ack.json": ("ack", None),  # 3-symbol batch, so no single symbol
    "gate/subscribe_error.json": ("error", "FOOXYZ_USDT"),
    "gate/error.json": ("error", None),
    "gate/pong.json": ("pong", None),
    "gate/data.json": ("data", "BTC_USDT"),
    "gate/data_no_size_header.json": ("data", "BTC_USDT"),
    "kucoin/welcome.json": ("welcome", None),
    "kucoin/subscribe_ack.json": ("ack", None),
    "kucoin/error.json": ("error", None),
    "kucoin/pong.json": ("pong", None),
    "kucoin/data.json": ("data", "XBTUSDTM"),
}


def test_every_capture_is_covered():
    on_disk = {str(p.relative_to(FORMATS)) for p in FORMATS.glob("*/*") if p.suffix in (".json", ".txt")}
    assert on_disk == set(CASES)


@pytest.mark.parametrize(("path", "expected"), CASES.items())
def test_capture_classifies(path, expected):
    exchange = path.split("/")[0]
    frame = classify(exchange, (FORMATS / path).read_text().strip())
    assert (frame.kind, frame.symbol) == expected


def test_kucoin_ack_and_error_carry_request_id():
    assert classify("kucoin", (FORMATS / "kucoin/subscribe_ack.json").read_text()).ref == "1"
    assert classify("kucoin", (FORMATS / "kucoin/error.json").read_text()).ref == "9"


def test_gate_size_header_turns_sizes_into_strings():
    with_header = json.loads((FORMATS / "gate/data.json").read_text())["result"]
    without = json.loads((FORMATS / "gate/data_no_size_header.json").read_text())["result"]
    assert isinstance(with_header["B"], str)
    assert isinstance(with_header["A"], str)
    assert isinstance(without["B"], int)
    assert isinstance(without["A"], int)


@pytest.mark.parametrize("exchange", ["binance", "bybit", "okx", "gate", "kucoin"])
def test_garbage_is_unknown(exchange):
    assert classify(exchange, "not json").kind == "unknown"
    assert classify(exchange, '{"foo":1}').kind == "unknown"
