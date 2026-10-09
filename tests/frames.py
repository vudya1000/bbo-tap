"""Classify venue frames, for tests only: the ingestor itself never reads frames.

Rules follow the captures in tests/formats/<exchange>/.
"""

import json
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, NamedTuple

Json = Mapping[str, Any]
_EMPTY: Json = MappingProxyType({})


class Frame(NamedTuple):
    kind: str  # welcome | ack | error | pong | data | notice | unknown
    symbol: str | None = None  # data: its symbol; ack/error: the symbol, when the frame names it
    ref: str | None = None  # ack/error: request id, when that is the only link (KuCoin)
    msg: Json = _EMPTY  # the parsed frame


def classify(exchange: str, text: str) -> Frame:
    if exchange == "okx" and text == "pong":
        return Frame("pong")
    try:
        m = json.loads(text)
    except ValueError:
        return Frame("unknown")
    if not isinstance(m, dict):
        return Frame("unknown")
    return _RULES[exchange](m)


def _binance(m: Json) -> Frame:
    if "stream" in m:
        return Frame("data", m["data"]["s"], msg=m)
    if "error" in m:
        return Frame("error", msg=m)
    if "result" in m and m["result"] is None and "id" in m:
        return Frame("ack", ref=str(m["id"]), msg=m)
    return Frame("unknown", msg=m)


def _bybit(m: Json) -> Frame:
    if m.get("topic", "").startswith("orderbook.1."):
        return Frame("data", m["data"]["s"], msg=m)
    if m.get("op") == "ping" and m.get("ret_msg") == "pong":
        return Frame("pong", msg=m)
    if m.get("success") is False:
        return Frame("error", m.get("req_id") or None, msg=m)
    if m.get("op") == "subscribe" and m.get("success") is True:
        return Frame("ack", m.get("req_id") or None, msg=m)
    return Frame("unknown", msg=m)


def _okx(m: Json) -> Frame:
    event = m.get("event")
    if event == "subscribe":
        return Frame("ack", m["arg"]["instId"], msg=m)
    if event == "error":
        return Frame("error", msg=m)
    if event == "notice":
        return Frame("notice", msg=m)
    if "data" in m and m.get("arg", {}).get("channel") == "bbo-tbt":
        return Frame("data", m["arg"]["instId"], msg=m)
    return Frame("unknown", msg=m)


def _gate(m: Json) -> Frame:
    payload = m.get("payload") or []
    one = payload[0] if len(payload) == 1 else None
    if m.get("error"):
        return Frame("error", one, msg=m)
    if m.get("channel") == "futures.pong":
        return Frame("pong", msg=m)
    if m.get("channel") == "futures.book_ticker":
        if m.get("event") == "subscribe" and (m.get("result") or {}).get("status") == "success":
            return Frame("ack", one, msg=m)
        if m.get("event") == "update":
            return Frame("data", m["result"]["s"], msg=m)
    return Frame("unknown", msg=m)


def _kucoin(m: Json) -> Frame:
    if m.get("message") == "welcome":
        return Frame("welcome", msg=m)
    if m.get("op") == "pong":
        return Frame("pong", msg=m)
    if m.get("result") is True:
        return Frame("ack", ref=m.get("id"), msg=m)
    if m.get("result") is False:
        return Frame("error", ref=m.get("id"), msg=m)
    if m.get("T") == "obu.FUTURES":
        return Frame("data", m["d"]["s"], msg=m)
    return Frame("unknown", msg=m)


_RULES = {"binance": _binance, "bybit": _bybit, "okx": _okx, "gate": _gate, "kucoin": _kucoin}
