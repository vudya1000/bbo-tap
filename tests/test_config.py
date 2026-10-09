import copy
from typing import Any

import pytest

from bbo_tap.config import EXAMPLE, ConfigError, load, parse

GOOD: dict[str, Any] = {
    "redpanda": {"brokers": ["b:9092"]},
    "exchanges": {"okx": {"enabled": True, "partition": 2}, "gate": {"enabled": False, "partition": 3}},
    "symbols": {
        "BTC-USDT": {"okx": "BTC-USDT-SWAP", "gate": "BTC_USDT"},
        "PEPE-USDT": {"okx": "PEPE-USDT-SWAP"},
    },
}


def test_example_config_loads():
    cfg = load(EXAMPLE)
    assert set(cfg.venues) == {"binance", "bybit", "okx", "gate", "kucoin"}
    assert sorted(v.partition for v in cfg.venues.values()) == [0, 1, 2, 3, 4]
    assert cfg.venues["kucoin"].symbols == ("XBTUSDTM", "ETHUSDTM")
    assert cfg.topic == "raw.ws"
    assert cfg.symbols_topic == "ref.symbols"


def test_venue_symbols_come_from_table_and_skip_disabled():
    cfg = parse(GOOD)
    assert list(cfg.venues) == ["okx"]
    assert cfg.venues["okx"].partition == 2
    assert cfg.venues["okx"].symbols == ("BTC-USDT-SWAP", "PEPE-USDT-SWAP")
    assert cfg.symbols == GOOD["symbols"]  # whole table, disabled venues included, for ref.symbols


def _set(path, value):
    def edit(c):
        *keys, last = path
        for k in keys:
            c = c[k]
        if value is _DELETE:
            del c[last]
        else:
            c[last] = value

    return edit


_DELETE = object()


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (_set(["exchanges", "ftx"], {"enabled": True, "partition": 9}), "unknown venue"),
        (_set(["exchanges", "gate", "partition"], 2), "already used by okx"),
        (_set(["exchanges", "gate", "partition"], True), "integer >= 0"),
        (_set(["exchanges", "gate", "partition"], -1), "integer >= 0"),
        (_set(["exchanges", "gate", "partition"], _DELETE), "missing ['partition']"),
        (_set(["exchanges", "okx", "enabled"], "yes"), "true or false"),
        (_set(["exchanges", "okx", "symbol"], ["X"]), "unknown key(s) ['symbol']"),
        (_set(["exchanges", "okx", "enabled"], False), "no venue is enabled"),
        (_set(["symbols", "BTC-USDT", "bybit"], "BTCUSDT"), "not listed under exchanges"),
        (_set(["symbols", "PEPE-USDT", "okx"], "BTC-USDT-SWAP"), "already mapped to BTC-USDT"),
        (_set(["symbols", "X-USDT"], {}), "non-empty mapping"),
        (_set(["symbols", "BTC-USDT", "okx"], 5), "non-empty string"),
        (_set(["symbols"], _DELETE), "missing ['symbols']"),
        (_set(["redpanda", "brokers"], "b:9092"), "non-empty list"),
        (_set(["redpanda", "brokers"], ["a", "a"]), "duplicates"),
        (_set(["redpanda", "topic"], ""), "non-empty string"),
    ],
)
def test_rejected(edit, message):
    raw = copy.deepcopy(GOOD)
    edit(raw)
    with pytest.raises(
        ConfigError, match=message.replace("[", r"\[").replace("]", r"\]").replace("(", r"\(").replace(")", r"\)")
    ):
        parse(raw)


def test_enabled_venue_without_symbols_rejected():
    raw = copy.deepcopy(GOOD)
    raw["exchanges"]["gate"]["enabled"] = True
    del raw["symbols"]["BTC-USDT"]["gate"]
    with pytest.raises(ConfigError, match="no symbol in the table maps to it"):
        parse(raw)


def test_duplicate_yaml_key_rejected(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(
        "redpanda: {brokers: [b]}\n"
        "exchanges:\n  okx: {enabled: true, partition: 0}\n"
        "symbols:\n  BTC-USDT: {okx: A}\n  BTC-USDT: {okx: B}\n"
    )
    with pytest.raises(ConfigError, match="duplicate key 'BTC-USDT' at line 6"):
        load(path)


def test_brokers_env_override(monkeypatch):
    monkeypatch.setenv("BBO_TAP_BROKERS", "redpanda:29092,other:29092")
    assert load(EXAMPLE).brokers == ("redpanda:29092", "other:29092")


def test_brokers_env_override_rejects_garbage(monkeypatch):
    monkeypatch.setenv("BBO_TAP_BROKERS", "a,,b")
    with pytest.raises(ConfigError, match="BBO_TAP_BROKERS"):
        load(EXAMPLE)
