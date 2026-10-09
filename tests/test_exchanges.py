"""What each venue class sends: the exact requests verified live (spec.md, tests/formats)."""

import json

from bbo_tap.exchanges import EXCHANGES, Binance, Bybit, Gate, Kucoin, Okx


def sent(cls, symbols):
    return [json.loads(m) for m in cls().subscribe_messages(symbols)]


def test_registry():
    assert set(EXCHANGES) == {"binance", "bybit", "okx", "gate", "kucoin"}
    for name, cls in EXCHANGES.items():
        assert cls.name == name
        assert cls.url.startswith("wss://")
        assert cls.max_symbols_per_conn == 25


def test_binance_one_batch_lowercased_no_ping():
    assert sent(Binance, ["BTCUSDT", "ETHUSDT"]) == [
        {"method": "SUBSCRIBE", "params": ["btcusdt@bookTicker", "ethusdt@bookTicker"], "id": 1}
    ]
    assert Binance.ping_interval is None
    assert Binance().ping_message() is None


def test_bybit_one_request_per_symbol_req_id_is_symbol():
    assert sent(Bybit, ["BTCUSDT", "ETHUSDT"]) == [
        {"req_id": "BTCUSDT", "op": "subscribe", "args": ["orderbook.1.BTCUSDT"]},
        {"req_id": "ETHUSDT", "op": "subscribe", "args": ["orderbook.1.ETHUSDT"]},
    ]
    assert Bybit.ping_interval == 20.0
    assert json.loads(Bybit().ping_message()) == {"op": "ping"}


def test_okx_one_batch_text_ping():
    assert sent(Okx, ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]) == [
        {
            "op": "subscribe",
            "args": [
                {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
                {"channel": "bbo-tbt", "instId": "ETH-USDT-SWAP"},
            ],
        }
    ]
    assert ":8443" not in Okx.url
    assert Okx.ping_interval == 20.0
    assert Okx().ping_message() == "ping"


def test_gate_one_request_per_contract_with_size_header():
    msgs = sent(Gate, ["BTC_USDT", "ETH_USDT"])
    assert [(m["id"], m["payload"]) for m in msgs] == [(1, ["BTC_USDT"]), (2, ["ETH_USDT"])]
    assert all(
        m["channel"] == "futures.book_ticker" and m["event"] == "subscribe" and isinstance(m["time"], int) for m in msgs
    )
    assert Gate.headers == {"X-Gate-Size-Decimal": "1"}
    assert Gate.ping_interval == 10.0
    assert Gate.subscribe_interval == 0.1
    ping = json.loads(Gate().ping_message())
    assert ping["channel"] == "futures.ping"
    assert isinstance(ping["time"], int)


def test_kucoin_one_request_per_symbol_ids_in_order():
    msgs = sent(Kucoin, ["XBTUSDTM", "ETHUSDTM"])
    assert msgs[0] == {
        "id": "1",
        "action": "SUBSCRIBE",
        "channel": "obu",
        "tradeType": "FUTURES",
        "symbol": "XBTUSDTM",
        "depth": "1",
        "rpiFilter": 0,
    }
    assert [(m["id"], m["symbol"]) for m in msgs] == [("1", "XBTUSDTM"), ("2", "ETHUSDTM")]
    assert Kucoin.ping_interval == 18.0
    assert Kucoin.subscribe_interval == 0.2
    assert json.loads(Kucoin().ping_message()) == {"op": "ping"}
