import json

from .base_class import Exchange


class Bybit(Exchange):
    """Bybit V5 linear `orderbook.1.{symbol}`."""

    name = "bybit"
    url = "wss://stream.bybit.com/v5/public/linear"
    ping_interval = 20.0

    def subscribe_messages(self, symbols: list[str]) -> list[str]:
        # One request per symbol: an unknown symbol fails its whole request.
        # req_id = symbol, so each ack/error names the symbol it belongs to.
        return [json.dumps({"req_id": s, "op": "subscribe", "args": [f"orderbook.1.{s}"]}) for s in symbols]

    def ping_message(self) -> str:
        return json.dumps({"op": "ping"})
