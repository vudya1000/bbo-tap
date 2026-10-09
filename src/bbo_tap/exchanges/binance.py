import json

from .base_class import Exchange


class Binance(Exchange):
    """Binance USDⓈ-M `<symbol>@bookTicker`.

    No application ping: the server sends WS pings every 3 min and the
    websocket library answers them.
    """

    name = "binance"
    url = "wss://fstream.binance.com/public/stream"

    def subscribe_messages(self, symbols: list[str]) -> list[str]:
        params = [f"{s.lower()}@bookTicker" for s in symbols]
        return [json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1})]
