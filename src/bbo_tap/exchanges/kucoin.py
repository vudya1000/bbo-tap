import json

from .base_class import Exchange


class Kucoin(Exchange):
    """KuCoin futures Pro WS, `obu` depth 1 (order-book snapshot per change).

    No token needed. Frames arrive as binary websocket frames holding UTF-8
    JSON. `obu` rejects a batched `symbols` list, so one subscribe per symbol.
    """

    name = "kucoin"
    url = "wss://x-push-futures.kucoin.com"
    ping_interval = 18.0  # welcome frame announces pingInterval 18000 ms
    subscribe_interval = 0.2

    def subscribe_messages(self, symbols: list[str]) -> list[str]:
        return [
            json.dumps(
                {
                    "id": str(i + 1),
                    "action": "SUBSCRIBE",
                    "channel": "obu",
                    "tradeType": "FUTURES",
                    "symbol": s,
                    "depth": "1",
                    "rpiFilter": 0,
                }
            )
            for i, s in enumerate(symbols)
        ]

    def ping_message(self) -> str:
        return json.dumps({"op": "ping"})
