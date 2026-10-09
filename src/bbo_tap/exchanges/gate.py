import json
import time
from typing import ClassVar

from .base_class import Exchange


class Gate(Exchange):
    """Gate USDT futures `futures.book_ticker`.

    Needs X-Gate-Size-Decimal, otherwise sizes arrive as ints.
    """

    name = "gate"
    url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    headers: ClassVar[dict[str, str]] = {"X-Gate-Size-Decimal": "1"}
    ping_interval = 10.0
    subscribe_interval = 0.1

    def subscribe_messages(self, symbols: list[str]) -> list[str]:
        # One request per symbol: an unknown symbol fails its whole request.
        # The ack echoes `payload`, so each ack/error names its symbol.
        now = int(time.time())
        return [
            json.dumps(
                {
                    "time": now,
                    "id": i + 1,
                    "channel": "futures.book_ticker",
                    "event": "subscribe",
                    "payload": [s],
                }
            )
            for i, s in enumerate(symbols)
        ]

    def ping_message(self) -> str:
        return json.dumps({"time": int(time.time()), "channel": "futures.ping"})
