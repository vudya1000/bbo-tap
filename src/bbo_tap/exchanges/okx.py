import json

from .base_class import Exchange


class Okx(Exchange):
    """OKX V5 SWAP `bbo-tbt`. Port 443; `:8443` stops working on 2026-10-31."""

    name = "okx"
    url = "wss://ws.okx.com/ws/v5/public"
    ping_interval = 20.0  # server drops links idle > 30 s

    def subscribe_messages(self, symbols: list[str]) -> list[str]:
        args = [{"channel": "bbo-tbt", "instId": s} for s in symbols]
        return [json.dumps({"op": "subscribe", "args": args})]

    def ping_message(self) -> str:
        return "ping"
