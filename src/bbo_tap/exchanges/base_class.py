"""Base class for exchange connectors.

A connector only describes a venue: where to connect and what to send.
Connecting, keepalive timing, liveness, reconnecting and producing to
Redpanda belong to the runner and are the same for every venue.
"""

from abc import ABC, abstractmethod
from typing import ClassVar


class Exchange(ABC):
    name: ClassVar[str]
    url: ClassVar[str]
    headers: ClassVar[dict[str, str]] = {}
    ping_interval: ClassVar[float | None] = None  # seconds; None = no application ping
    subscribe_interval: ClassVar[float] = 0.0  # pause between subscribe messages
    max_symbols_per_conn: ClassVar[int] = 25

    @abstractmethod
    def subscribe_messages(self, symbols: list[str]) -> list[str]:
        """Exact text frames to send after open, in order."""

    def ping_message(self) -> str | None:
        """Application ping, built fresh each time (some carry a timestamp).

        Its reply is not inspected: the runner's "no frame for 30 s" rule and
        the websocket library's protocol pings cover liveness.
        """
        return None
