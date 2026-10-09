from .base_class import Exchange
from .binance import Binance
from .bybit import Bybit
from .gate import Gate
from .kucoin import Kucoin
from .okx import Okx

EXCHANGES: dict[str, type[Exchange]] = {cls.name: cls for cls in (Binance, Bybit, Okx, Gate, Kucoin)}

__all__ = ["EXCHANGES", "Binance", "Bybit", "Exchange", "Gate", "Kucoin", "Okx"]
