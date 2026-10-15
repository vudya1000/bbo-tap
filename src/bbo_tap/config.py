"""Config: the sink (`redpanda:` or `pubsub:`), per-venue settings, and the symbol table.

The symbol table maps each unified symbol (`BTC-USDT`) to its venue-native
names. Venue subscribe lists are built from it. Everything is checked at load
time, so a bad file stops the process before any connection is opened.
"""

import dataclasses
import os
from collections.abc import Hashable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .exchanges import EXCHANGES

EXAMPLE = Path(__file__).with_name("config.example.yaml")  # shipped in the package; `bbo-tap --example-config`


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Kafka:
    brokers: tuple[str, ...]
    topic: str
    symbols_topic: str


@dataclass(frozen=True)
class PubSub:
    project: str | None  # None: GOOGLE_CLOUD_PROJECT or the credentials' project, found at startup
    topic: str
    symbols_topic: str
    endpoint: str | None  # e.g. asia-northeast1-pubsub.googleapis.com:443; None: the global endpoint


@dataclass(frozen=True)
class Venue:
    partition: int | None  # Kafka partition of the raw topic; None with Pub/Sub
    symbols: tuple[str, ...]  # venue-native, in symbol-table order


@dataclass(frozen=True)
class Config:
    sink: Kafka | PubSub
    venues: dict[str, Venue]  # enabled venues only
    symbols: dict[str, dict[str, str]]  # unified -> {venue: venue-native}, as written; goes to symbols_topic


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate keys instead of keeping the last one."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Hashable, Any]:
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise ConfigError(f"duplicate key {key!r} at line {key_node.start_mark.line + 1}")
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def load(path: str | Path) -> Config:
    """Load and check the file. BBO_TAP_BROKERS (comma-separated), if set, replaces redpanda.brokers,
    so one symbol config serves every environment (e.g. redpanda:29092 inside compose)."""
    with open(path) as f:
        cfg = parse(yaml.load(f, Loader=_StrictLoader) or {})
    if brokers := os.environ.get("BBO_TAP_BROKERS"):
        if not isinstance(cfg.sink, Kafka):
            raise ConfigError("BBO_TAP_BROKERS is set, but the config has no redpanda section")
        sink = dataclasses.replace(cfg.sink, brokers=_strings(brokers.split(","), "BBO_TAP_BROKERS"))
        cfg = dataclasses.replace(cfg, sink=sink)
    return cfg


def parse(raw: Any) -> Config:
    _expect_keys(raw, "config", required={"exchanges", "symbols"}, optional=frozenset({"redpanda", "pubsub"}))
    sink: Kafka | PubSub
    if ("redpanda" in raw) == ("pubsub" in raw):
        raise ConfigError("config: needs exactly one of redpanda or pubsub")
    if "redpanda" in raw:
        redpanda = raw["redpanda"]
        _expect_keys(redpanda, "redpanda", required={"brokers"}, optional=frozenset({"topic", "symbols_topic"}))
        sink = Kafka(
            brokers=_strings(redpanda["brokers"], "redpanda.brokers"),
            topic=_name(redpanda.get("topic", "raw.ws"), "redpanda.topic"),
            symbols_topic=_name(redpanda.get("symbols_topic", "ref.symbols"), "redpanda.symbols_topic"),
        )
    else:
        pubsub = raw["pubsub"]
        _expect_keys(
            pubsub, "pubsub", required=set(), optional=frozenset({"project", "topic", "symbols_topic", "endpoint"})
        )
        sink = PubSub(
            project=_name(pubsub["project"], "pubsub.project") if "project" in pubsub else None,
            topic=_name(pubsub.get("topic", "raw-ws"), "pubsub.topic"),
            symbols_topic=_name(pubsub.get("symbols_topic", "ref-symbols"), "pubsub.symbols_topic"),
            endpoint=_name(pubsub["endpoint"], "pubsub.endpoint") if "endpoint" in pubsub else None,
        )
    kafka = isinstance(sink, Kafka)

    exchanges = raw["exchanges"]
    if not isinstance(exchanges, dict) or not exchanges:
        raise ConfigError("exchanges: must be a mapping of venue name -> settings")
    unknown = exchanges.keys() - EXCHANGES.keys()
    if unknown:
        raise ConfigError(f"exchanges: unknown venue(s) {sorted(unknown)}, known: {sorted(EXCHANGES)}")
    partitions: dict[int, str] = {}
    for name, venue in exchanges.items():
        # One raw-topic partition per venue with Kafka; Pub/Sub orders by an ordering key instead.
        if kafka:
            _expect_keys(venue, f"exchanges.{name}", required={"enabled", "partition"})
        else:
            _expect_keys(venue, f"exchanges.{name}", required={"enabled"})
        if not isinstance(venue["enabled"], bool):
            raise ConfigError(f"exchanges.{name}.enabled: must be true or false")
        if not kafka:
            continue
        p = venue["partition"]
        if not isinstance(p, int) or isinstance(p, bool) or p < 0:
            raise ConfigError(f"exchanges.{name}.partition: must be an integer >= 0")
        if p in partitions:
            raise ConfigError(f"exchanges.{name}.partition: {p} already used by {partitions[p]}")
        partitions[p] = name

    table = raw["symbols"]
    if not isinstance(table, dict) or not table:
        raise ConfigError("symbols: must be a non-empty mapping of unified symbol -> {venue: symbol}")
    symbols: dict[str, dict[str, str]] = {}
    owner: dict[tuple[str, str], str] = {}  # (venue, venue symbol) -> unified symbol
    for unified, row in table.items():
        _name(unified, f"symbols: key {unified!r}")
        where = f"symbols.{unified}"
        if not isinstance(row, dict) or not row:
            raise ConfigError(f"{where}: must be a non-empty mapping of venue -> symbol")
        not_listed = row.keys() - exchanges.keys()
        if not_listed:
            raise ConfigError(f"{where}: venue(s) {sorted(not_listed)} not listed under exchanges")
        for venue, native in row.items():
            _name(native, f"{where}.{venue}")
            if (venue, native) in owner:
                raise ConfigError(f"{where}.{venue}: {native} already mapped to {owner[venue, native]}")
            owner[venue, native] = unified
        symbols[unified] = dict(row)

    venues = {}
    for name, venue in exchanges.items():
        if not venue["enabled"]:
            continue
        venue_symbols = tuple(row[name] for row in symbols.values() if name in row)
        if not venue_symbols:
            raise ConfigError(f"exchanges.{name}: enabled but no symbol in the table maps to it")
        venues[name] = Venue(partition=venue.get("partition"), symbols=venue_symbols)
    if not venues:
        raise ConfigError("exchanges: no venue is enabled")

    return Config(sink=sink, venues=venues, symbols=symbols)


def _expect_keys(value: Any, where: str, required: set[str], optional: frozenset[str] = frozenset()) -> None:
    if not isinstance(value, dict):
        raise ConfigError(f"{where}: must be a mapping")
    missing = required - value.keys()
    if missing:
        raise ConfigError(f"{where}: missing {sorted(missing)}")
    extra = value.keys() - required - optional
    if extra:
        raise ConfigError(f"{where}: unknown key(s) {sorted(extra)}")


def _name(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}: must be a non-empty string")
    return value


def _strings(value: Any, where: str) -> tuple[str, ...]:
    """Non-empty list of distinct, non-empty strings."""
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{where}: must be a non-empty list")
    for v in value:
        _name(v, where)
    dupes = sorted({v for v in value if value.count(v) > 1})
    if dupes:
        raise ConfigError(f"{where}: duplicates {dupes}")
    return tuple(value)
