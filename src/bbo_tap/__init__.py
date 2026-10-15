"""bbo-tap: best bid/ask websockets from five crypto exchanges, captured verbatim to Redpanda or Pub/Sub."""

import argparse
import asyncio
import logging
import sys
from importlib.metadata import version
from pathlib import Path

from . import metrics, runner
from .config import EXAMPLE, ConfigError, load
from .sink import SinkError

__version__ = version("bbo-tap")

log = logging.getLogger("bbo_tap")


def main() -> None:
    parser = argparse.ArgumentParser(prog="bbo-tap", description="Exchange websockets -> Redpanda or Pub/Sub")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--example-config", action="store_true", help="print an example config and exit")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--heartbeat", type=Path, help="file touched every 10 s while records flow (for healthchecks)")
    parser.add_argument("--metrics-port", type=int, help="serve Prometheus metrics on this port at /metrics")
    parser.add_argument("--metrics-addr", default="0.0.0.0", help="address the metrics server binds (default 0.0.0.0)")
    args = parser.parse_args()
    if args.example_config:
        print(EXAMPLE.read_text(), end="")
        return

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("websockets").setLevel(logging.WARNING)  # one INFO line per connect otherwise
    # librdkafka logs every failed reconnect (~10/s while Redpanda is down);
    # the sink's error callback already reports that state every 10 s.
    logging.getLogger("librdkafka").setLevel(logging.CRITICAL)

    try:
        cfg = load(args.config)
    except (ConfigError, OSError) as e:
        log.error("config %s: %s", args.config, e)
        sys.exit(2)

    if args.metrics_port is not None:
        metrics.INFO.info({"version": __version__})
        try:
            metrics.serve(args.metrics_port, args.metrics_addr)
        except OSError as e:
            log.error("metrics %s:%d: %s", args.metrics_addr, args.metrics_port, e)
            sys.exit(2)
        log.info("metrics on http://%s:%d/metrics", args.metrics_addr, args.metrics_port)

    try:
        asyncio.run(runner.run(cfg, args.heartbeat))
    except SinkError as e:
        log.error("sink: %s", e)
        sys.exit(1)
