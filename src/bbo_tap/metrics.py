"""Prometheus metrics, served on /metrics by `bbo-tap --metrics-port`.

Labels are venue and shard, never conn_id (new on every connect) or symbol
(frames are not parsed). The default registry also carries the client's
process and Python collectors (CPU, RSS, open fds, GC).
"""

from prometheus_client import Counter, Gauge, Info, start_http_server

INFO = Info("bbo_tap", "Build information")
FRAMES = Counter("bbo_tap_frames", "Frames received", ["exchange", "shard"])
LAST_FRAME = Gauge(
    "bbo_tap_last_frame_timestamp_seconds", "Wall clock of the last frame received", ["exchange", "shard"]
)
CONNECTED = Gauge("bbo_tap_connected", "1 while the shard's websocket is open", ["exchange", "shard"])
DISCONNECTS = Counter(
    "bbo_tap_disconnects",
    "Connection attempts that ended, by reason (rotate, idle, closed, connect_failed)",
    ["exchange", "reason"],
)
RECORDS = Counter("bbo_tap_records_produced", "Records handed to the sink client (Kafka producer or Pub/Sub publisher)")
QUEUE = Gauge("bbo_tap_producer_queue_messages", "Records waiting in the sink client's local queue")
SEND_BLOCKED = Counter("bbo_tap_send_blocked_seconds", "Time reading was paused because the local queue was full")

REASONS = ("rotate", "idle", "closed", "connect_failed")


def serve(port: int, addr: str) -> int:
    """Serve /metrics from a daemon thread and return the bound port; OSError if it can't be bound."""
    server, _ = start_http_server(port, addr)
    return server.server_port
