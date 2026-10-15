# bbo-tap

[![CI](https://github.com/vudya1000/bbo-tap/actions/workflows/ci.yml/badge.svg)](https://github.com/vudya1000/bbo-tap/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/bbo-tap)](https://pypi.org/project/bbo-tap/)
[![Python](https://img.shields.io/pypi/pyversions/bbo-tap)](https://pypi.org/project/bbo-tap/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://github.com/vudya1000/bbo-tap/blob/main/LICENSE)

Best bid/ask websockets from **Binance, Bybit, OKX, Gate and KuCoin** USDT perpetuals, captured
**verbatim** to a Kafka-API topic (Redpanda, Kafka) or a Google Cloud Pub/Sub topic. One small asyncio process; no parsing, no
normalization: every received frame is one record, so whatever you build downstream (SQL,
stream processing) works on exactly what the exchanges sent.

- Explicit symbol table: one unified name (`BTC-USDT`) mapped to each venue's own symbol
- One topic partition per exchange (Pub/Sub: one ordering key per exchange), each in receive order
- Reconnects with backoff, proactive 12 h reconnect, idle detection, fast clean shutdown
- Fails loudly: exits non-zero when the broker is unreachable, never drops frames silently

## Install

```sh
pip install bbo-tap            # or: uv tool install bbo-tap / uvx bbo-tap
pip install 'bbo-tap[pubsub]'  # with the Pub/Sub sink
```

Or the container, which includes both sinks: `docker pull ghcr.io/vudya1000/bbo-tap`.

## Quick start

```sh
bbo-tap --example-config > config.yaml     # edit brokers and symbols
bbo-tap --config config.yaml
```

The broker needs the topics described in [Output](#output) first, e.g. with Redpanda's `rpk`:

```sh
rpk topic create raw.ws -p 5 -c retention.ms=604800000   # 7 days, ~8 GB/day
rpk topic create ref.symbols -p 1 -c cleanup.policy=compact
```

## Config

```yaml
redpanda:
  brokers: [localhost:9092]
  topic: raw.ws               # optional, default raw.ws
  symbols_topic: ref.symbols  # optional, default ref.symbols
exchanges:                    # every venue listed keeps its partition, enabled or not
  binance: {enabled: true, partition: 0}
  bybit:   {enabled: true, partition: 1}
  okx:     {enabled: true, partition: 2}
  gate:    {enabled: true, partition: 3}
  kucoin:  {enabled: true, partition: 4}
symbols:                      # unified name -> each venue's own symbol; leave a venue out if not listed there
  BTC-USDT: {binance: BTCUSDT, bybit: BTCUSDT, okx: BTC-USDT-SWAP, gate: BTC_USDT, kucoin: XBTUSDTM}
  ETH-USDT: {binance: ETHUSDT, bybit: ETHUSDT, okx: ETH-USDT-SWAP, gate: ETH_USDT, kucoin: ETHUSDTM}
```

Symbols are mapped explicitly, never derived by a naming rule: KuCoin calls BTC `XBT`, and
contracts like `1000PEPE` differ between venues. The file is checked at startup and rejected on an
unknown venue, a row naming a venue not listed under `exchanges`, two venues on one partition, a
venue symbol used twice, an enabled venue with no symbols, or duplicate YAML keys.

`BBO_TAP_BROKERS` (comma-separated) overrides `redpanda.brokers`, so one symbol list serves every
environment.

### Pub/Sub instead of Redpanda

Replace the `redpanda:` section with `pubsub:` and drop `partition` from the exchanges:

```yaml
pubsub:                       # every key is optional
  project: my-project         # default: GOOGLE_CLOUD_PROJECT, else the credentials' project (on GKE: the cluster's)
  topic: raw-ws               # default raw-ws
  symbols_topic: ref-symbols  # default ref-symbols
  endpoint: asia-northeast1-pubsub.googleapis.com:443   # default: the global endpoint
exchanges:
  binance: {enabled: true}
  okx:     {enabled: true}
symbols:
  ETH-USDT: {binance: ETHUSDT, okx: ETH-USDT-SWAP}
```

- **Messages**: `raw-ws` gets the same JSON record as `raw.ws` (see [Output](#output)), with
  **ordering key = exchange**: a subscription with message ordering enabled receives each venue's
  frames in receive order, like one Kafka partition per venue. `ref-symbols` gets one message per
  unified symbol: data `{"binance":"ETHUSDT",…}`, attribute `symbol` = `ETH-USDT`.
- **Credentials**: Application Default Credentials (Workload Identity on GKE,
  `gcloud auth application-default login` locally). `PUBSUB_EMULATOR_HOST` points it at the emulator.
- **Permissions** on both topics: `roles/pubsub.publisher`, and `roles/pubsub.viewer` for the
  startup check that they exist.
- **Ordering across regions**: Pub/Sub keeps order only among messages published to the same region.
  Set `endpoint` to a regional endpoint so every publish goes to one region.
- **Same delivery rules** as Redpanda: at least once; up to 100k messages / 128 MB queue locally,
  then reading pauses; a publish failing for 30 s (or failing outright, e.g. topic deleted) exits
  with code 1. Batches: up to 1000 messages or 1 MB, 50 ms.

## Output

Every received frame (data, acks, errors, pongs, notices) becomes one record on `raw.ws`:

```json
{"exchange":"okx","conn_id":"okx-0-1759820000131","seq":918273,"ts_recv_ns":1759820000131402000,"payload":"<exact frame text>"}
```

| Field | Meaning |
| --- | --- |
| `exchange` | Venue name; also the record key |
| `conn_id` | `{exchange}-{shard}-{connect_unix_ms}`, new on every connect |
| `seq` | 0, 1, 2 … per `conn_id`: a gap is a lost frame, a repeat is a harmless retry |
| `ts_recv_ns` | Wall clock right after the socket read (run NTP); also the Kafka timestamp, in ms |
| `payload` | The frame text, unmodified (KuCoin's binary frames decoded as UTF-8) |

- **`raw.ws`**: one partition per exchange, set from config (not hashed), so each partition is one
  venue in receive order. Recommended retention: 7 days, set on the topic (`retention.ms`) rather than
  left to the cluster default; add `retention.bytes` as a cap if disk is tight.
- **`ref.symbols`** (compacted): written on every start, one record per unified symbol, key
  `BTC-USDT`, value `{"binance":"BTCUSDT","bybit":"BTCUSDT",…}`. Join on it to map venue symbols
  to unified ones; frames themselves are never tagged.
- **Delivery is at least once.** Every data frame on all five venues is a full top-of-book
  snapshot, not a delta, so the latest frame per (exchange, symbol) is the state at that time:
  duplicates don't matter, take the latest `ts_recv_ns`. Producer: `acks=1`, no idempotence,
  zstd, `linger.ms=50`.
- **Loss is never silent.** Frames queue locally (up to 512 MB) while the broker is unreachable;
  when the queue is full, reading pauses instead of dropping. After 30 s without delivery the
  process exits with code 1. At startup it checks that both topics exist and `raw.ws` has every
  configured partition.

## Exchanges

| Venue | Stream | Subscribe | App ping |
| --- | --- | --- | --- |
| Binance USDⓈ-M | `<symbol>@bookTicker` on `/public/stream` | One batch | None (server pings) |
| Bybit V5 linear | `orderbook.1.<symbol>` | One request per symbol | `{"op":"ping"}` / 20 s |
| OKX V5 SWAP | `bbo-tbt` | One batch | `ping` / 20 s |
| Gate USDT futures | `futures.book_ticker`, decimal sizes header | One request per symbol | `futures.ping` / 10 s |
| KuCoin futures (Pro, no token) | `obu` depth 1, binary frames | One request per symbol, 5/s | `{"op":"ping"}` / 18 s |

Bybit and Gate fail a whole request when one symbol in it is unknown, hence one request per
symbol. Binance and KuCoin accept unknown symbols silently, which is why the live check exists
(see [Development](#development)).

**Connections**: up to 25 symbols each; reconnect on socket error, server close, failed protocol
ping, or 30 s without any frame; backoff 1 s doubling to 60 s with full jitter, at most one new
connection per second per venue; proactive reconnect every 12 h (the short gap is accepted).
Frames/s per connection is logged every 60 s.

## Running in production

```sh
docker run -d --name bbo-tap \
  -e BBO_TAP_BROKERS=redpanda:9092 \
  -v ./config.yaml:/etc/bbo-tap/config.yaml:ro \
  -p 9464:9464 \
  --memory 1g --stop-timeout 20 --restart unless-stopped \
  --log-opt max-size=10m --log-opt max-file=3 \
  ghcr.io/vudya1000/bbo-tap:latest
```

- **Exit codes**: 0 stopped by SIGTERM/SIGINT (sockets dropped at once, broker flushed), 1 broker
  failure, 2 bad config or metrics port unavailable. Run it under something that restarts on non-zero.
- **Health**: `--heartbeat FILE` touches the file every 10 s only while records flow; the image's
  healthcheck fails when it is older than 60 s (alive but stuck, e.g. every connection down).
- **Logs** go to stdout. Docker's default `json-file` driver never rotates, so cap it (`--log-opt`
  above, or in the daemon or compose config).
- **Metrics**: see [Metrics](#metrics); the image serves them on port 9464.
- **Resources** (measured, 25 symbols × 5 venues, ~1.8k frames/s): ~17 % of one core, ~60 MB RSS,
  ~1 TB/month inbound traffic, ~8 GB/day in the broker after compression. Allow 1 GB of memory for
  the outage queue.

## Metrics

`--metrics-port PORT` (and `--metrics-addr`, default `0.0.0.0`) serves Prometheus metrics at
`/metrics`. Off by default; the container image turns it on at 9464. Labels are venue and shard,
never `conn_id` or symbol.

| Metric | Type | Meaning |
| --- | --- | --- |
| `bbo_tap_frames_total{exchange,shard}` | counter | Frames received |
| `bbo_tap_last_frame_timestamp_seconds{exchange,shard}` | gauge | Wall clock of the last frame |
| `bbo_tap_connected{exchange,shard}` | gauge | 1 while the websocket is open |
| `bbo_tap_disconnects_total{exchange,reason}` | counter | Connections ended: `rotate`, `idle`, `closed`, `connect_failed` |
| `bbo_tap_records_produced_total` | counter | Records handed to the producer (Kafka) or publisher (Pub/Sub) |
| `bbo_tap_producer_queue_messages` | gauge | Local queue depth (grows while the broker or Pub/Sub is unreachable) |
| `bbo_tap_send_blocked_seconds_total` | counter | Time reading was paused on a full queue |
| `bbo_tap_info{version}` | info | Running version |

Plus the client library's `process_*` (CPU, RSS, fds; Linux only) and `python_*` metrics.

Example alerts:

```yaml
- alert: BboTapStale            # a shard has had no frame for a minute
  expr: time() - bbo_tap_last_frame_timestamp_seconds > 60
- alert: BboTapFlapping         # unplanned reconnects
  expr: sum by (exchange) (increase(bbo_tap_disconnects_total{reason!="rotate"}[15m])) > 5
- alert: BboTapQueueBuilding    # broker slow or unreachable; the process exits after 30 s of no delivery
  expr: bbo_tap_producer_queue_messages > 50000
- alert: BboTapDown
  expr: up{job="bbo-tap"} == 0
```

## Development

```sh
uv sync
uv run ruff format . && uv run ruff check . && uv run mypy
uv run pytest                     # offline, plus broker tests if Redpanda is on localhost:9092
                                  # and the Pub/Sub emulator on localhost:8085 (else skipped)
uv run pytest --live -s           # 30 s against the real exchanges
uv run pytest --live --live-config config.yaml --live-duration 120   # your symbol list; quiet coins need longer
```

The Pub/Sub emulator:

```sh
docker run -d --name pubsub -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:587.0.0-emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=bbo-tap-test
```

The live check verifies, per venue: acks, at least one data frame per symbol, pongs, no error
frames, no early close, and mids agreeing across venues (catches a ticker meaning different coins
on different venues). It needs network access to the exchanges, several of which block US IPs,
so it is not part of CI.

`tests/formats/<exchange>/` holds captured frames of every kind, with notes; the classifier the
live check uses is tested against them.

### Releasing

1. Run the live check from a machine the exchanges accept (not CI):
   `uv run pytest --live --live-config <your config> --live-duration 120`.
2. `uv version --bump patch` (or `minor`), commit, then
   `git tag v$(uv version --short) && git push origin main --tags`.
3. The release workflow checks the tag against the version, runs the checks, builds, publishes to
   TestPyPI, waits for approval in the `pypi` environment, publishes to PyPI, pushes
   `ghcr.io/vudya1000/bbo-tap:<version>` and `:latest` (amd64 + arm64), and creates a GitHub release.

## License

MIT
