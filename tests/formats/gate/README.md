# Gate — captured frames (live, 2026-10-08)

| File | Frame | Notes |
| --- | --- | --- |
| `subscribe_ack.json` | Ack for one subscribe request | Echoes `id` and `payload` (this sample was a 3-symbol batch; we now send one symbol per request) |
| `subscribe_error.json` | Unknown contract (code 2) | **Fails the whole request it is in**, so we send one request per symbol; others keep streaming. Connection stays open |
| `error.json` | Not JSON (code 1); unknown channel gives code 2 `"Unknown channel …"` | Connection stays open |
| `pong.json` | Reply to `futures.ping` | `result: null` |
| `data.json` | `futures.book_ticker` update, **with** `X-Gate-Size-Decimal: 1` | `B`/`A` are strings; `u` is a number; ~40–60 frames/s per contract measured |
| `data_no_size_header.json` | Same **without** the header | `B`/`A` are ints: proves the header is applied |

## 30 s check

1. Handshake 101 within 5 s.
2. One `subscribe_ack` per contract (`result.status == "success"`, `payload == [SYM]`) within 5 s; no `"status":"fail"` frame at any time.
3. Every contract has ≥ 1 `data` frame during the run (`result.s == SYM`), with `B`/`A` as strings. A quiet book may send none: the ack already proves the symbol, so silence is reported, not failed.
4. Each ping (every 10 s) gets one `pong` within 5 s.
5. No close before 30 s.
6. Every frame is text JSON matching `subscribe_ack`, `pong` or `data`; anything else is flagged and saved.
