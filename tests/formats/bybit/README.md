# Bybit — captured frames (live, 2026-10-08)

| File | Frame | Notes |
| --- | --- | --- |
| `subscribe_ack.json` | Ack for one subscribe request | `ret_msg` is `""` (docs show `"subscribe"`); `req_id` echoes ours (= symbol) |
| `subscribe_error.json` | Subscribe with an unknown symbol | Fails the whole request it is in, so we send one request per symbol; other symbols keep streaming. Connection stays open |
| `error.json` | Unknown `op` / malformed JSON | `success:false`, reason in `ret_msg`. Connection stays open |
| `pong.json` | Reply to `{"op":"ping"}` | `op` is `"ping"`, `ret_msg` is `"pong"` |
| `data.json` | `orderbook.1.<symbol>` | Always `type:"snapshot"`, one level per side; ~25–50 frames/s per symbol measured |

## 30 s check

1. Handshake 101 within 5 s.
2. Exactly one `subscribe_ack` per symbol (`success:true`, `op:"subscribe"`, `req_id == SYM`) within 5 s; no `success:false` frame at any time (its `req_id` names the bad symbol).
3. Every subscribed symbol has ≥ 1 `data` frame during the run (`topic == "orderbook.1.<SYM>"`, `data.s == SYM`). A quiet book may send none: the ack already proves the symbol, so silence is reported, not failed.
4. Ping sent at 20 s gets exactly one `pong` within 5 s.
5. No close before 30 s.
6. Every frame is text JSON matching `subscribe_ack`, `pong` or `data`; anything else is flagged and saved.
