# Binance — captured frames (live, 2026-10-08)

| File | Frame | Notes |
| --- | --- | --- |
| `subscribe_ack.json` | Ack for `{"method":"SUBSCRIBE",...,"id":1}` | Unknown symbols get the same ack |
| `data.json` | `<symbol>@bookTicker` data frame | Arrives together with the ack |
| `error.json` | Reply to a malformed request | Followed by server close `1008 Invalid request` |

Not captured as files (WS control frames): server PING every ≤ 3 min, payload = server ms timestamp as text; the library answers it.

## 30 s check

1. Handshake 101 within 5 s.
2. Exactly one `subscribe_ack` within 5 s of subscribe.
3. Every subscribed symbol has ≥ 1 `data` frame during the run (`stream == "<sym>@bookTicker"`, `data.s == SYM`).
4. No `error` frame, no close before 30 s.
5. Every frame is text JSON matching `subscribe_ack` or `data`; anything else is flagged and saved.

No ping check: the client sends no application ping.
