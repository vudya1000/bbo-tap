# KuCoin (Pro WS) — captured frames (live, 2026-10-08)

**All frames are binary** websocket frames with UTF-8 JSON; files hold the decoded text.

| File | Frame | Notes |
| --- | --- | --- |
| `welcome.json` | First frame after connect | Subscribing before it arrives also works |
| `subscribe_ack.json` | Ack for one subscribe | Echoes our `id`. **Unknown symbols are acked `result:true` silently**: only a data frame proves a symbol |
| `error.json` | Unknown `action` or `channel` | `result:false`. Non-JSON input gets **no reply at all**. Connection stays open |
| `pong.json` | Reply to `{"op":"ping"}` | `id` is server-generated, `ts` in ns |
| `data.json` | `obu` depth-1 snapshot | Sizes are integer lots as strings; ~60–225 frames/s per symbol measured |

## 30 s check

1. Handshake 101 within 5 s; first frame is `welcome`.
2. One `subscribe_ack` (`result:true`) per symbol, matched by `id`, within 5 s; no `result:false` at any time.
3. Every symbol has ≥ 1 `data` frame during the run (`d.s == SYM`). This is the only check that catches a wrong symbol.
4. Ping at 18 s gets one `pong` within 5 s.
5. No close before 30 s.
6. Every frame is binary, decodes to JSON matching `welcome`, `subscribe_ack`, `pong` or `data`; anything else is flagged and saved.
