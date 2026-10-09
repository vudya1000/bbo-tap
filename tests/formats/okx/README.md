# OKX — captured frames (live, 2026-10-08)

| File | Frame | Notes |
| --- | --- | --- |
| `subscribe_ack.json` | One ack **per instrument** in the subscribe request | No `id` unless the request had one |
| `subscribe_error.json` | Unknown instrument (code `60018`) | Only that instrument fails; the others in the same request are acked and stream. No `arg` field: the instId appears only inside `msg`. Connection stays open |
| `error.json` | Malformed request / unknown `op` (code `60012`) | Connection stays open |
| `pong.txt` | Reply to text `ping` | Plain text, not JSON |
| `data.json` | `bbo-tbt` snapshot | Sizes are decimal contracts (`"621.77"`); ~70–80 frames/s per instrument measured |

Not seen in a 30 s run: `{"event":"notice","code":"64008",...}` (sent 60 s before a service-upgrade disconnect).

## 30 s check

1. Handshake 101 within 5 s.
2. One `subscribe_ack` per instrument (`arg.instId == SYM`) within 5 s; no `"event":"error"` frame at any time.
3. Every instrument has ≥ 1 `data` frame during the run (`arg.instId == SYM`). A quiet book may send none: the ack already proves the symbol, so silence is reported, not failed.
4. Ping sent at 20 s gets `pong` within 5 s.
5. No close before 30 s.
6. Every frame is `pong` or JSON matching `subscribe_ack` or `data`; anything else is flagged and saved.
