# §A.8 conformance vectors — from the FROZEN §A-v3 contract

Source: brain `ops-specs/specs/2026-06-17-mcp-shim-ph2-3-acceptor-protocol` (§A-v3 FROZEN, a2a-core-pm @2e795b2), handed to dev-pm 2026-06-17. Opus already executed #3/#4 under Bun → MATCH. The shim MUST reproduce #3 (PoP) and #4 (canon-sign) FIRST, before any other build work.

> ⚠️ BYTE-EXACTNESS: #4a contains edge chars (`|`, `\`, `"`, non-ASCII `ünï`, emoji `🎯`, and a LITERAL newline). The authoritative byte-exact vector files ship in a2a-core-pm's `tests/shim-conformance/` — use those for the Rust test fixtures; the strings below are the spec's human representation (verify byte-for-byte against a2a-core-pm's files).

## Identity (TEST seed — NOT a real identity)
- `seed_hex` = `000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f` (the 32 bytes 0x00..0x1f)
- `pubkey_raw_b64` = `A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=` (importEd25519Pub for verify)

## #3 — PoP (hello auth)
- `nonce_b64` = `//79/Pv6+fj39vX08/Lx8O/u7ezr6uno5+bl5OPi4eA=` (decodes to 32 bytes 0xff..0xe0)
- Procedure: base64-DECODE the nonce → exactly 32 bytes → **ed25519-sign those 32 raw bytes** → base64.
- EXPECTED `sig_b64` = `FKJzdLTgP9ZC137K+bnUbp5CfXVzFN6Y5yy691hrJFv+sCzOcFTkPauJLld7gD7z5zWD25KFbuRuSPCB6zX6Aw==`
- NEGATIVE (load-bearing): signing the base64 STRING (instead of the decoded 32 bytes) → verify FALSE. The decode-then-sign-32 pin MUST be tested.

## #4 — canon-sign (shim-signs CTRL{sign,canon} → CTRL{sig})
The shim signs the **UTF-8 bytes of the DECODED `canon` string VERBATIM** (ed25519 → b64). The shim MUST NOT canonicalize (canonical() is core-side TS only). `alg`/`thread`/`attrs` are EXCLUDED from canon; `|` and `\` inside the body are escaped `\|` and `\\`.

### #4a (msg + edge chars; contains a literal LF)
canon (the `<LF>` token below is a LITERAL newline byte 0x0A):
```
1|a2a-shim-vec-0001|a2a-shim-test|topic:agent-beat|msg||2026-06-17T00:00:00.000Z||pipe\|backslash\\quote"unicode ünï 🎯<LF>nl
```
EXPECTED `sig_b64` = `EkrVgMA4FSlu/Qto+xcYiWB5Y9unXHzeLVpLT7BtnH24SLHAgxmaGBafF6HwA4r23SYRrjCyaZu0iQMpU6xPDQ==`

### #4b (reply + corr + ttl_ms + attrs-EXCLUDED + empty body)
canon:
```
1|a2a-shim-vec-0002|a2a-shim-test|agent-x|reply|a2a-shim-vec-0001|2026-06-17T00:00:00.000Z|60000|
```
EXPECTED `sig_b64` = `9I84hXpfAsG2OVJHHeU4IsRgQEy5q1Arq+tLN49XfQ2Tt9Sz11Swq2yvR/peSEvoHS59ORxdqlcfKy5KcO8HBg==`

## #5 — initialize byte-parity (core-side; shim relays)
Golden = CAPTURED from webhook.ts initialize output (`capabilities.experimental.{'claude/channel':{}}`, `serverInfo`, `instructions = baseInstructions + A2AChannel.INSTRUCTIONS + BrainTools.INSTRUCTIONS + KaiTools.INSTRUCTIONS`, webhook.ts:30-40). The shim relays verbatim — no shim-side assertion beyond pass-through.

## Scenario vectors (executable, both sides — from a2a-core-pm's tests/shim-conformance/)
#1 framing (partial / oversize / unknown-type / len<1); #2 hello happy + each err + hello-timeout; #6 sign-DURING-tools/call → no deadlock; #7 reconnect → no duplicate initialize to claude; #8 lifecycle race (old-conn close after new epoch); #9 timeout-retry → no double-publish; #10 durable-inbox: kill mid-inject → redelivered (not ACKed-lost) after reconnect.
