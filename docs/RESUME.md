# as resume protocol v1

> 中文摘要：as-shim 與「實作了本協定的伺服器」之間的斷線續傳協定。伺服器把每個串流回應的原始 bytes 暫存起來；連線中途斷掉時，as-shim 用同一個 id 和已收到的 byte 數重新接上，從斷點繼續，Claude Code 完全不會察覺。沒有實作本協定的上游會忽略這些 header，as-shim 的行為與以前相同。

This document describes how `as-shim` (the client) and a resume-capable server cooperate so
that a streamed Anthropic Messages response survives a broken network connection. When the
connection between the client and the server drops in the middle of a response, the client
reconnects and continues from the exact byte where it stopped. Claude Code only talks to
as-shim over loopback and never sees the break.

```
Claude Code ⇄ as-shim (127.0.0.1) ══ internet / Cloudflare ══ resume server ⇄ gateway (e.g. sub2api)
```

The resume server is typically a small reverse proxy that runs next to the Anthropic-compatible
gateway, so the link between them is local and reliable, while the long-haul link (often through
Cloudflare) is the one that may break.

The protocol is opt-in on both sides:

- A server that does not implement it ignores the extra request headers and never sends
  `X-AS-Resume: 1`; as-shim then behaves exactly as before.
- A client that does not send `X-AS-Resume-Id` gets a plain streaming reverse proxy.

Key words: MUST, SHOULD and MAY are used as in RFC 2119.

## 1. Opt-in request

The client adds these headers to every POST that expects an SSE (`text/event-stream`)
response:

| Header | Value |
|---|---|
| `X-AS-Resume-Id` | 32 lowercase hex characters (128 random bits). A **new id for every upstream attempt**, including silent re-sends. |
| `Accept-Encoding` | `identity` (replaces whatever the original client sent; byte offsets always refer to identity bytes) |

**Credential**: the value of `x-api-key` if present, otherwise the token part of
`Authorization: Bearer <token>`. The server keys every entry by
`(sha256_hex(credential), id)`. Resume and delete requests MUST carry the same credential;
otherwise the server answers `404`, indistinguishable from "not found". An id is therefore
useless without the key that created it.

### When the server buffers

The server buffers a response (and makes it resumable) only when **all** of these hold:

- the request has a valid `X-AS-Resume-Id` (exactly 32 hex chars) and a credential;
- the method is `POST`;
- the path is one the server proxies (for example `/v1/messages`);
- the upstream status is `200`;
- the upstream `Content-Type` starts with `text/event-stream`.

It then adds these response headers:

| Header | Meaning |
|---|---|
| `X-AS-Resume: 1` | This response is resumable. |
| `Cache-Control: no-cache, no-transform` | Keeps intermediaries (e.g. Cloudflare) from compressing or altering the bytes. |

Otherwise it passes the response through unchanged and MUST NOT send `X-AS-Resume`.

The server SHOULD also send `Accept-Encoding: identity` to the gateway on buffer-eligible
requests.

If `(keyhash, id)` already exists, the server answers `409` with an Anthropic-style error JSON
(`{"type":"error","error":{...}}`). The client treats this like a failure before any content
and re-sends with a new id.

## 2. Server buffering semantics

- **One upstream reader per entry.** It reads the gateway body to the end and appends every
  byte to the entry. It runs **independently of the client connection**: if the client
  disappears, the reader keeps going until upstream EOF or error. The reader never waits on a
  client writer; writers follow the buffer.
- **Entry states**

  | State | Meaning |
  |---|---|
  | `live` | still reading from the gateway |
  | `complete` | the gateway body ended cleanly |
  | `failed` | upstream I/O error |
  | `overflow` | exceeded the per-entry cap; buffering stopped, resume now returns `410` |

- **The original response body is a cursor over the entry starting at offset 0**, i.e. the
  same mechanism as a resume.
- **Limits** (recommended defaults, configurable):

  | Limit | Default |
  |---|---|
  | per-entry max bytes | 32 MiB |
  | total max bytes | 512 MiB |
  | keep after done | 600 s |
  | max entry age | 3600 s (also enforced while live; the reader is aborted) |

  When the total limit is exceeded, evict the oldest finished entries first. If that is not
  enough, stop buffering new requests (pass them through without `X-AS-Resume`). A live entry
  whose growth would exceed the total limit also evicts finished entries first and otherwise
  switches to `overflow`, so the total limit holds while responses are still streaming. The
  total SHOULD count the memory the buffers really allocate (including per-allocation
  overhead), not only body bytes. Expired entries are swept periodically (e.g. every 30 s).

## 3. Resume

```
GET /__as/resume/{id}?offset={n}
```

with the same credential header as the original request.

| Status | Meaning |
|---|---|
| `200` | Resumable; the body follows |
| `404` | Unknown, expired, deleted, or credential mismatch |
| `410` | The entry overflowed its buffer |
| `416` | `n` is larger than the current length |

A `200` response carries:

| Header | Value |
|---|---|
| `Content-Type` | `text/event-stream` |
| `Cache-Control` | `no-cache, no-transform` |
| `X-AS-Resume-State` | `live`, `complete` or `failed` (at the time the response starts) |
| `X-AS-Resume-Length` | current length of the entry |

The body is the raw bytes `[n..]`, followed by the live tail until the entry is done, and then
it ends cleanly. The bytes are identical to the original response, so the client can splice at
any byte position, even in the middle of an SSE event. Several resume readers may exist at the
same time; each has its own cursor.

## 4. Ack / cancel

```
DELETE /__as/resume/{id}?reason=ack|cancel
```

with the same credential. Response: `204`, or `404`.

| Reason | Effect |
|---|---|
| `ack` | Delete the entry (the client has everything). |
| `cancel` | Delete the entry **and abort the upstream reader**, closing the gateway request. |

## 5. Server path handling (recommendations)

- `/__as/resume/*` MUST be handled by the server itself and never forwarded upstream.
- A server that is exposed to the internet SHOULD forward only the API paths it needs (for
  example `/v1/*`) and answer `404` for everything else, so that admin APIs or web UIs of the
  gateway stay unreachable. That includes `CONNECT` and request targets that are not
  origin-form (absolute-form, authority-form, `*`): an HTTP client library may rewrite them
  (e.g. `CONNECT /v1/x` to authority-form with an empty path) after the path check passed.
- Strip hop-by-hop headers; forward end-to-end headers (including `CF-Connecting-IP`,
  `X-Forwarded-For`) unchanged so the gateway still sees the real client address. Names listed
  in `Connection` are hop-by-hop too, but SHOULD NOT remove headers added by trusted
  intermediaries or needed by the gateway (`Host`, credentials, `Content-*`, `CF-*`,
  `X-Forwarded-*`, `X-Real-IP`, `True-Client-IP`, `Forwarded`, …); otherwise a client could
  send `Connection: cf-connecting-ip, x-forwarded-for` to hide its address.
- Stream request bodies without buffering (with a size limit).
- Requests without `X-AS-Resume-Id` are plain streaming reverse-proxy requests with
  backpressure; a client disconnect cancels the upstream request.

## 6. Client behavior (as-shim)

### Availability

Resume is used for a response if and only if the response had `X-AS-Resume: 1` and the client
config has `resume = true` (the default).

### Tracking

- `offset` counts **raw upstream body bytes** received (identity encoding).
- The client watches the SSE frames as they pass (without altering any byte) and remembers
  whether the terminal event `message_stop` has been seen. as-shim also treats an `error`
  event as terminal for the purpose of trigger 2.
- A terminal event counts only once its frame is **complete**, i.e. its terminating blank line
  has arrived. SSE decoders discard an event that is not terminated when the stream ends, so a
  `message_stop` frame cut before its blank line has not been delivered yet.

### Triggers (only when resume is available)

1. An upstream body read error.
2. A clean upstream EOF before a complete `message_stop` frame was seen (including an EOF
   inside that frame, before its blank line: the server has the missing bytes).
3. **Upstream idle**: no bytes for `resume_idle_secs` (default 45). Anthropic-compatible
   gateways send a keepalive `ping` at least every ~10 s, so silence means a black-holed
   connection (for example lost NAT state without a RST). The client aborts that connection
   and resumes.

### Resume loop

- Keep the downstream (Claude Code) connection open. Never write anything synthetic in the
  middle of a frame.
- Send `GET /__as/resume/{id}?offset={offset}`. The first attempt is immediate; later attempts
  back off 0.5, 1, 2, 4, 8, 8, … s. The server answers the headers at once, so waiting for them
  is bounded by 10 s (or `resume_idle_secs`, if shorter).
- Every resume `GET` (and every ack/cancel `DELETE`) uses a **new connection**, never an idle
  pooled keep-alive connection: the failure that caused the break (a black hole such as lost NAT
  state) usually killed those too, and each one would cost a full timeout.
- **One hard deadline per break**: `resume_idle_secs + resume_budget_secs` (default
  45 + 225 = 270 s) after the last byte the client received, which it forwarded to Claude Code
  at once. Every wait while recovering ends by that deadline: the response headers of a resume
  `GET`, the backoff pauses, and the idle wait on a resumed body that stays silent. When the
  deadline passes, give up. Claude Code therefore never goes longer than
  `resume_idle_secs + resume_budget_secs` without data, which must stay below its own 300 s
  idle watchdog (as-shim warns at startup otherwise). With `resume_idle_secs = 0` the deadline
  is `resume_budget_secs` after the last byte.
- On `200`: append the body bytes downstream, advance the offset, and keep tracking. The same
  triggers apply to the resume stream, so it can be resumed again. Once new bytes arrive the
  episode is over; a later break gets a new deadline, measured from the latest byte.
- If a resume response ends cleanly while `message_stop` was still not seen:
  - `X-AS-Resume-State` describes the entry when the response *started*. If the offset is still
    below that response's `X-AS-Resume-Length`, the body was cut short on the way (same fault
    as trigger 2): resume again, whatever the state.
  - Otherwise, `X-AS-Resume-State: complete` — the upstream itself ended without
    `message_stop`; end the downstream stream cleanly, as a plain proxy would.
  - Otherwise, `X-AS-Resume-State: failed` — give up.
  - `live` — resume again (the entry was still growing).
  - Without `X-AS-Resume-Length` the state alone decides.
- On `404` / `410` / `416` / deadline passed / other errors until the deadline: **give
  up**, i.e. fall back to the behavior without resume. A broken connection (I/O error, idle)
  aborts the downstream connection, and Claude Code shows "Connection lost" and retries itself;
  a clean EOF stays a clean end.

### Pre-content failures

If the response headers had `X-AS-Resume: 1`, a transport failure before any content resumes
instead of re-sending the POST (which would start a second generation). Frames are still held
back until content arrives, exactly as without resume. Failures before response headers
(connect errors etc.) keep the silent re-send, with a new id. Unless the connection was never
established, the failed attempt may already be running on the server, so the client first sends
`DELETE /__as/resume/{old id}?reason=cancel` (fire-and-forget; `404` if the server never saw it
or already dropped it). If resuming gives up before any content, the usual silent re-send rules
apply.

### Non-stream requests

as-shim turns non-stream requests into streams and aggregates the SSE into a Message JSON. The
same resume logic applies while reading that SSE.

### Completion

After `message_stop` has been received and the stream ended, send `DELETE …?reason=ack`
(fire-and-forget, on a new connection, 5 s per attempt). DELETE is idempotent, so an attempt
that fails in transit or gets no answer in time is retried once on another new connection (a
lost `cancel` would leave a generation running).

### Downstream disconnects first

When Claude Code disconnects (Esc, process exit, …), drop the upstream connection and send
`DELETE …?reason=cancel` (fire-and-forget, bounded timeout), so the server stops the
generation. This includes a disconnect while the POST's response headers are still pending:
the server may already have admitted that attempt (and keeps generating independently of the
client connection once it is live), so its id is cancelled too (`404` if the server never saw
it). A client that gives up after `410`, `416`, other errors or a passed deadline also sends
`cancel` (after `404` there is nothing to delete). An attempt whose response was received is
cancelled or acked exactly once, by the code that reads that response.

### Logging

Never log credentials or bodies. Logging the id is fine: it is useless without the key.

## 7. Client configuration (as-shim)

| Key | Default | Meaning |
|---|---|---|
| `resume` | `true` | Send `X-AS-Resume-Id` and resume when the server advertises support |
| `resume_idle_secs` | `45` | Silence that counts as a broken connection (`0` = only I/O errors and EOF) |
| `resume_budget_secs` | `225` | Recovery time on top of `resume_idle_secs`: give up once Claude Code has gone `resume_idle_secs + resume_budget_secs` without a byte (keep the sum below 300) |
