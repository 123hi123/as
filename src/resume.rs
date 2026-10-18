//! Client side of the as resume protocol v1 (see `docs/RESUME.md`).
//!
//! Every resumable upstream POST carries a fresh random `X-AS-Resume-Id`. When the response
//! advertises `X-AS-Resume: 1`, [`UpstreamBody`] counts the raw body bytes and, if the
//! connection breaks before `message_stop` (I/O error, clean EOF, or silence for
//! `resume_idle_secs`), fetches `GET /__as/resume/{id}?offset=N` and continues the same byte
//! stream. Consumers only ever see upstream bytes, in order, without gaps or duplicates.

use crate::logdb::ResumeEventRow;
use crate::proxy::{describe_reqwest_error, AppState, ReqLog};
use crate::sse::{Frame, SseParser};
use crate::{log_debug, log_info, log_warn};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue};
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const RESUME_ID_HEADER: &str = "x-as-resume-id";
pub const RESUME_HEADER: &str = "x-as-resume";
pub const RESUME_STATE_HEADER: &str = "x-as-resume-state";
pub const RESUME_LENGTH_HEADER: &str = "x-as-resume-length";

/// Pause before the 2nd, 3rd, ... resume attempt of one break (the first is immediate).
const BACKOFF_MS: &[u64] = &[500, 1000, 2000, 4000, 8000];
/// Upper bound for the response headers of one resume GET; the server sends them at once.
const RESUME_HEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound for one attempt of a fire-and-forget ack/cancel DELETE.
const DELETE_TIMEOUT: Duration = Duration::from_secs(5);
/// The frame tracker stops tracking a single frame larger than this.
const MAX_TRACK_FRAME: usize = 64 << 20;

/// 128-bit random id as 32 lowercase hex chars.
pub fn new_resume_id() -> Option<String> {
    let mut b = [0u8; 16];
    getrandom::getrandom(&mut b).ok()?;
    Some(b.iter().map(|x| format!("{x:02x}")).collect())
}

pub fn is_valid_id(s: &str) -> bool {
    s.len() == 32
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The credential headers of an upstream request, re-sent verbatim on resume / delete.
pub fn credential_headers(h: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for name in [HeaderName::from_static("x-api-key"), header::AUTHORIZATION] {
        for v in h.get_all(&name) {
            out.append(name.clone(), v.clone());
        }
    }
    out
}

/// Watches the SSE frames passing by (without changing them) for content and terminal events.
#[derive(Default)]
pub struct StopTracker {
    parser: SseParser,
    /// `message_stop` seen.
    stop: bool,
    /// `message_stop` or an `error` event seen: nothing follows on a healthy stream.
    terminal: bool,
    /// A content event (`content_block_*`, `message_delta`, `message_stop`) seen.
    content: bool,
}

impl StopTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, chunk: &[u8]) {
        if self.stop {
            return;
        }
        self.parser.push(chunk);
        while let Some(f) = self.parser.next_frame() {
            self.observe(&f);
        }
        if self.parser.pending_len() > MAX_TRACK_FRAME {
            // Give up on this frame; tracking resynchronises at the next blank line.
            self.parser = SseParser::new();
        }
    }

    fn observe(&mut self, f: &Frame) {
        let Some(data) = f.data.as_deref() else {
            return;
        };
        // After content only terminal events matter; skip JSON parsing for the bulk of deltas.
        if self.content && !data.contains("message_stop") && !data.contains("error") {
            return;
        }
        let json = f.json();
        let Some(kind) = f.kind(json.as_ref()) else {
            return;
        };
        if crate::aggregate::is_content_event(&kind) {
            self.content = true;
        }
        match kind.as_str() {
            "message_stop" => {
                self.stop = true;
                self.terminal = true;
            }
            "error" => self.terminal = true,
            _ => {}
        }
    }

    /// A complete terminal frame (`message_stop` or `error`, through its blank line) has passed.
    /// A terminal frame cut before that blank line does not count: SSE decoders drop an
    /// unterminated event at EOF, so the missing bytes still have to be fetched.
    fn terminal_seen(&self) -> bool {
        self.terminal
    }

    pub fn stop_seen(&self) -> bool {
        self.stop
    }

    pub fn content_seen(&self) -> bool {
        self.content
    }
}

#[derive(Clone)]
enum Trigger {
    Io(String),
    Eof,
    Idle(Duration),
}

impl Trigger {
    fn label(&self) -> &'static str {
        match self {
            Trigger::Io(_) => "io_error",
            Trigger::Eof => "eof_no_stop",
            Trigger::Idle(_) => "idle",
        }
    }

    fn describe(&self) -> String {
        match self {
            Trigger::Io(e) => format!("upstream I/O error ({e})"),
            Trigger::Eof => "upstream ended before a complete message_stop".into(),
            Trigger::Idle(d) => format!("no upstream bytes for {:.1}s", d.as_secs_f32()),
        }
    }
}

/// What to tell the server when this body is done with.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Fin {
    /// Still streaming (decided on drop: ack after `message_stop`, otherwise cancel).
    Open,
    /// Finished; `DELETE ?reason=ack` (already sent when `released`).
    Done,
    /// Entry is gone (404); nothing to delete.
    Gone,
    /// Gave up or abandoned; `DELETE ?reason=cancel`.
    Abandon,
}

/// One break-recovery episode: from the first trigger until bytes flow again (or giving up).
struct Break {
    /// Hard end of the episode: `last_rx + resume_idle + resume_budget`. Every wait while
    /// recovering (resume headers, backoff, a silent resumed body) ends by then, so Claude Code
    /// never goes longer than `resume_idle + resume_budget` without a byte.
    deadline: Instant,
    trigger: Trigger,
    backoff: usize,
}

struct Resumable {
    st: Arc<AppState>,
    log: ReqLog,
    id: String,
    creds: HeaderMap,
    offset: u64,
    tracker: StopTracker,
    /// When the last upstream byte arrived (or the response headers, before any byte). Bytes
    /// are handed on at once, so this is also when Claude Code last heard from us.
    last_rx: Instant,
    /// Set while recovering from a break; cleared as soon as bytes flow again.
    broken: Option<Break>,
    /// `X-AS-Resume-State` of the resume response currently being read.
    server_state: Option<String>,
    /// `X-AS-Resume-Length` of that response: the entry length when it started.
    server_len: Option<u64>,
    resumed: u32,
    fin: Fin,
    released: bool,
}

/// An upstream SSE body. Without resume it is the plain byte stream; with resume it heals
/// breaks transparently and yields an error only when resuming gave up.
pub struct UpstreamBody {
    inner: BoxStream<'static, reqwest::Result<Bytes>>,
    rs: Option<Box<Resumable>>,
}

impl UpstreamBody {
    /// `id` is the `X-AS-Resume-Id` that was sent (None when resume is off); resume is used only
    /// when the response advertised `X-AS-Resume: 1`.
    pub(crate) fn new(
        st: &Arc<AppState>,
        resp: reqwest::Response,
        id: Option<String>,
        req_headers: &HeaderMap,
        log: &ReqLog,
    ) -> Self {
        let advertised = resp
            .headers()
            .get(RESUME_HEADER)
            .is_some_and(|v| v.as_bytes().trim_ascii() == b"1");
        let rs = match id {
            Some(id) if advertised && st.resume => {
                log.update(|r| r.resume_id = Some(id.clone()));
                Some(Box::new(Resumable {
                    st: st.clone(),
                    log: log.clone(),
                    id,
                    creds: credential_headers(req_headers),
                    offset: 0,
                    tracker: StopTracker::new(),
                    last_rx: Instant::now(),
                    broken: None,
                    server_state: None,
                    server_len: None,
                    resumed: 0,
                    fin: Fin::Open,
                    released: false,
                }))
            }
            _ => None,
        };
        UpstreamBody {
            inner: resp.bytes_stream().boxed(),
            rs,
        }
    }

    pub fn is_resumable(&self) -> bool {
        self.rs.is_some()
    }

    /// Next chunk of upstream bytes; `None` at the end, `Err` when the stream is broken for good.
    pub async fn next(&mut self) -> Option<Result<Bytes, String>> {
        let UpstreamBody { inner, rs } = self;
        let Some(rs) = rs.as_deref_mut() else {
            return inner
                .next()
                .await
                .map(|r| r.map_err(|e| describe_reqwest_error(&e)));
        };
        if rs.fin != Fin::Open {
            return None;
        }
        loop {
            let wait = match &rs.broken {
                // Reading a resumed body: its idle wait also ends at the episode deadline.
                Some(brk) => {
                    let left = brk.deadline.saturating_duration_since(Instant::now());
                    Some(rs.st.resume_idle.map_or(left, |d| d.min(left)))
                }
                None => rs.st.resume_idle,
            };
            let item = match wait {
                Some(d) => tokio::time::timeout(d, inner.next()).await.ok(),
                None => Some(inner.next().await),
            };
            let trigger = match item {
                Some(Some(Ok(b))) => {
                    if b.is_empty() {
                        continue;
                    }
                    rs.last_rx = Instant::now();
                    rs.offset += b.len() as u64;
                    rs.tracker.push(&b);
                    if rs.broken.take().is_some() {
                        log_info!(
                            "resume {}: stream continues at offset {}",
                            rs.id,
                            rs.offset - b.len() as u64
                        );
                    }
                    return Some(Ok(b));
                }
                Some(Some(Err(e))) => Trigger::Io(describe_reqwest_error(&e)),
                Some(None) => {
                    if rs.tracker.terminal_seen() {
                        rs.finish_ok();
                        return None;
                    }
                    // The state is a snapshot from when the resume response started: it is final
                    // only if everything the server had by then arrived. A clean end short of
                    // that length means the body was cut on the way.
                    let short_of = rs.server_len.filter(|&len| rs.offset < len);
                    match (rs.server_state.as_deref(), short_of) {
                        (Some("complete"), None) => {
                            log_warn!("resume {}: server finished without a complete message_stop; ending the stream", rs.id);
                            rs.release(Fin::Done);
                            return None;
                        }
                        (Some("failed"), None) => {
                            log_warn!(
                                "resume {}: server reports the upstream failed; giving up",
                                rs.id
                            );
                            rs.release(Fin::Done);
                            return Some(Err("upstream failed (resume state: failed)".into()));
                        }
                        (Some(state @ ("complete" | "failed")), Some(len)) => {
                            log_info!(
                                "resume {}: resume body ended at offset {} of {len} (state {state})",
                                rs.id,
                                rs.offset
                            );
                            Trigger::Eof
                        }
                        _ => Trigger::Eof,
                    }
                }
                None => Trigger::Idle(wait.unwrap_or_default()),
            };
            // Drop the broken (possibly black-holed) connection right away.
            *inner = futures_util::stream::empty().boxed();
            match rs.recover(trigger).await {
                Ok(stream) => *inner = stream,
                Err(out) => return out,
            }
        }
    }
}

impl Resumable {
    fn event(&self, attempt: u32, trigger: &'static str, result: String, error: Option<String>) {
        let ev = ResumeEventRow {
            resume_id: self.id.clone(),
            ts_ms: crate::util::unix_ms(),
            attempt,
            offset: self.offset,
            trigger,
            result,
            error: error.map(|e| crate::errmap::truncate(&e, 300)),
        };
        self.log.update(|r| r.events.push(ev));
    }

    fn url(&self) -> String {
        format!("{}/__as/resume/{}", self.st.upstream, self.id)
    }

    /// The stream reached its natural end: ack, and note a successful recovery.
    fn finish_ok(&mut self) {
        if self.resumed > 0 {
            self.log.update(|r| r.resume_recovered = true);
        }
        self.release(Fin::Done);
    }

    fn release(&mut self, fin: Fin) {
        self.fin = fin;
        let reason = match fin {
            Fin::Open | Fin::Gone => return,
            Fin::Done => "ack",
            Fin::Abandon => "cancel",
        };
        if self.released {
            return;
        }
        self.released = true;
        spawn_delete(
            self.st.fresh_client.clone(),
            format!("{}?reason={reason}", self.url()),
            self.creds.clone(),
            self.id.clone(),
        );
    }

    /// Re-attach after a break. `Err` carries what the consumer gets when resuming gives up.
    async fn recover(
        &mut self,
        trigger: Trigger,
    ) -> Result<BoxStream<'static, reqwest::Result<Bytes>>, Option<Result<Bytes, String>>> {
        let again = self.broken.is_some();
        let deadline = episode_deadline(
            self.last_rx,
            self.st.resume_idle.unwrap_or_default(),
            self.st.resume_budget,
        );
        let deadline = self
            .broken
            .get_or_insert_with(|| Break {
                deadline,
                trigger: trigger.clone(),
                backoff: 0,
            })
            .deadline;
        let label = if self.tracker.content_seen() {
            trigger.label()
        } else {
            "pre_content"
        };
        let cause = trigger.describe();
        log_info!(
            "resume {}: {} at offset {} ({label}); resuming",
            self.id,
            cause,
            self.offset
        );
        if again {
            // The previous resume stream broke before delivering anything: back off first.
            self.backoff(deadline).await;
        }
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let attempts = self.log.update(|r| r.resume_attempts);
                self.event(attempts, label, "budget".into(), Some(cause.clone()));
                return Err(self.give_up("resume budget exhausted", Fin::Abandon));
            }
            let wait = RESUME_HEADER_TIMEOUT
                .min(self.st.resume_idle.unwrap_or(RESUME_HEADER_TIMEOUT))
                .min(remaining);
            let attempt = self.log.update(|r| {
                r.resume_attempts += 1;
                r.resume_attempts
            });
            // Always a new connection: the break that brought us here (black hole, NAT loss)
            // usually killed the idle pooled ones too, and each would cost a full `wait`.
            let req = self
                .st
                .fresh_client
                .get(format!("{}?offset={}", self.url(), self.offset))
                .headers(self.creds.clone())
                .header(
                    header::ACCEPT_ENCODING,
                    HeaderValue::from_static("identity"),
                )
                .send();
            match tokio::time::timeout(wait, req).await {
                Ok(Ok(resp)) => {
                    let code = resp.status().as_u16();
                    let usable = crate::proxy::is_sse(resp.headers())
                        && !crate::proxy::is_encoded(resp.headers());
                    if code == 200 && !usable {
                        // Never splice something that is not the original byte stream.
                        log_info!("resume {}: attempt {attempt} got a non-SSE 200", self.id);
                        self.event(
                            attempt,
                            label,
                            "http_200".into(),
                            Some(format!("not an identity event stream; {cause}")),
                        );
                    } else if code == 200 {
                        let state = resp
                            .headers()
                            .get(RESUME_STATE_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.trim().to_ascii_lowercase());
                        log_info!(
                            "resume {}: attempt {attempt} at offset {} ok (state {})",
                            self.id,
                            self.offset,
                            state.as_deref().unwrap_or("?")
                        );
                        self.event(attempt, label, "ok".into(), Some(cause.clone()));
                        self.server_state = state;
                        self.server_len = resp
                            .headers()
                            .get(RESUME_LENGTH_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.trim().parse().ok());
                        self.resumed += 1;
                        return Ok(resp.bytes_stream().boxed());
                    }
                    if code != 200 {
                        log_info!("resume {}: attempt {attempt} got HTTP {code}", self.id);
                        self.event(attempt, label, format!("http_{code}"), Some(cause.clone()));
                    }
                    match code {
                        404 => {
                            return Err(self.give_up("resume entry not found (HTTP 404)", Fin::Gone))
                        }
                        410 => {
                            return Err(
                                self.give_up("resume buffer overflowed (HTTP 410)", Fin::Abandon)
                            )
                        }
                        416 => {
                            return Err(
                                self.give_up("resume offset out of range (HTTP 416)", Fin::Abandon)
                            )
                        }
                        _ => {}
                    }
                }
                Ok(Err(e)) => {
                    let msg = describe_reqwest_error(&e);
                    log_info!("resume {}: attempt {attempt} failed: {msg}", self.id);
                    self.event(attempt, label, "io_error".into(), Some(msg));
                }
                Err(_) => {
                    let msg = format!("no response within {}s", wait.as_secs_f32());
                    log_info!("resume {}: attempt {attempt} failed: {msg}", self.id);
                    self.event(attempt, label, "io_error".into(), Some(msg));
                }
            }
            self.backoff(deadline).await;
        }
    }

    async fn backoff(&mut self, deadline: Instant) {
        let Some(brk) = self.broken.as_mut() else {
            return;
        };
        let ms = BACKOFF_MS[brk.backoff.min(BACKOFF_MS.len() - 1)];
        brk.backoff += 1;
        let d = Duration::from_millis(ms).min(deadline.saturating_duration_since(Instant::now()));
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
    }

    /// Fall back to what happened before resume existed: a clean EOF stays a clean end,
    /// anything else aborts the client connection.
    fn give_up(&mut self, why: &str, fin: Fin) -> Option<Result<Bytes, String>> {
        let trigger = self
            .broken
            .take()
            .map(|b| b.trigger)
            .unwrap_or(Trigger::Eof);
        log_warn!(
            "resume {}: giving up at offset {}: {why} (after {})",
            self.id,
            self.offset,
            trigger.describe()
        );
        self.log.update(|r| {
            r.error = Some(crate::errmap::truncate(
                &format!("{}; {why}", trigger.describe()),
                300,
            ))
        });
        self.release(fin);
        match trigger {
            Trigger::Eof => None,
            t => Some(Err(format!("{}; {why}", t.describe()))),
        }
    }
}

impl Drop for Resumable {
    fn drop(&mut self) {
        if self.fin == Fin::Open {
            if self.tracker.stop_seen() {
                // e.g. PATH N stops reading right after message_stop.
                self.finish_ok();
            } else {
                // Claude Code went away (or the consumer stopped early): stop the generation.
                log_debug!("resume {}: abandoned at offset {}", self.id, self.offset);
                self.release(Fin::Abandon);
            }
        }
    }
}

/// End of a break-recovery episode whose last byte arrived at `last_rx`: Claude Code may go
/// `idle + budget` without a byte, never longer (`idle` is zero when idle detection is off).
fn episode_deadline(last_rx: Instant, idle: Duration, budget: Duration) -> Instant {
    let max_silence = idle.saturating_add(budget);
    last_rx
        .checked_add(max_silence)
        .unwrap_or_else(|| last_rx + Duration::from_secs(10 * 365 * 86_400))
}

/// A POST carrying `id` ended before its response headers arrived (`why`): cancel that
/// attempt (fire-and-forget). Harmless (404) when the server never saw it.
pub(crate) fn cancel_unanswered(st: &AppState, id: &str, req_headers: &HeaderMap, why: &str) {
    log_debug!("resume {id}: {why} before response headers; cancelling the attempt");
    spawn_delete(
        st.fresh_client.clone(),
        format!("{}/__as/resume/{id}?reason=cancel", st.upstream),
        credential_headers(req_headers),
        id.to_string(),
    );
}

/// Armed while a POST carrying a resume id waits for its response headers. If the request is
/// dropped meanwhile (Claude Code pressed Esc, so axum dropped the handler), the server may
/// already have admitted the attempt and be generating: cancel it. Once headers (or an error)
/// arrive, [`PendingAttempt::settle`] stands it down; the response's own ack/cancel takes over.
pub(crate) struct PendingAttempt<'a> {
    st: &'a AppState,
    id: String,
    req_headers: &'a HeaderMap,
    armed: bool,
}

impl<'a> PendingAttempt<'a> {
    pub(crate) fn new(st: &'a AppState, id: &str, req_headers: &'a HeaderMap) -> Self {
        PendingAttempt {
            st,
            id: id.to_string(),
            req_headers,
            armed: true,
        }
    }

    /// The request finished (answered or failed); nothing to do on drop any more.
    pub(crate) fn settle(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingAttempt<'_> {
    fn drop(&mut self) {
        if self.armed {
            cancel_unanswered(self.st, &self.id, self.req_headers, "request abandoned");
        }
    }
}

fn spawn_delete(client: reqwest::Client, url: String, creds: HeaderMap, id: String) {
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return;
    };
    rt.spawn(async move {
        // `client` opens a new connection per request, so a retry never lands on the same
        // (possibly black-holed) connection.
        let send = || {
            tokio::time::timeout(
                DELETE_TIMEOUT,
                client.delete(&url).headers(creds.clone()).send(),
            )
        };
        let describe = |r: &Result<reqwest::Result<reqwest::Response>, _>| match r {
            Ok(Ok(r)) => format!("-> {}", r.status().as_u16()),
            Ok(Err(e)) => format!("failed: {}", describe_reqwest_error(e)),
            Err(_) => "timed out".to_string(),
        };
        let mut res = send().await;
        // DELETE is idempotent and a lost cancel leaves a generation running: retry once after
        // a dropped or unanswered attempt (not after a refused connect: the server is down).
        if res.is_err() || matches!(&res, Ok(Err(e)) if !e.is_connect()) {
            log_debug!("resume {id}: DELETE {}; retrying once", describe(&res));
            res = send().await;
        }
        match &res {
            Ok(Ok(_)) => log_debug!("resume {id}: DELETE {}", describe(&res)),
            _ => log_info!("resume {id}: DELETE {}", describe(&res)),
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_random_lower_hex() {
        let a = new_resume_id().unwrap();
        let b = new_resume_id().unwrap();
        assert!(is_valid_id(&a), "{a}");
        assert_ne!(a, b);
        assert!(!is_valid_id("ABCDEF0123456789abcdef0123456789"));
        assert!(!is_valid_id("abc"));
    }

    #[test]
    fn credentials_only() {
        let mut h = HeaderMap::new();
        h.insert("x-api-key", HeaderValue::from_static("k"));
        h.insert("authorization", HeaderValue::from_static("Bearer t"));
        h.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        let c = credential_headers(&h);
        assert_eq!(c.len(), 2);
        assert_eq!(c["x-api-key"], "k");
    }

    #[test]
    fn tracker_sees_content_and_stop_across_splits() {
        let s = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
        for cut in 1..s.len() {
            let mut t = StopTracker::new();
            t.push(&s[..cut]);
            t.push(&s[cut..]);
            assert!(t.content_seen() && t.stop_seen(), "cut {cut}");
        }
        let mut t = StopTracker::new();
        t.push(b"event: ping\ndata: {\"type\":\"ping\"}\n\n");
        assert!(!t.content_seen() && !t.terminal_seen());
        let mut e = StopTracker::new();
        e.push(b"data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n");
        assert!(e.terminal_seen() && !e.stop_seen());
    }

    #[test]
    fn terminal_frame_counts_only_with_its_blank_line() {
        // A decoder drops an event that is not terminated by a blank line at EOF.
        for frame in [
            &b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"[..],
            b"event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\n\r\n",
            b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n",
        ] {
            for cut in 1..frame.len() {
                let mut t = StopTracker::new();
                t.push(&frame[..cut]);
                assert!(
                    !t.terminal_seen() && !t.stop_seen(),
                    "{:?} cut at {cut}",
                    String::from_utf8_lossy(&frame[..cut])
                );
                t.push(&frame[cut..]);
                assert!(t.terminal_seen(), "{cut}");
            }
        }
    }

    #[test]
    fn episode_deadline_is_last_byte_plus_idle_plus_budget() {
        let t = Instant::now();
        let s = Duration::from_secs;
        assert_eq!(episode_deadline(t, s(45), s(225)), t + s(270));
        assert_eq!(episode_deadline(t, Duration::ZERO, s(2)), t + s(2));
        // Absurd config values must not panic.
        assert!(episode_deadline(t, Duration::MAX, Duration::MAX) > t + s(86_400));
    }
}
