//! The local proxy.
//!
//! * PATH S (`stream: true`): pass the stream through, but hold back everything except pings
//!   until the first content frame, so a stream that dies before any content can be re-sent
//!   silently instead of pushing Claude Code into its non-streaming fallback.
//! * PATH N (no `stream: true`): send the request upstream as a stream (so the gateway keeps
//!   the Cloudflare connection busy with pings) and rebuild the non-stream Message JSON.
//! * Everything else is a transparent passthrough.

use crate::aggregate::{is_content_event, Aggregator, Step};
use crate::config::Config;
use crate::errmap;
use crate::sse::{Frame, SseParser};
use crate::{log_info, log_warn};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use axum::Router;
use bytes::{Bytes, BytesMut};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use serde_json::Value;
use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A single SSE frame larger than this is treated as a broken stream.
const MAX_FRAME_BYTES: usize = 64 << 20;
/// PATH N: total SSE bytes aggregated for one response.
const MAX_AGGREGATE_BYTES: usize = 256 << 20;
/// PATH S: if this much is held back before content, stop holding and pass everything through.
const MAX_HOLD_BYTES: usize = 8 << 20;
/// Upstream error bodies are read into memory up to this size.
const MAX_ERROR_BODY: usize = 1 << 20;
/// After this many consecutive pre-content failures (across requests) silent retries pause...
const BREAKER_THRESHOLD: u32 = 3;
/// ...for this long, so an upstream outage is not multiplied by the shim.
const BREAKER_COOLDOWN: Duration = Duration::from_secs(60);

pub const CF_524_MESSAGE: &str = "Cloudflare 524 upstream timeout — the request may still be running upstream; not retried locally";

pub struct AppState {
    upstream: String,
    client: reqwest::Client,
    max_body: usize,
    silent_retries: u32,
    retry_window: Duration,
    retry_delay: Duration,
    epoch: Instant,
    breaker_failures: AtomicU32,
    breaker_last_ms: AtomicU64,
}

impl AppState {
    pub fn new(cfg: &Config) -> Result<Self, String> {
        let upstream = crate::config::normalize_upstream(&cfg.upstream)?;
        let mut builder = reqwest::Client::builder();
        if !cfg.use_env_proxy {
            builder = builder.no_proxy();
        }
        let client = builder
            .connect_timeout(Duration::from_secs(cfg.connect_timeout_secs.max(1)))
            .redirect(reqwest::redirect::Policy::none())
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(60))
            .http1_only()
            .build()
            .map_err(|e| format!("cannot build HTTP client: {e}"))?;
        Ok(AppState {
            upstream,
            client,
            max_body: (cfg.max_body_mib.max(1) as usize).saturating_mul(1 << 20),
            silent_retries: cfg.silent_retries,
            retry_window: Duration::from_secs(cfg.silent_retry_max_elapsed_secs),
            retry_delay: Duration::from_millis(cfg.silent_retry_delay_ms),
            epoch: Instant::now(),
            breaker_failures: AtomicU32::new(0),
            breaker_last_ms: AtomicU64::new(0),
        })
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Record a transient failure before any content and decide whether to retry silently.
    fn pre_content_failure(&self, retries_done: u32, started: Instant) -> bool {
        let now = self.now_ms();
        let last = self.breaker_last_ms.swap(now, Ordering::Relaxed);
        let mut n = self.breaker_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if now.saturating_sub(last) >= BREAKER_COOLDOWN.as_millis() as u64 {
            self.breaker_failures.store(1, Ordering::Relaxed);
            n = 1;
        }
        let breaker_open = n > BREAKER_THRESHOLD;
        if breaker_open && retries_done < self.silent_retries {
            log_warn!("{n} consecutive failures before content; silent retries paused");
        }
        retries_done < self.silent_retries && started.elapsed() < self.retry_window && !breaker_open
    }

    fn content_ok(&self) {
        self.breaker_failures.store(0, Ordering::Relaxed);
    }

    fn url(&self, uri: &axum::http::Uri) -> String {
        let pq = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
        format!("{}{}", self.upstream, pq)
    }

    async fn send(
        &self,
        method: &Method,
        url: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<reqwest::Response, String> {
        self.client
            .request(method.clone(), url)
            .headers(headers.clone())
            .body(body)
            .send()
            .await
            .map_err(|e| describe_reqwest_error(&e))
    }
}

fn describe_reqwest_error(e: &reqwest::Error) -> String {
    let kind = if e.is_connect() {
        "connect error"
    } else if e.is_timeout() {
        "timeout"
    } else if e.is_body() || e.is_decode() {
        "body error"
    } else {
        "request error"
    };
    let mut msg = format!("{kind}: {}", e.without_url_ref());
    let mut src = std::error::Error::source(e);
    while let Some(s) = src {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        src = s.source();
    }
    errmap::truncate(&msg, 300)
}

trait WithoutUrlRef {
    fn without_url_ref(&self) -> String;
}
impl WithoutUrlRef for reqwest::Error {
    fn without_url_ref(&self) -> String {
        let mut s = self.to_string();
        if let Some(u) = self.url() {
            s = s.replace(&format!(" for url ({u})"), "");
        }
        s
    }
}

pub fn app(cfg: &Config) -> Result<Router, String> {
    let st = Arc::new(AppState::new(cfg)?);
    Ok(Router::new().fallback(handle).with_state(st))
}

/// Serve until `shutdown` resolves.
pub async fn serve<F>(listener: tokio::net::TcpListener, cfg: &Config, shutdown: F) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = app(cfg).map_err(std::io::Error::other)?;
    axum::serve(listener, app).with_graceful_shutdown(shutdown).await
}

// ------------------------------------------------------------------------------ logging

/// One log line per request, written when the request (including a streamed body) ends.
struct ReqLog {
    method: Method,
    path: String,
    started: Instant,
    mode: &'static str,
    status: u16,
    retries: u32,
    outcome: String,
}

impl ReqLog {
    fn new(method: Method, path: String) -> Self {
        ReqLog {
            method,
            path,
            started: Instant::now(),
            mode: "passthrough",
            status: 0,
            retries: 0,
            outcome: "client_closed".into(),
        }
    }
    fn set(&mut self, status: u16, outcome: impl Into<String>) {
        self.status = status;
        self.outcome = outcome.into();
    }
}

impl Drop for ReqLog {
    fn drop(&mut self) {
        log_info!(
            "{} {} mode={} status={} dur={}ms retries={} outcome={}",
            self.method,
            self.path,
            self.mode,
            self.status,
            self.started.elapsed().as_millis(),
            self.retries,
            self.outcome
        );
    }
}

// ------------------------------------------------------------------------------ helpers

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection" | "keep-alive" | "te" | "trailer" | "trailers" | "transfer-encoding" | "upgrade"
    ) || name.starts_with("proxy-")
}

fn connection_listed(h: &HeaderMap) -> Vec<String> {
    h.get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

fn filter_headers(src: &HeaderMap, skip: &[HeaderName]) -> HeaderMap {
    let listed = connection_listed(src);
    let mut out = HeaderMap::with_capacity(src.len());
    for (name, value) in src {
        let n = name.as_str();
        if is_hop_by_hop(n) || listed.iter().any(|l| l == n) || skip.contains(name) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

/// Headers for an upstream `/v1/messages` request (body is re-sent, length recomputed).
fn messages_upstream_headers(src: &HeaderMap) -> HeaderMap {
    let mut h = filter_headers(
        src,
        &[
            header::HOST,
            header::EXPECT,
            header::CONTENT_LENGTH,
            header::ACCEPT_ENCODING,
        ],
    );
    h.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    h
}

fn header_str<'a>(h: &'a HeaderMap, name: &HeaderName) -> &'a str {
    h.get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
}

fn is_sse(h: &HeaderMap) -> bool {
    header_str(h, &header::CONTENT_TYPE)
        .to_ascii_lowercase()
        .starts_with("text/event-stream")
}

fn is_encoded(h: &HeaderMap) -> bool {
    let ce = header_str(h, &header::CONTENT_ENCODING).trim();
    !ce.is_empty() && !ce.eq_ignore_ascii_case("identity")
}

fn host_is_local(v: &str) -> bool {
    let v = v.trim();
    let host = if let Some(rest) = v.strip_prefix('[') {
        match rest.split_once(']') {
            Some((h, _)) => h,
            None => return false,
        }
    } else {
        match v.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => h,
            _ => v,
        }
    };
    let host = host.trim_end_matches('.');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| match ip {
                std::net::IpAddr::V4(a) => a.is_loopback(),
                std::net::IpAddr::V6(a) => {
                    a.is_loopback() || a.to_ipv4_mapped().is_some_and(|m| m.is_loopback())
                }
            })
            .unwrap_or(false)
}

/// Reject requests that are not addressed to the loopback name (DNS rebinding) or that come
/// from a web page on another origin.
fn request_is_local(h: &HeaderMap) -> bool {
    if let Some(host) = h.get(header::HOST) {
        match host.to_str() {
            Ok(s) if host_is_local(s) => {}
            _ => return false,
        }
    }
    if let Some(origin) = h.get(header::ORIGIN) {
        let ok = origin
            .to_str()
            .ok()
            .and_then(|o| reqwest::Url::parse(o).ok())
            .and_then(|u| u.host_str().map(host_is_local))
            .unwrap_or(false);
        if !ok {
            return false;
        }
    }
    true
}

fn status_code(s: u16) -> StatusCode {
    StatusCode::from_u16(s).unwrap_or(StatusCode::BAD_GATEWAY)
}

fn json_response(status: u16, mut headers: HeaderMap, body: Vec<u8>) -> Response {
    headers.remove(header::CONTENT_LENGTH);
    headers.remove(header::CONTENT_ENCODING);
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = status_code(status);
    *r.headers_mut() = headers;
    r
}

fn error_response(status: u16, error_type: &str, message: &str, request_id: Option<&str>) -> Response {
    json_response(
        status,
        HeaderMap::new(),
        errmap::error_json(error_type, message, request_id),
    )
}

fn request_id(h: &HeaderMap) -> Option<String> {
    h.get("request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

async fn read_limited(resp: reqwest::Response, limit: usize) -> Bytes {
    let mut out = BytesMut::new();
    let mut s = resp.bytes_stream();
    while let Some(Ok(chunk)) = s.next().await {
        let room = limit.saturating_sub(out.len());
        out.extend_from_slice(&chunk[..chunk.len().min(room)]);
        if out.len() >= limit {
            break;
        }
    }
    out.freeze()
}

/// Stream an upstream response back unchanged.
fn stream_through(resp: reqwest::Response, mut log: ReqLog, outcome: &'static str) -> Response {
    let status = resp.status().as_u16();
    let headers = filter_headers(resp.headers(), &[]);
    // hyper stops polling once `content-length` bytes are written, so count them ourselves.
    let mut remaining = resp.content_length();
    log.set(status, if remaining == Some(0) { outcome } else { "client_closed" });
    let body = futures_util::stream::unfold(
        (resp.bytes_stream().boxed(), Some(log)),
        move |(mut s, mut log)| async move {
            let l = log.as_mut()?;
            match s.next().await {
                Some(Ok(b)) => {
                    if let Some(r) = remaining.as_mut() {
                        *r = r.saturating_sub(b.len() as u64);
                        if *r == 0 {
                            l.outcome = outcome.into();
                        }
                    }
                    Some((Ok(b), (s, log)))
                }
                Some(Err(e)) => {
                    l.outcome = "upstream_io_error".into();
                    let err = std::io::Error::other(describe_reqwest_error(&e));
                    Some((Err(err), (s, None)))
                }
                None => {
                    l.outcome = outcome.into();
                    None
                }
            }
        },
    );
    let mut r = Response::new(Body::from_stream(body));
    *r.status_mut() = status_code(status);
    *r.headers_mut() = headers;
    r
}

/// Non-2xx upstream response: pass through, except Cloudflare HTML error pages, which are
/// turned into Anthropic-style JSON (524 becomes a non-retried 529).
async fn upstream_error_response(resp: reqwest::Response, mut log: ReqLog) -> Response {
    let status = resp.status().as_u16();
    let ct = header_str(resp.headers(), &header::CONTENT_TYPE).to_ascii_lowercase();
    let html = ct.contains("text/html");
    let maybe_cf = status == 524 || (status >= 500 && html) || (520..=530).contains(&status);
    if !maybe_cf {
        return stream_through(resp, log, "upstream_status");
    }
    let mut headers = filter_headers(resp.headers(), &[]);
    let body = read_limited(resp, MAX_ERROR_BODY).await;
    let is_json = !html && serde_json::from_slice::<Value>(&body).is_ok();
    if status != 524 && is_json {
        log.set(status, "upstream_status");
        headers.remove(header::CONTENT_LENGTH);
        let mut r = Response::new(Body::from(body));
        *r.status_mut() = status_code(status);
        *r.headers_mut() = headers;
        return r;
    }
    headers.remove("x-should-retry");
    if status == 524 {
        if !headers.contains_key(header::RETRY_AFTER) {
            headers.insert(header::RETRY_AFTER, HeaderValue::from_static("120"));
        }
        log.set(529, "cf524_not_retried");
        return json_response(
            529,
            headers,
            errmap::error_json("overloaded_error", CF_524_MESSAGE, None),
        );
    }
    log.set(status, "upstream_html_error");
    let msg = format!(
        "upstream returned HTTP {status}: {}",
        errmap::body_message(&body)
    );
    json_response(status, headers, errmap::error_json("api_error", &msg, None))
}

// ------------------------------------------------------------------------------ handler

async fn handle(State(st): State<Arc<AppState>>, req: Request) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    if !request_is_local(req.headers()) {
        let mut log = ReqLog::new(method, path);
        log.set(403, "rejected_non_local_host");
        return error_response(
            403,
            "permission_error",
            "as-shim only accepts requests addressed to 127.0.0.1/localhost",
            None,
        );
    }
    if path == "/__as/health" && (method == Method::GET || method == Method::HEAD) {
        let body = serde_json::json!({"ok": true, "version": crate::VERSION, "upstream": st.upstream});
        return json_response(200, HeaderMap::new(), body.to_string().into_bytes());
    }
    let log = ReqLog::new(method.clone(), path.clone());
    if method == Method::POST && path == "/v1/messages" {
        return messages(st, req, log).await;
    }
    passthrough(st, req, log).await
}

async fn passthrough(st: Arc<AppState>, req: Request, mut log: ReqLog) -> Response {
    let (parts, body) = req.into_parts();
    let declared_len = header_str(&parts.headers, &header::CONTENT_LENGTH)
        .parse::<u64>()
        .ok();
    if declared_len.is_some_and(|n| n > st.max_body as u64) {
        log.set(413, "request_too_large");
        return error_response(
            413,
            "request_too_large",
            "as-shim: request body exceeds max_body_mib",
            None,
        );
    }
    let has_body = declared_len.is_some_and(|n| n > 0)
        || parts.headers.contains_key(header::TRANSFER_ENCODING);
    let headers = filter_headers(&parts.headers, &[header::HOST, header::EXPECT]);
    let url = st.url(&parts.uri);
    let mut rb = st.client.request(parts.method.clone(), &url).headers(headers);
    if has_body {
        let max = st.max_body;
        let mut seen = 0usize;
        let limited = body.into_data_stream().map(move |r| match r {
            Ok(b) => {
                seen += b.len();
                if seen > max {
                    Err(std::io::Error::other("request body exceeds max_body_mib"))
                } else {
                    Ok(b)
                }
            }
            Err(e) => Err(std::io::Error::other(e)),
        });
        rb = rb.body(reqwest::Body::wrap_stream(limited));
    }
    match rb.send().await {
        Ok(resp) if resp.status().is_success() => stream_through(resp, log, "ok"),
        Ok(resp) => upstream_error_response(resp, log).await,
        Err(e) => {
            let msg = describe_reqwest_error(&e);
            log.set(502, "upstream_request_failed");
            error_response(502, "api_error", &format!("as-shim: upstream request failed: {msg}"), None)
        }
    }
}

async fn messages(st: Arc<AppState>, req: Request, mut log: ReqLog) -> Response {
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, st.max_body).await {
        Ok(b) => b,
        Err(e) => {
            let too_large = std::error::Error::source(&e)
                .is_some_and(|s| s.is::<http_body_util::LengthLimitError>());
            if too_large {
                log.set(413, "request_too_large");
                return error_response(
                    413,
                    "request_too_large",
                    "as-shim: request body exceeds max_body_mib",
                    None,
                );
            }
            log.set(400, "client_body_error");
            return error_response(400, "invalid_request_error", "as-shim: could not read request body", None);
        }
    };
    let headers = messages_upstream_headers(&parts.headers);
    let url = st.url(&parts.uri);
    let obj = match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(m)) => m,
        _ => {
            log.mode = "passthrough";
            return match st.send(&parts.method, &url, &headers, bytes).await {
                Ok(resp) if resp.status().is_success() => stream_through(resp, log, "ok"),
                Ok(resp) => upstream_error_response(resp, log).await,
                Err(msg) => {
                    log.set(502, "upstream_request_failed");
                    error_response(502, "api_error", &format!("as-shim: upstream request failed: {msg}"), None)
                }
            };
        }
    };
    if obj.get("stream") == Some(&Value::Bool(true)) {
        log.mode = "S";
        path_s(st, parts.method, url, headers, bytes, log).await
    } else {
        log.mode = "N";
        let mut obj = obj;
        obj.insert("stream".into(), Value::Bool(true));
        let body = Bytes::from(serde_json::to_vec(&Value::Object(obj)).unwrap_or_default());
        path_n(st, parts.method, url, headers, body, log).await
    }
}

// ------------------------------------------------------------------------------ failures

enum Failure {
    Eof,
    Io(String),
    Frame {
        error_type: String,
        message: String,
        request_id: Option<String>,
        frame: Box<Frame>,
    },
    Malformed(String),
}

impl Failure {
    fn from_error_frame(frame: Frame, json: Option<&Value>) -> Self {
        let (error_type, message, request_id) = errmap::parse_error(json.unwrap_or(&Value::Null));
        Failure::Frame {
            error_type,
            message,
            request_id,
            frame: Box::new(frame),
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            Failure::Eof | Failure::Io(_) => true,
            Failure::Frame { error_type, .. } => errmap::is_transient(error_type),
            Failure::Malformed(_) => false,
        }
    }

    fn describe(&self) -> String {
        match self {
            Failure::Eof => "stream ended early".into(),
            Failure::Io(e) => format!("stream I/O error ({e})"),
            Failure::Frame {
                error_type, message, ..
            } => format!("error frame {error_type}: {message}"),
            Failure::Malformed(e) => format!("malformed stream ({e})"),
        }
    }

    fn outcome(&self) -> String {
        match self {
            Failure::Eof => "eof".into(),
            Failure::Io(_) => "io_error".into(),
            Failure::Frame { error_type, .. } => {
                format!("error_frame:{}", errmap::truncate(error_type, 64).replace(' ', "_"))
            }
            Failure::Malformed(_) => "malformed_stream".into(),
        }
    }
}

// ------------------------------------------------------------------------------ PATH N

async fn path_n(
    st: Arc<AppState>,
    method: Method,
    url: String,
    headers: HeaderMap,
    body: Bytes,
    mut log: ReqLog,
) -> Response {
    let mut retries = 0u32;
    loop {
        let resp = match st.send(&method, &url, &headers, body.clone()).await {
            Ok(r) => r,
            Err(msg) => {
                if st.pre_content_failure(retries, log.started) {
                    retries += 1;
                    log.retries = retries;
                    log_info!("PATH N: upstream request failed ({msg}); silent retry {retries}");
                    tokio::time::sleep(st.retry_delay).await;
                    continue;
                }
                log.set(529, "upstream_request_failed");
                return error_response(
                    529,
                    "overloaded_error",
                    &format!("as-shim: upstream request failed: {msg}"),
                    None,
                );
            }
        };
        if !resp.status().is_success() {
            return upstream_error_response(resp, log).await;
        }
        if !is_sse(resp.headers()) || is_encoded(resp.headers()) {
            return stream_through(resp, log, "ok_not_sse");
        }
        let status = resp.status().as_u16();
        let resp_headers = resp.headers().clone();
        match aggregate(&st, resp).await {
            Ok(msg) => {
                let mut h = filter_headers(&resp_headers, &[header::CONTENT_TYPE]);
                h.remove(header::CONTENT_LENGTH);
                log.set(status, "ok");
                return json_response(status, h, serde_json::to_vec(&msg).unwrap_or_default());
            }
            Err((failure, content_started)) => {
                if !content_started
                    && failure.is_transient()
                    && st.pre_content_failure(retries, log.started)
                {
                    retries += 1;
                    log.retries = retries;
                    log_info!(
                        "PATH N: {} before content; silent retry {retries}",
                        failure.outcome()
                    );
                    tokio::time::sleep(st.retry_delay).await;
                    continue;
                }
                return path_n_error(failure, &resp_headers, retries, log);
            }
        }
    }
}

fn path_n_error(failure: Failure, upstream_headers: &HeaderMap, retries: u32, mut log: ReqLog) -> Response {
    let outcome = failure.outcome();
    let rid = request_id(upstream_headers);
    let attempts = retries + 1;
    let (status, ty, msg, rid) = match failure {
        Failure::Frame {
            error_type,
            message,
            request_id,
            ..
        } => {
            let (status, ty) = errmap::status_for_error_type(&error_type);
            let msg = if ty == error_type {
                message
            } else {
                format!("upstream {error_type}: {message}")
            };
            (status, ty.to_string(), msg, request_id.or(rid))
        }
        Failure::Eof => (
            529,
            "overloaded_error".to_string(),
            format!("as-shim: upstream stream ended before message_stop (attempts: {attempts})"),
            rid,
        ),
        Failure::Io(e) => (
            529,
            "overloaded_error".to_string(),
            format!("as-shim: upstream stream I/O error: {e} (attempts: {attempts})"),
            rid,
        ),
        Failure::Malformed(e) => (
            502,
            "api_error".to_string(),
            format!("as-shim: upstream sent a malformed stream: {e}"),
            rid,
        ),
    };
    let mut h = HeaderMap::new();
    if status == 429 {
        if let Some(ra) = upstream_headers.get(header::RETRY_AFTER) {
            h.insert(header::RETRY_AFTER, ra.clone());
        }
    }
    log.set(status, outcome);
    json_response(status, h, errmap::error_json(&ty, &msg, rid.as_deref()))
}

enum Handled {
    Continue,
    Done,
}

fn aggregate_frame(agg: &mut Aggregator, frame: Frame) -> Result<Handled, Failure> {
    let json = frame.json();
    let Some(kind) = frame.kind(json.as_ref()) else {
        return Ok(Handled::Continue);
    };
    match kind.as_str() {
        "ping" => Ok(Handled::Continue),
        "error" => Err(Failure::from_error_frame(frame, json.as_ref())),
        _ => {
            drop(frame);
            match agg.apply(&kind, json.as_ref().unwrap_or(&Value::Null)) {
                Ok(Step::Done) => Ok(Handled::Done),
                Ok(Step::Continue) => Ok(Handled::Continue),
                Err(e) => Err(Failure::Malformed(e)),
            }
        }
    }
}

/// Read an SSE response into a non-stream Message. Err carries whether content had started.
async fn aggregate(st: &AppState, resp: reqwest::Response) -> Result<Value, (Failure, bool)> {
    let mut parser = SseParser::new();
    let mut agg = Aggregator::new();
    let mut stream = resp.bytes_stream();
    let mut total = 0usize;
    let mut reported = false;
    loop {
        while let Some(frame) = parser.next_frame() {
            let r = aggregate_frame(&mut agg, frame);
            if agg.content_started() && !reported {
                reported = true;
                st.content_ok();
            }
            match r {
                Ok(Handled::Continue) => {}
                Ok(Handled::Done) => return Ok(agg.finish()),
                Err(f) => {
                    let started = agg.content_started();
                    return Err((f, started));
                }
            }
        }
        if parser.pending_len() > MAX_FRAME_BYTES {
            let started = agg.content_started();
            return Err((Failure::Io("SSE frame too large".into()), started));
        }
        match stream.next().await {
            Some(Ok(chunk)) => {
                total += chunk.len();
                if total > MAX_AGGREGATE_BYTES {
                    let started = agg.content_started();
                    return Err((
                        Failure::Malformed("response exceeds the 256 MiB aggregation limit".into()),
                        started,
                    ));
                }
                parser.push(&chunk);
            }
            Some(Err(e)) => {
                let started = agg.content_started();
                return Err((Failure::Io(describe_reqwest_error(&e)), started));
            }
            None => {
                parser.set_eof();
                let mut tail = Vec::new();
                while let Some(f) = parser.next_frame() {
                    tail.push(f);
                }
                tail.extend(parser.finish());
                for frame in tail {
                    match aggregate_frame(&mut agg, frame) {
                        Ok(Handled::Continue) => {}
                        Ok(Handled::Done) => return Ok(agg.finish()),
                        Err(f) => {
                            let started = agg.content_started();
                            return Err((f, started));
                        }
                    }
                }
                if agg.is_complete_without_stop() {
                    log_warn!("PATH N: stream ended after stop_reason without message_stop; returning the complete message");
                    return Ok(agg.finish());
                }
                let started = agg.content_started();
                return Err((Failure::Eof, started));
            }
        }
    }
}

// ------------------------------------------------------------------------------ PATH S

async fn path_s(
    st: Arc<AppState>,
    method: Method,
    url: String,
    headers: HeaderMap,
    body: Bytes,
    mut log: ReqLog,
) -> Response {
    let mut retries = 0u32;
    let resp = loop {
        match st.send(&method, &url, &headers, body.clone()).await {
            Ok(r) => break r,
            Err(msg) => {
                if st.pre_content_failure(retries, log.started) {
                    retries += 1;
                    log.retries = retries;
                    log_info!("PATH S: upstream request failed ({msg}); silent retry {retries}");
                    tokio::time::sleep(st.retry_delay).await;
                    continue;
                }
                log.set(529, "upstream_request_failed");
                return error_response(
                    529,
                    "overloaded_error",
                    &format!("as-shim: upstream request failed: {msg}"),
                    None,
                );
            }
        }
    };
    if !resp.status().is_success() {
        return upstream_error_response(resp, log).await;
    }
    if !is_sse(resp.headers()) || is_encoded(resp.headers()) {
        return stream_through(resp, log, "ok_not_sse");
    }
    let status = resp.status().as_u16();
    let mut out_headers = filter_headers(resp.headers(), &[]);
    out_headers.remove(header::CONTENT_LENGTH);
    log.set(status, "client_closed");
    let relay = HoldRelay {
        st,
        method,
        url,
        headers,
        body,
        retries,
        stream: resp.bytes_stream().boxed(),
        parser: SseParser::new(),
        held: BytesMut::new(),
        phase: Phase::Hold,
        eof: false,
        fail: None,
        log,
    };
    let stream = futures_util::stream::unfold(relay, |mut r| async move {
        r.next_chunk().await.map(|c| (c, r))
    });
    let mut resp = Response::new(Body::from_stream(stream));
    *resp.status_mut() = status_code(status);
    *resp.headers_mut() = out_headers;
    resp
}

#[derive(PartialEq)]
enum Phase {
    Hold,
    Pass,
    Done,
}

struct HoldRelay {
    st: Arc<AppState>,
    method: Method,
    url: String,
    headers: HeaderMap,
    body: Bytes,
    retries: u32,
    stream: BoxStream<'static, reqwest::Result<Bytes>>,
    parser: SseParser,
    held: BytesMut,
    phase: Phase,
    eof: bool,
    fail: Option<Failure>,
    log: ReqLog,
}

/// `event: error` frame built from an upstream error frame that lacked the event line.
fn as_event_error(frame: &Frame) -> Bytes {
    if frame.event.as_deref() == Some("error") {
        return frame.raw.clone();
    }
    let mut out = String::from("event: error\n");
    for line in frame.data.as_deref().unwrap_or("").split('\n') {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    Bytes::from(out)
}

impl HoldRelay {
    async fn next_chunk(&mut self) -> Option<Result<Bytes, std::io::Error>> {
        loop {
            match self.phase {
                Phase::Done => return None,
                Phase::Pass => {
                    return match self.stream.next().await {
                        Some(Ok(b)) => Some(Ok(b)),
                        Some(Err(e)) => {
                            self.phase = Phase::Done;
                            self.log.outcome = "upstream_io_error_after_content".into();
                            Some(Err(std::io::Error::other(describe_reqwest_error(&e))))
                        }
                        None => {
                            self.phase = Phase::Done;
                            self.log.outcome = "ok".into();
                            None
                        }
                    };
                }
                Phase::Hold => {}
            }
            let mut out = BytesMut::new();
            if let Some(f) = self.fail.take() {
                self.handle_failure(f, &mut out).await;
            } else {
                self.drain(&mut out);
                if out.is_empty() && self.phase == Phase::Hold && self.fail.is_none() {
                    if self.eof {
                        let tail = self.parser.finish();
                        self.fail = Some(match tail {
                            Some(frame) => {
                                let json = frame.json();
                                if frame.kind(json.as_ref()).as_deref() == Some("error") {
                                    Failure::from_error_frame(frame, json.as_ref())
                                } else {
                                    Failure::Eof
                                }
                            }
                            None => Failure::Eof,
                        });
                        continue;
                    }
                    if let Some(flushed) = self.read_more().await {
                        return Some(Ok(flushed));
                    }
                    continue;
                }
            }
            if !out.is_empty() {
                return Some(Ok(out.freeze()));
            }
        }
    }

    /// Read the next upstream chunk. Returns bytes to emit when HOLD had to be abandoned.
    async fn read_more(&mut self) -> Option<Bytes> {
        match self.stream.next().await {
            Some(Ok(chunk)) => {
                self.parser.push(&chunk);
                if self.parser.pending_len() + self.held.len() > MAX_HOLD_BYTES {
                    log_warn!("PATH S: more than 8 MiB before any content; passing the stream through");
                    let mut out = std::mem::take(&mut self.held);
                    out.extend_from_slice(&self.parser.take_remaining());
                    self.phase = Phase::Pass;
                    return Some(out.freeze());
                }
            }
            Some(Err(e)) => self.fail = Some(Failure::Io(describe_reqwest_error(&e))),
            None => {
                self.parser.set_eof();
                self.eof = true;
            }
        }
        None
    }

    /// Process complete frames: pings/comments go out at once, the rest is held until content.
    fn drain(&mut self, out: &mut BytesMut) {
        while let Some(frame) = self.parser.next_frame() {
            let json = frame.json();
            let kind = frame.kind(json.as_ref());
            match kind.as_deref() {
                _ if frame.is_empty() => out.extend_from_slice(&frame.raw),
                Some("ping") => out.extend_from_slice(&frame.raw),
                Some("error") => {
                    self.fail = Some(Failure::from_error_frame(frame, json.as_ref()));
                    return;
                }
                Some(k) if is_content_event(k) => {
                    out.extend_from_slice(&self.held);
                    self.held.clear();
                    out.extend_from_slice(&frame.raw);
                    out.extend_from_slice(&self.parser.take_remaining());
                    self.phase = Phase::Pass;
                    self.st.content_ok();
                    return;
                }
                _ => self.held.extend_from_slice(&frame.raw),
            }
        }
    }

    async fn handle_failure(&mut self, f: Failure, out: &mut BytesMut) {
        let transient = f.is_transient();
        if transient && self.st.pre_content_failure(self.retries, self.log.started) {
            self.retries += 1;
            self.log.retries = self.retries;
            log_info!("PATH S: {} before content; silent retry {}", f.outcome(), self.retries);
            tokio::time::sleep(self.st.retry_delay).await;
            self.parser = SseParser::new();
            self.held.clear();
            self.eof = false;
            match self
                .st
                .send(&self.method, &self.url, &self.headers, self.body.clone())
                .await
            {
                Err(msg) => self.fail = Some(Failure::Io(msg)),
                Ok(resp)
                    if resp.status().is_success()
                        && is_sse(resp.headers())
                        && !is_encoded(resp.headers()) =>
                {
                    self.stream = resp.bytes_stream().boxed();
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let body = read_limited(resp, 64 << 10).await;
                    let (ty, msg) = if status == 524 {
                        ("overloaded_error", CF_524_MESSAGE.to_string())
                    } else if (200..300).contains(&status) {
                        (
                            "overloaded_error",
                            "as-shim: silent retry got a non-SSE response".to_string(),
                        )
                    } else {
                        (
                            errmap::error_type_for_status(status),
                            format!(
                                "upstream returned HTTP {status} on silent retry: {}",
                                errmap::body_message(&body)
                            ),
                        )
                    };
                    out.extend_from_slice(&errmap::sse_error_frame(ty, &msg));
                    self.phase = Phase::Done;
                    self.log.outcome = format!("retry_http_{status}");
                }
            }
            return;
        }
        let outcome = f.outcome();
        match f {
            Failure::Frame { ref frame, .. } if !transient => {
                out.extend_from_slice(&as_event_error(frame));
            }
            other => {
                let msg = format!(
                    "as-shim: upstream {} before any content (silent retries used: {})",
                    other.describe(),
                    self.retries
                );
                out.extend_from_slice(&errmap::sse_error_frame("overloaded_error", &msg));
            }
        }
        self.phase = Phase::Done;
        self.log.outcome = outcome;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_hosts() {
        for h in ["127.0.0.1:8787", "localhost", "LOCALHOST.:1", "[::1]:8787", "127.3.4.5"] {
            assert!(host_is_local(h), "{h}");
        }
        for h in ["evil.example:8787", "192.168.1.2", "[::2]:1", "localhost.evil.com"] {
            assert!(!host_is_local(h), "{h}");
        }
    }

    #[test]
    fn hop_by_hop_filtering() {
        let mut h = HeaderMap::new();
        h.insert("connection", HeaderValue::from_static("keep-alive, X-Foo"));
        h.insert("x-foo", HeaderValue::from_static("1"));
        h.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        h.insert("proxy-authorization", HeaderValue::from_static("x"));
        h.insert("x-api-key", HeaderValue::from_static("k"));
        h.insert("host", HeaderValue::from_static("127.0.0.1:1"));
        h.insert("accept-encoding", HeaderValue::from_static("gzip"));
        h.insert("content-length", HeaderValue::from_static("10"));
        let m = messages_upstream_headers(&h);
        assert_eq!(m.len(), 2, "{m:?}");
        assert_eq!(m["x-api-key"], "k");
        assert_eq!(m["accept-encoding"], "identity");
    }

    #[test]
    fn event_line_added() {
        let mut p = SseParser::new();
        p.push(b"data: {\"type\":\"error\",\ndata: \"x\":1}\n\nevent: error\ndata: {}\n\n");
        let f = p.next_frame().unwrap();
        assert_eq!(
            &as_event_error(&f)[..],
            b"event: error\ndata: {\"type\":\"error\",\ndata: \"x\":1}\n\n"
        );
        let g = p.next_frame().unwrap();
        assert_eq!(&as_event_error(&g)[..], b"event: error\ndata: {}\n\n");
    }
}
