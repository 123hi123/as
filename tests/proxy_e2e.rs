//! End-to-end tests: the real proxy against an in-process mock upstream on 127.0.0.1.

use as_shim::config::Config;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ------------------------------------------------------------------------------ mock upstream

#[derive(Clone, Debug)]
struct Seen {
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// One scripted reply: a list of (delay before, bytes) writes, then either a clean close or
/// an abrupt reset-like close mid-body.
#[derive(Clone)]
struct Reply {
    head: String,
    parts: Vec<(u64, Vec<u8>)>,
    chunked: bool,
}

impl Reply {
    fn sse(frames: &[&str]) -> Self {
        Reply {
            head: "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nrequest-id: req_mock\r\ntransfer-encoding: chunked\r\n".into(),
            parts: frames.iter().map(|f| (0, f.as_bytes().to_vec())).collect(),
            chunked: true,
        }
    }
    fn sse_timed(parts: Vec<(u64, String)>) -> Self {
        Reply {
            head: "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n".into(),
            parts: parts.into_iter().map(|(d, s)| (d, s.into_bytes())).collect(),
            chunked: true,
        }
    }
    fn plain(status: &str, ctype: &str, extra: &str, body: &str) -> Self {
        Reply {
            head: format!(
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\n{extra}content-length: {}\r\n",
                body.len()
            ),
            parts: vec![(0, body.as_bytes().to_vec())],
            chunked: false,
        }
    }
}

struct Mock {
    port: u16,
    seen: Arc<Mutex<Vec<Seen>>>,
    closed_early: Arc<AtomicUsize>,
}

/// `script(n)` = reply for the n-th request (0-based). A reply part with bytes == b"<RESET>"
/// closes the socket without the chunked terminator.
async fn mock<F>(script: F) -> Mock
where
    F: Fn(usize, &Seen) -> Reply + Send + Sync + 'static,
{
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let closed_early = Arc::new(AtomicUsize::new(0));
    let script = Arc::new(script);
    let (s2, c2) = (seen.clone(), closed_early.clone());
    tokio::spawn(async move {
        loop {
            let (sock, _) = l.accept().await.unwrap();
            let (seen, closed, script) = (s2.clone(), c2.clone(), script.clone());
            tokio::spawn(async move {
                let _ = serve_conn(sock, seen, closed, script).await;
            });
        }
    });
    Mock {
        port,
        seen,
        closed_early,
    }
}

async fn read_request(sock: &mut TcpStream) -> Option<Seen> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let n = sock.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let path = lines.next()?.split(' ').nth(1)?.to_string();
    let mut headers = HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let mut body = buf[head_end..].to_vec();
    if let Some(len) = headers.get("content-length").and_then(|v| v.parse::<usize>().ok()) {
        while body.len() < len {
            let n = sock.read(&mut tmp).await.ok()?;
            if n == 0 {
                return None;
            }
            body.extend_from_slice(&tmp[..n]);
        }
    } else if headers.get("transfer-encoding").is_some_and(|v| v.contains("chunked")) {
        while !body.ends_with(b"0\r\n\r\n") {
            let n = sock.read(&mut tmp).await.ok()?;
            if n == 0 {
                return None;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        let mut out = Vec::new();
        let mut rest = &body[..];
        loop {
            let i = rest.windows(2).position(|w| w == b"\r\n").unwrap();
            let n = usize::from_str_radix(std::str::from_utf8(&rest[..i]).unwrap(), 16).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&rest[i + 2..i + 2 + n]);
            rest = &rest[i + 4 + n..];
        }
        body = out;
    }
    Some(Seen {
        path,
        headers,
        body,
    })
}

async fn serve_conn<F>(
    mut sock: TcpStream,
    seen: Arc<Mutex<Vec<Seen>>>,
    closed: Arc<AtomicUsize>,
    script: Arc<F>,
) -> std::io::Result<()>
where
    F: Fn(usize, &Seen) -> Reply,
{
    while let Some(req) = read_request(&mut sock).await {
        let n = {
            let mut s = seen.lock().unwrap();
            s.push(req.clone());
            s.len() - 1
        };
        let reply = script(n, &req);
        sock.write_all(reply.head.as_bytes()).await?;
        sock.write_all(b"\r\n").await?;
        for (delay, bytes) in &reply.parts {
            if *delay > 0 {
                tokio::time::sleep(Duration::from_millis(*delay)).await;
            }
            if bytes == b"<RESET>" {
                sock.shutdown().await.ok();
                return Ok(());
            }
            if bytes == b"<EOF>" {
                break;
            }
            let r = if reply.chunked {
                let mut c = format!("{:x}\r\n", bytes.len()).into_bytes();
                c.extend_from_slice(bytes);
                c.extend_from_slice(b"\r\n");
                sock.write_all(&c).await
            } else {
                sock.write_all(bytes).await
            };
            if r.is_err() {
                closed.fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
        }
        if reply.chunked {
            sock.write_all(b"0\r\n\r\n").await?;
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------------ shim

async fn shim(upstream_port: u16, tweak: impl FnOnce(&mut Config)) -> u16 {
    let mut cfg = Config::new(format!("http://127.0.0.1:{upstream_port}"));
    cfg.silent_retry_delay_ms = 10;
    tweak(&mut cfg);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        as_shim::proxy::serve(l, &cfg, std::future::pending()).await.unwrap();
    });
    port
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

// ------------------------------------------------------------------------------ SSE fixtures

fn ev(name: &str, data: Value) -> String {
    format!("event: {name}\ndata: {data}\n\n")
}
const PING: &str = "event: ping\ndata: {\"type\": \"ping\"}\n\n";
const DATA_ONLY_ERR: &str = "data: {\"type\":\"error\",\"error\":{\"type\":\"upstream_error\",\"message\":\"Upstream request failed\"}}\n\n";

fn message_start() -> String {
    ev(
        "message_start",
        json!({"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":1}}}),
    )
}

fn text_frames(text: &str) -> Vec<String> {
    vec![
        message_start(),
        ev("content_block_start", json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})),
        PING.to_string(),
        ev("content_block_delta", json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}})),
        ev("content_block_stop", json!({"type":"content_block_stop","index":0})),
        ev("message_delta", json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}})),
        ev("message_stop", json!({"type":"message_stop"})),
    ]
}

fn sse_ok(text: &str) -> Reply {
    let f = text_frames(text);
    Reply::sse(&f.iter().map(String::as_str).collect::<Vec<_>>())
}

fn body(stream: bool) -> Value {
    let mut b = json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":"hi"}]});
    if stream {
        b["stream"] = json!(true);
    }
    b
}

async fn post(port: u16, body: &Value) -> reqwest::Response {
    client()
        .post(format!("http://127.0.0.1:{port}/v1/messages?beta=true"))
        .header("x-api-key", "sk-test")
        .header("accept-encoding", "gzip, br")
        .json(body)
        .send()
        .await
        .unwrap()
}

// ------------------------------------------------------------------------------ PATH N

#[tokio::test]
async fn path_n_converts_to_stream_and_aggregates() {
    let m = mock(|_, _| sse_ok("Hello")).await;
    let port = shim(m.port, |_| {}).await;
    let mut b = json!({"model":"m","stream":false,"max_tokens":10,"messages":[]});
    b["metadata"] = json!({"x":1});
    let r = post(port, &b).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "application/json");
    assert_eq!(r.headers()["request-id"], "req_mock");
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["content"][0]["text"], "Hello");
    assert_eq!(v["stop_reason"], "end_turn");
    assert_eq!(v["usage"]["output_tokens"], 5);
    let seen = m.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].path, "/v1/messages?beta=true");
    assert_eq!(seen[0].headers["accept-encoding"], "identity");
    assert_eq!(seen[0].headers["x-api-key"], "sk-test");
    assert_eq!(seen[0].headers["host"], format!("127.0.0.1:{}", m.port));
    assert_eq!(
        seen[0].headers["content-length"],
        seen[0].body.len().to_string()
    );
    let sent = String::from_utf8(seen[0].body.clone()).unwrap();
    assert_eq!(
        sent,
        r#"{"model":"m","stream":true,"max_tokens":10,"messages":[],"metadata":{"x":1}}"#
    );
}

#[tokio::test]
async fn path_n_silent_retry_on_data_only_error_before_content() {
    let m = mock(|n, _| {
        if n == 0 {
            Reply::sse(&[PING, DATA_ONLY_ERR])
        } else {
            sse_ok("again")
        }
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["content"][0]["text"], "again");
    assert_eq!(m.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn path_n_retries_exhausted_gives_529() {
    let m = mock(|_, _| Reply::sse(&[PING, "<EOF>"])).await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 529);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["error"]["type"], "overloaded_error");
    assert!(v["error"]["message"].as_str().unwrap().contains("ended before message_stop"));
    assert_eq!(m.seen.lock().unwrap().len(), 2, "one silent retry");
}

#[tokio::test]
async fn path_n_error_frame_mapping_and_no_retry_after_content() {
    let m = mock(|_, _| {
        Reply::sse(&[
            &message_start(),
            &ev("content_block_start", json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})),
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"boom\"}}\n\n",
        ])
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 500);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["error"]["type"], "api_error");
    assert_eq!(v["error"]["message"], "boom");
    assert_eq!(m.seen.lock().unwrap().len(), 1, "content started: no retry");
}

#[tokio::test]
async fn path_n_non_transient_error_frame_not_retried() {
    let m = mock(|_, _| {
        Reply::sse(&["data: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow\"}}\n\n"])
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 429);
    assert_eq!(m.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn path_n_eof_after_stop_reason_is_success() {
    let m = mock(|_, _| {
        let mut f = text_frames("done");
        f.pop();
        Reply::sse(&f.iter().map(String::as_str).collect::<Vec<_>>())
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["content"][0]["text"], "done");
}

#[tokio::test]
async fn path_n_upstream_json_passthrough_and_4xx_verbatim() {
    let m = mock(|n, _| {
        if n == 0 {
            Reply::plain("200 OK", "application/json", "", r#"{"id":"x","content":[]}"#)
        } else {
            Reply::plain(
                "400 Bad Request",
                "application/json",
                "x-custom: 1\r\n",
                r#"{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}"#,
            )
        }
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), r#"{"id":"x","content":[]}"#);
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 400);
    assert_eq!(r.headers()["x-custom"], "1");
    assert!(r.text().await.unwrap().contains("\"bad\""));
}

// ------------------------------------------------------------------------------ CF errors

const CF524: &str = "<!DOCTYPE html><html><head><title>gw.example | 524: A timeout occurred</title></head><body>Error code 524</body></html>";

#[tokio::test]
async fn cf_524_becomes_529_with_retry_after_and_is_not_retried() {
    for (stream, extra) in [(false, "retry-after: 300\r\n"), (true, "")] {
        let extra = extra.to_string();
        let m = mock(move |_, _| Reply::plain("524 A Timeout Occurred", "text/html; charset=UTF-8", &extra, CF524)).await;
        let port = shim(m.port, |_| {}).await;
        let r = post(port, &body(stream)).await;
        assert_eq!(r.status(), 529);
        let want_ra = if stream { "120" } else { "300" };
        assert_eq!(r.headers()["retry-after"], want_ra);
        assert_eq!(r.headers()["content-type"], "application/json");
        let v: Value = r.json().await.unwrap();
        assert_eq!(v["error"]["type"], "overloaded_error");
        assert!(v["error"]["message"].as_str().unwrap().contains("not retried locally"));
        assert_eq!(m.seen.lock().unwrap().len(), 1, "524 must never be retried locally");
    }
}

#[tokio::test]
async fn cf_502_html_becomes_json_api_error() {
    let m = mock(|_, _| Reply::plain("502 Bad Gateway", "text/html", "", "<html><title>gw | 502: Bad gateway</title></html>")).await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 502);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["error"]["type"], "api_error");
    assert!(v["error"]["message"].as_str().unwrap().contains("502: Bad gateway"));
}

// ------------------------------------------------------------------------------ PATH S

#[tokio::test]
async fn path_s_happy_path_is_byte_identical() {
    let frames = text_frames("streamed");
    let expected: String = frames.concat();
    let m = mock(move |_, _| sse_ok("streamed")).await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/event-stream");
    assert_eq!(r.headers()["request-id"], "req_mock");
    assert_eq!(r.text().await.unwrap(), expected);
    let seen = m.seen.lock().unwrap();
    let sent: Value = serde_json::from_slice(&seen[0].body).unwrap();
    assert_eq!(sent, body(true));
    assert_eq!(seen[0].headers["accept-encoding"], "identity");
}

#[tokio::test]
async fn path_s_pings_forwarded_during_hold_and_silent_retry() {
    let m = mock(|n, _| {
        if n == 0 {
            Reply::sse(&[&message_start(), PING, DATA_ONLY_ERR])
        } else {
            sse_ok("second")
        }
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 200);
    let text = r.text().await.unwrap();
    let mut want = String::from(PING);
    want.push_str(&text_frames("second").concat());
    assert_eq!(text, want, "first attempt's message_start dropped, ping kept");
    assert_eq!(m.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn path_s_retries_exhausted_emits_overloaded_event() {
    let m = mock(|_, _| Reply::sse(&[PING, "<EOF>"])).await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 200);
    let text = r.text().await.unwrap();
    assert!(text.starts_with(PING));
    assert!(text.contains("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\""), "{text}");
    assert_eq!(m.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn path_s_non_transient_data_only_error_gets_event_line() {
    let m = mock(|_, _| {
        Reply::sse(&["data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"nope\"}}\n\n"])
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert_eq!(
        text,
        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"nope\"}}\n\n"
    );
    assert_eq!(m.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn path_s_retry_gets_http_error_maps_to_event() {
    let m = mock(|n, _| {
        if n == 0 {
            Reply::sse(&[PING, "<EOF>"])
        } else {
            Reply::plain("429 Too Many Requests", "application/json", "", r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#)
        }
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert!(text.contains("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\""), "{text}");
    assert!(text.contains("slow down"));
}

#[tokio::test]
async fn path_s_after_content_is_passthrough_and_io_error_aborts() {
    let m = mock(|_, _| {
        let f = text_frames("partial");
        Reply::sse(&[&f[0], &f[1], &f[3], "<RESET>"])
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 200);
    let res = r.bytes().await;
    assert!(res.is_err(), "client must see a broken body, got {res:?}");
    assert_eq!(m.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn path_s_after_content_clean_eof_is_clean_end() {
    let m = mock(|_, _| {
        let f = text_frames("partial");
        Reply::sse(&[&f[0], &f[1], &f[3], DATA_ONLY_ERR])
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert!(text.ends_with(DATA_ONLY_ERR), "frames after content are not rewritten: {text}");
    assert_eq!(m.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn path_s_retry_window_respected() {
    let m = mock(|_, _| Reply::sse(&[PING, "<EOF>"])).await;
    let port = shim(m.port, |c| c.silent_retry_max_elapsed_secs = 0).await;
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert!(text.contains("overloaded_error"));
    assert_eq!(m.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn breaker_pauses_silent_retries_after_repeated_failures() {
    let m = mock(|_, _| Reply::sse(&[PING, "<EOF>"])).await;
    let port = shim(m.port, |_| {}).await;
    for _ in 0..4 {
        let _ = post(port, &body(true)).await.text().await;
    }
    let n = m.seen.lock().unwrap().len();
    assert!(n < 8, "breaker must stop multiplying a persistent outage (got {n} upstream requests)");
}

// ------------------------------------------------------------------------------ passthrough

#[tokio::test]
async fn passthrough_other_paths_and_methods() {
    let m = mock(|_, req| {
        let b = format!(r#"{{"path":"{}","len":{}}}"#, req.path, req.body.len());
        Reply::plain("200 OK", "application/json", "x-up: 1\r\n", &b)
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let c = client();
    let r = c
        .post(format!("http://127.0.0.1:{port}/v1/messages/count_tokens?beta=true"))
        .header("connection", "keep-alive, x-secret")
        .header("x-secret", "drop-me")
        .body("{\"a\":1}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-up"], "1");
    assert_eq!(r.text().await.unwrap(), r#"{"path":"/v1/messages/count_tokens?beta=true","len":7}"#);
    let r = c.get(format!("http://127.0.0.1:{port}/v1/models")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let r = c.post(format!("http://127.0.0.1:{port}/v1/messages")).body("not json").send().await.unwrap();
    assert_eq!(r.status(), 200, "unparseable body: transparent passthrough");
    let seen = m.seen.lock().unwrap();
    assert!(!seen[0].headers.contains_key("x-secret"));
    assert_eq!(seen[2].body, b"not json");
}

#[tokio::test]
async fn health_and_host_check_and_body_limit() {
    let m = mock(|_, _| sse_ok("x")).await;
    let port = shim(m.port, |c| c.max_body_mib = 1).await;
    let c = client();
    let v: Value = c
        .get(format!("http://127.0.0.1:{port}/__as/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["upstream"], format!("http://127.0.0.1:{}", m.port));
    let r = c
        .get(format!("http://127.0.0.1:{port}/__as/health"))
        .header("host", "evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let big = json!({"model":"m","messages":[{"role":"user","content":"x".repeat(2 << 20)}]});
    let r = post(port, &big).await;
    assert_eq!(r.status(), 413);
    assert!(m.seen.lock().unwrap().is_empty());
}

// ------------------------------------------------------------------------------ disconnect

#[tokio::test]
async fn client_disconnect_cancels_upstream() {
    let m = mock(|_, _| {
        let mut parts: Vec<(u64, String)> = vec![(0, message_start())];
        for _ in 0..40 {
            parts.push((100, PING.to_string()));
        }
        Reply::sse_timed(parts)
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let b = body(false).to_string();
    let req = format!(
        "POST /v1/messages HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{b}",
        b.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    drop(s);
    for _ in 0..50 {
        if m.closed_early.load(Ordering::SeqCst) > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("upstream connection was not closed after the client went away (PATH N)");
}

#[tokio::test]
async fn client_disconnect_cancels_upstream_path_s() {
    let m = mock(|_, _| {
        let mut parts: Vec<(u64, String)> = vec![(0, message_start())];
        for _ in 0..40 {
            parts.push((100, PING.to_string()));
        }
        Reply::sse_timed(parts)
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 200);
    let mut s = r.bytes_stream();
    use futures_util::StreamExt;
    let first = s.next().await.unwrap().unwrap();
    assert_eq!(&first[..], PING.as_bytes(), "ping forwarded during HOLD");
    drop(s);
    for _ in 0..50 {
        if m.closed_early.load(Ordering::SeqCst) > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("upstream connection was not closed after the client went away (PATH S)");
}
