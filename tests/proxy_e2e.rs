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
    method: String,
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
    let mut request_line = lines.next()?.split(' ');
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
    let mut headers = HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let mut body = buf[head_end..].to_vec();
    if let Some(len) = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
    {
        while body.len() < len {
            let n = sock.read(&mut tmp).await.ok()?;
            if n == 0 {
                return None;
            }
            body.extend_from_slice(&tmp[..n]);
        }
    } else if headers
        .get("transfer-encoding")
        .is_some_and(|v| v.contains("chunked"))
    {
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
        method,
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
        if reply.head == "<CLOSE>" {
            // Close before sending any response bytes (cut before headers).
            return Ok(());
        }
        if reply.head == "<HANG>" {
            // Never answer, keep the socket open (black-holed request).
            tokio::time::sleep(Duration::from_secs(3600)).await;
            return Ok(());
        }
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
    // Never write into the real ~/.local/state from tests.
    cfg.log_db = Some("off".into());
    tweak(&mut cfg);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        as_shim::proxy::serve(l, &cfg, std::future::pending())
            .await
            .unwrap();
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
        ev(
            "content_block_start",
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        ),
        PING.to_string(),
        ev(
            "content_block_delta",
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        ),
        ev(
            "content_block_stop",
            json!({"type":"content_block_stop","index":0}),
        ),
        ev(
            "message_delta",
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}),
        ),
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
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains("ended before message_stop"));
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
            Reply::plain(
                "200 OK",
                "application/json",
                "",
                r#"{"id":"x","content":[]}"#,
            )
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
        let m = mock(move |_, _| {
            Reply::plain(
                "524 A Timeout Occurred",
                "text/html; charset=UTF-8",
                &extra,
                CF524,
            )
        })
        .await;
        let port = shim(m.port, |_| {}).await;
        let r = post(port, &body(stream)).await;
        assert_eq!(r.status(), 529);
        let want_ra = if stream { "120" } else { "300" };
        assert_eq!(r.headers()["retry-after"], want_ra);
        assert_eq!(r.headers()["content-type"], "application/json");
        let v: Value = r.json().await.unwrap();
        assert_eq!(v["error"]["type"], "overloaded_error");
        assert!(v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not retried locally"));
        assert_eq!(
            m.seen.lock().unwrap().len(),
            1,
            "524 must never be retried locally"
        );
    }
}

#[tokio::test]
async fn cf_502_html_becomes_json_api_error() {
    let m = mock(|_, _| {
        Reply::plain(
            "502 Bad Gateway",
            "text/html",
            "",
            "<html><title>gw | 502: Bad gateway</title></html>",
        )
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 502);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["error"]["type"], "api_error");
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains("502: Bad gateway"));
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
    assert_eq!(
        text, want,
        "first attempt's message_start dropped, ping kept"
    );
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
    assert!(
        text.contains(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\""
        ),
        "{text}"
    );
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
            Reply::plain(
                "429 Too Many Requests",
                "application/json",
                "",
                r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#,
            )
        }
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert!(
        text.contains(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\""
        ),
        "{text}"
    );
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
    assert!(
        text.ends_with(DATA_ONLY_ERR),
        "frames after content are not rewritten: {text}"
    );
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
    assert!(
        n < 8,
        "breaker must stop multiplying a persistent outage (got {n} upstream requests)"
    );
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
        .post(format!(
            "http://127.0.0.1:{port}/v1/messages/count_tokens?beta=true"
        ))
        .header("connection", "keep-alive, x-secret")
        .header("x-secret", "drop-me")
        .body("{\"a\":1}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-up"], "1");
    assert_eq!(
        r.text().await.unwrap(),
        r#"{"path":"/v1/messages/count_tokens?beta=true","len":7}"#
    );
    let r = c
        .get(format!("http://127.0.0.1:{port}/v1/models"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = c
        .post(format!("http://127.0.0.1:{port}/v1/messages"))
        .body("not json")
        .send()
        .await
        .unwrap();
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

// ------------------------------------------------------------------------------ resume (protocol v1)

const RESUMABLE_HEAD: &str = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache, no-transform\r\nx-as-resume: 1\r\nrequest-id: req_resume\r\ntransfer-encoding: chunked\r\n";

impl Reply {
    /// A resumable SSE response (advertises `X-AS-Resume: 1`).
    fn resumable(parts: Vec<(u64, String)>) -> Self {
        Reply {
            head: RESUMABLE_HEAD.into(),
            parts: parts
                .into_iter()
                .filter(|(_, s)| !s.is_empty())
                .map(|(d, s)| (d, s.into_bytes()))
                .collect(),
            chunked: true,
        }
    }
    fn no_content() -> Self {
        Reply {
            head: "HTTP/1.1 204 No Content\r\n".into(),
            parts: vec![],
            chunked: false,
        }
    }
    fn not_found() -> Self {
        Reply::plain(
            "404 Not Found",
            "application/json",
            "",
            r#"{"type":"error","error":{"type":"not_found_error","message":"resume entry not found"}}"#,
        )
    }
}

fn offset_of(path: &str) -> usize {
    path.split("offset=")
        .nth(1)
        .unwrap()
        .split('&')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

/// `GET /__as/resume/{id}?offset=n` answered from `full`, in two chunks, then `tail`
/// (`<RESET>` to break the resume stream too, or nothing for a clean end).
fn resume_reply(
    full: &str,
    path: &str,
    state: &str,
    upto: Option<usize>,
    tail: Option<&str>,
) -> Reply {
    let off = offset_of(path);
    let end = upto.unwrap_or(full.len());
    let rest = &full[off..end];
    let mid = rest.len() / 2;
    let mut parts = vec![(0, rest[..mid].to_string()), (0, rest[mid..].to_string())];
    if let Some(t) = tail {
        parts.push((0, t.to_string()));
    }
    Reply {
        head: format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache, no-transform\r\nx-as-resume-state: {state}\r\nx-as-resume-length: {}\r\ntransfer-encoding: chunked\r\n",
            full.len()
        ),
        ..Reply::resumable(parts)
    }
}

fn requests(m: &Mock, method: &str, prefix: &str) -> Vec<Seen> {
    m.seen
        .lock()
        .unwrap()
        .iter()
        .filter(|s| s.method == method && s.path.starts_with(prefix))
        .cloned()
        .collect()
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..100 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

fn is_resume_id(s: &str) -> bool {
    s.len() == 32
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Mock for one resumable request: `post` scripts the original POST, `resume(n, seen)` the
/// n-th resume GET (0-based); DELETEs get 204.
async fn resume_mock<P, R>(post: P, resume: R) -> Mock
where
    P: Fn(usize) -> Reply + Send + Sync + 'static,
    R: Fn(usize, &Seen) -> Reply + Send + Sync + 'static,
{
    let posts = AtomicUsize::new(0);
    let gets = AtomicUsize::new(0);
    mock(move |_, req| match req.method.as_str() {
        "POST" => post(posts.fetch_add(1, Ordering::SeqCst)),
        "GET" if req.path.starts_with("/__as/resume/") => {
            resume(gets.fetch_add(1, Ordering::SeqCst), req)
        }
        "DELETE" => Reply::no_content(),
        other => panic!("unexpected {other} {}", req.path),
    })
    .await
}

fn full_text_stream(text: &str) -> String {
    text_frames(text).concat()
}

#[tokio::test]
async fn resume_after_mid_frame_drop_is_byte_identical() {
    let full = full_text_stream("resumed after a drop");
    let cut = full.find("text_delta").unwrap() + 4; // inside a frame, after content started
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())]),
        move |_, req| resume_reply(&f2, &req.path, "complete", None, None),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.text().await.unwrap(),
        full,
        "spliced output must be byte-identical"
    );

    let posts = requests(&m, "POST", "/v1/messages");
    assert_eq!(posts.len(), 1, "resume, never re-send");
    let id = posts[0].headers["x-as-resume-id"].clone();
    assert!(is_resume_id(&id), "{id}");
    assert_eq!(posts[0].headers["accept-encoding"], "identity");
    let gets = requests(&m, "GET", "/__as/resume/");
    assert_eq!(gets.len(), 1);
    assert_eq!(gets[0].path, format!("/__as/resume/{id}?offset={cut}"));
    assert_eq!(gets[0].headers["x-api-key"], "sk-test", "same credential");
    assert_eq!(gets[0].headers["accept-encoding"], "identity");
    wait_until("ack DELETE", || {
        !requests(&m, "DELETE", "/__as/resume/").is_empty()
    })
    .await;
    let dels = requests(&m, "DELETE", "/__as/resume/");
    assert_eq!(dels.len(), 1);
    assert_eq!(dels[0].path, format!("/__as/resume/{id}?reason=ack"));
    assert_eq!(dels[0].headers["x-api-key"], "sk-test");
}

#[tokio::test]
async fn resume_after_idle_black_hole() {
    let full = full_text_stream("idle then resumed");
    let cut = full.find("content_block_delta").unwrap() + 7;
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        // Sends part of the stream, then goes silent without closing.
        move |_| {
            Reply::resumable(vec![
                (0, f1[..cut].to_string()),
                (30_000, f1[cut..].to_string()),
            ])
        },
        move |_, req| resume_reply(&f2, &req.path, "live", None, None),
    )
    .await;
    let port = shim(m.port, |c| c.resume_idle_secs = 1).await;
    let t = std::time::Instant::now();
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert_eq!(text, full);
    assert!(t.elapsed() < Duration::from_secs(10));
    let gets = requests(&m, "GET", "/__as/resume/");
    assert_eq!(gets.len(), 1);
    assert_eq!(offset_of(&gets[0].path), cut);
    assert_eq!(requests(&m, "POST", "/").len(), 1);
}

#[tokio::test]
async fn resume_after_clean_eof_before_message_stop() {
    let full = full_text_stream("eof early");
    let cut = full.find("event: content_block_stop").unwrap(); // frame boundary
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<EOF>".into())]),
        move |_, req| resume_reply(&f2, &req.path, "complete", None, None),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    assert_eq!(
        offset_of(&requests(&m, "GET", "/__as/resume/")[0].path),
        cut
    );
    wait_until("ack DELETE", || {
        requests(&m, "DELETE", "/__as/resume/")
            .iter()
            .any(|d| d.path.ends_with("?reason=ack"))
    })
    .await;
}

#[tokio::test]
async fn resume_twice_when_the_resume_stream_breaks_too() {
    let full = full_text_stream("two breaks in one response");
    let cut1 = full.find("content_block_start").unwrap() + 3;
    let cut2 = full.find("text_delta").unwrap() + 2;
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut1].to_string()), (0, "<RESET>".into())]),
        move |n, req| {
            if n == 0 {
                resume_reply(&f2, &req.path, "live", Some(cut2), Some("<RESET>"))
            } else {
                resume_reply(&f2, &req.path, "complete", None, None)
            }
        },
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    let offs: Vec<usize> = requests(&m, "GET", "/__as/resume/")
        .iter()
        .map(|g| offset_of(&g.path))
        .collect();
    assert_eq!(offs, vec![cut1, cut2]);
}

#[tokio::test]
async fn resume_404_after_content_falls_back_to_abort() {
    let full = full_text_stream("lost");
    let cut = full.find("text_delta").unwrap();
    let f1 = full.clone();
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())]),
        |_, _| Reply::not_found(),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(r.status(), 200);
    let res = r.bytes().await;
    assert!(
        res.is_err(),
        "client connection must be aborted, got {res:?}"
    );
    assert_eq!(
        requests(&m, "GET", "/__as/resume/").len(),
        1,
        "404 is final"
    );
    assert_eq!(
        requests(&m, "POST", "/").len(),
        1,
        "no re-send after content"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        requests(&m, "DELETE", "/").is_empty(),
        "nothing to delete after 404"
    );
}

#[tokio::test]
async fn resume_pre_content_does_not_resend() {
    let full = full_text_stream("no duplicate generation");
    let cut = full.find("msg_1").unwrap(); // inside message_start: before any content
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |n| {
            assert_eq!(n, 0, "the POST must not be re-sent");
            Reply::resumable(vec![
                (0, PING.to_string()),
                (0, f1[..cut].to_string()),
                (0, "<RESET>".into()),
            ])
        },
        move |_, req| resume_reply(&format!("{PING}{f2}"), &req.path, "complete", None, None),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert_eq!(text, format!("{PING}{full}"));
    assert_eq!(requests(&m, "POST", "/").len(), 1);
    assert_eq!(
        offset_of(&requests(&m, "GET", "/__as/resume/")[0].path),
        PING.len() + cut
    );
}

#[tokio::test]
async fn resume_pre_content_404_falls_back_to_silent_resend() {
    let full = full_text_stream("second try");
    let cut = full.find("msg_1").unwrap();
    let f1 = full.clone();
    let m = resume_mock(
        move |n| {
            if n == 0 {
                Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())])
            } else {
                Reply::resumable(vec![(0, f1.clone())])
            }
        },
        |_, _| Reply::not_found(),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    let posts = requests(&m, "POST", "/");
    assert_eq!(
        posts.len(),
        2,
        "pre-existing silent re-send after resume gave up"
    );
    assert_ne!(
        posts[0].headers["x-as-resume-id"], posts[1].headers["x-as-resume-id"],
        "new id per attempt"
    );
}

#[tokio::test]
async fn resume_attempt_cut_before_headers_is_cancelled_then_resent() {
    let full = full_text_stream("cut before headers");
    let f1 = full.clone();
    let m = resume_mock(
        move |n| {
            if n == 0 {
                Reply {
                    head: "<CLOSE>".into(),
                    parts: vec![],
                    chunked: false,
                }
            } else {
                Reply::resumable(vec![(0, f1.clone())])
            }
        },
        |_, _| panic!("no resume expected"),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    let posts = requests(&m, "POST", "/");
    assert_eq!(posts.len(), 2, "no headers: legacy silent re-send");
    let id0 = posts[0].headers["x-as-resume-id"].clone();
    let id1 = posts[1].headers["x-as-resume-id"].clone();
    assert_ne!(id0, id1);
    wait_until("cancel and ack DELETEs", || {
        requests(&m, "DELETE", "/__as/resume/").len() >= 2
    })
    .await;
    let dels = requests(&m, "DELETE", "/__as/resume/");
    let mut paths: Vec<&str> = dels.iter().map(|s| s.path.as_str()).collect();
    paths.sort();
    let mut want = vec![
        format!("/__as/resume/{id0}?reason=cancel"),
        format!("/__as/resume/{id1}?reason=ack"),
    ];
    want.sort();
    assert_eq!(paths, want, "the unanswered attempt is cancelled");
    assert!(dels.iter().all(|d| d.headers["x-api-key"] == "sk-test"));
}

#[tokio::test]
async fn ack_delete_is_retried_once_after_a_dropped_connection() {
    let full = full_text_stream("ack retried");
    let f1 = full.clone();
    let dels = AtomicUsize::new(0);
    let m = mock(move |_, req| match req.method.as_str() {
        "POST" => Reply::resumable(vec![(0, f1.clone())]),
        // The first DELETE hits a connection the server closes without answering.
        "DELETE" if dels.fetch_add(1, Ordering::SeqCst) == 0 => Reply {
            head: "<CLOSE>".into(),
            parts: vec![],
            chunked: false,
        },
        "DELETE" => Reply::no_content(),
        other => panic!("unexpected {other} {}", req.path),
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    wait_until("ack retried", || {
        requests(&m, "DELETE", "/__as/resume/").len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let d = requests(&m, "DELETE", "/__as/resume/");
    assert_eq!(d.len(), 2, "exactly one retry");
    assert_eq!(d[0].path, d[1].path);
    assert!(d[0].path.ends_with("?reason=ack"), "{}", d[0].path);
}

#[tokio::test]
async fn resume_path_n_aggregates_across_a_break() {
    let full = full_text_stream("non-stream resumed");
    let cut = full.find("text_delta").unwrap() + 1;
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())]),
        move |_, req| resume_reply(&f2, &req.path, "complete", None, None),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(false)).await;
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["content"][0]["text"], "non-stream resumed");
    assert_eq!(v["stop_reason"], "end_turn");
    let posts = requests(&m, "POST", "/");
    assert_eq!(posts.len(), 1);
    let id = posts[0].headers["x-as-resume-id"].clone();
    assert_eq!(
        requests(&m, "GET", "/__as/resume/")[0].path,
        format!("/__as/resume/{id}?offset={cut}")
    );
    wait_until("ack DELETE", || {
        requests(&m, "DELETE", "/")
            .iter()
            .any(|d| d.path == format!("/__as/resume/{id}?reason=ack"))
    })
    .await;
}

#[tokio::test]
async fn resume_cancel_delete_when_client_disconnects() {
    let m = resume_mock(
        |_| {
            let f = text_frames("x");
            let mut parts = vec![(0, f[0].clone()), (0, f[1].clone())];
            for _ in 0..40 {
                parts.push((100, PING.to_string()));
            }
            Reply::resumable(parts)
        },
        |_, _| panic!("no resume expected"),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    let mut s = r.bytes_stream();
    use futures_util::StreamExt;
    let _ = s.next().await.unwrap().unwrap();
    drop(s);
    wait_until("cancel DELETE", || !requests(&m, "DELETE", "/").is_empty()).await;
    let id = requests(&m, "POST", "/")[0].headers["x-as-resume-id"].clone();
    let dels = requests(&m, "DELETE", "/");
    assert_eq!(dels[0].path, format!("/__as/resume/{id}?reason=cancel"));
    assert!(requests(&m, "GET", "/__as/resume/").is_empty());
}

#[tokio::test]
async fn server_without_resume_header_keeps_legacy_behavior() {
    let full = full_text_stream("legacy");
    let cut = full.find("text_delta").unwrap();
    let f1 = full.clone();
    let m = mock(move |_, req| {
        assert_eq!(
            req.method, "POST",
            "no resume traffic for a non-resumable response"
        );
        Reply::sse(&[&f1[..cut], "<RESET>"])
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    let res = post(port, &body(true)).await.bytes().await;
    assert!(res.is_err(), "abort, as before");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let seen = m.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(
        is_resume_id(&seen[0].headers["x-as-resume-id"]),
        "opt-in header is still sent"
    );
}

#[tokio::test]
async fn resume_disabled_by_config() {
    let full = full_text_stream("off");
    let cut = full.find("text_delta").unwrap();
    let f1 = full.clone();
    let m = mock(move |_, req| {
        assert_eq!(req.method, "POST");
        Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())])
    })
    .await;
    let port = shim(m.port, |c| c.resume = false).await;
    assert!(post(port, &body(true)).await.bytes().await.is_err());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let seen = m.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(!seen[0].headers.contains_key("x-as-resume-id"));
}

#[tokio::test]
async fn resume_conflict_409_is_resent_with_a_new_id() {
    let full = full_text_stream("after conflict");
    let f1 = full.clone();
    let m = resume_mock(
        move |n| {
            if n == 0 {
                Reply::plain("409 Conflict", "application/json", "", r#"{"type":"error","error":{"type":"invalid_request_error","message":"resume id already in use"}}"#)
            } else {
                Reply::resumable(vec![(0, f1.clone())])
            }
        },
        |_, _| panic!("no resume expected"),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    let posts = requests(&m, "POST", "/");
    assert_eq!(posts.len(), 2);
    assert_ne!(
        posts[0].headers["x-as-resume-id"],
        posts[1].headers["x-as-resume-id"]
    );
}

#[tokio::test]
async fn resume_server_complete_without_message_stop_ends_cleanly() {
    let mut frames = text_frames("cut short");
    frames.pop(); // the upstream itself never sends message_stop
    frames.pop();
    let full = frames.concat();
    let f1 = full.clone();
    let f2 = full.clone();
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1.clone()), (0, "<EOF>".into())]),
        // Nothing left: empty body, `X-AS-Resume-Length` == offset.
        move |_, req| resume_reply(&f2, &req.path, "complete", None, None),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = post(port, &body(true)).await;
    assert_eq!(
        r.text().await.unwrap(),
        full,
        "clean end, as without resume"
    );
    assert_eq!(requests(&m, "GET", "/__as/resume/").len(), 1);
}

#[tokio::test]
async fn resume_body_cut_cleanly_before_the_server_length_is_resumed_again() {
    // `X-AS-Resume-State` only describes the moment the resume response started: a resume body
    // that ends cleanly before `X-AS-Resume-Length` was truncated on the way, not finished.
    for state in ["complete", "failed"] {
        let full = full_text_stream(&format!("resume body truncated, state {state}"));
        let cut1 = full.find("text_delta").unwrap();
        let cut2 = full.find("event: message_delta").unwrap() + 5;
        let (f1, f2) = (full.clone(), full.clone());
        let m = resume_mock(
            move |_| Reply::resumable(vec![(0, f1[..cut1].to_string()), (0, "<RESET>".into())]),
            move |n, req| {
                // The first resume body ends with a proper chunked terminator at cut2.
                let upto = (n == 0).then_some(cut2);
                resume_reply(&f2, &req.path, state, upto, None)
            },
        )
        .await;
        let port = shim(m.port, |_| {}).await;
        let r = post(port, &body(true)).await;
        assert_eq!(r.text().await.unwrap(), full, "state {state}");
        let offs: Vec<usize> = requests(&m, "GET", "/__as/resume/")
            .iter()
            .map(|g| offset_of(&g.path))
            .collect();
        assert_eq!(offs, vec![cut1, cut2], "state {state}");
        wait_until("ack DELETE", || {
            !requests(&m, "DELETE", "/__as/resume/").is_empty()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let dels = requests(&m, "DELETE", "/__as/resume/");
        assert_eq!(dels.len(), 1, "state {state}");
        assert!(dels[0].path.ends_with("?reason=ack"), "{}", dels[0].path);
    }
}

#[tokio::test]
async fn resume_final_state_without_length_header_is_trusted() {
    // Older servers without `X-AS-Resume-Length`: keep trusting the state at a clean end.
    let full = full_text_stream("no length header");
    let cut1 = full.find("text_delta").unwrap();
    let cut2 = full.find("event: message_delta").unwrap();
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut1].to_string()), (0, "<RESET>".into())]),
        move |_, req| {
            let mut r = resume_reply(&f2, &req.path, "complete", Some(cut2), None);
            r.head = r
                .head
                .lines()
                .filter(|l| !l.starts_with("x-as-resume-length"))
                .map(|l| format!("{l}\r\n"))
                .collect();
            r
        },
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert_eq!(text, full[..cut2]);
    assert_eq!(requests(&m, "GET", "/__as/resume/").len(), 1);
}

#[tokio::test]
async fn resume_failed_state_at_full_length_aborts() {
    let mut frames = text_frames("upstream broke");
    frames.truncate(4); // the upstream died after the first delta
    let full = frames.concat();
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1.clone()), (0, "<EOF>".into())]),
        move |_, req| resume_reply(&f2, &req.path, "failed", None, None),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let res = post(port, &body(true)).await.bytes().await;
    assert!(res.is_err(), "server-side failure aborts, got {res:?}");
    let gets = requests(&m, "GET", "/__as/resume/");
    assert_eq!(gets.len(), 1);
    assert_eq!(offset_of(&gets[0].path), full.len());
}

#[tokio::test]
async fn resume_get_and_delete_never_use_stale_pooled_connections() {
    // A black-holed network path usually kills the idle pooled connections too: resume traffic
    // must go out on fresh connections instead of waiting on each dead one in turn.
    use std::sync::atomic::AtomicBool;
    let full = full_text_stream("fresh connections for resume");
    let cut = full.find("text_delta").unwrap();
    let blackhole = Arc::new(AtomicBool::new(false));
    let stale_used = Arc::new(AtomicUsize::new(0));
    let gets = Arc::new(AtomicUsize::new(0));
    let dels = Arc::new(Mutex::new(Vec::<String>::new()));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_port = l.local_addr().unwrap().port();
    {
        let (bh, stale, gets, dels, full) = (
            blackhole.clone(),
            stale_used.clone(),
            gets.clone(),
            dels.clone(),
            full.clone(),
        );
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = l.accept().await.unwrap();
                let born_before = !bh.load(Ordering::SeqCst);
                let (bh, stale, gets, dels, full) = (
                    bh.clone(),
                    stale.clone(),
                    gets.clone(),
                    dels.clone(),
                    full.clone(),
                );
                tokio::spawn(async move {
                    while let Some(req) = read_request(&mut sock).await {
                        if born_before && bh.load(Ordering::SeqCst) {
                            // Black-holed: the request vanishes, nothing comes back.
                            stale.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                            return;
                        }
                        let out = match req.method.as_str() {
                            "GET" if req.path.starts_with("/v1/models") => {
                                // Slow enough that the warm-up requests need 3 connections.
                                tokio::time::sleep(Duration::from_millis(300)).await;
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}".to_string()
                            }
                            "POST" => {
                                let out = format!(
                                    "{RESUMABLE_HEAD}\r\n{:x}\r\n{}\r\n",
                                    cut,
                                    &full[..cut]
                                );
                                let _ = sock.write_all(out.as_bytes()).await;
                                bh.store(true, Ordering::SeqCst);
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                                return;
                            }
                            "GET" => {
                                gets.fetch_add(1, Ordering::SeqCst);
                                let rest = &full[offset_of(&req.path)..];
                                format!(
                                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nx-as-resume-state: complete\r\nx-as-resume-length: {}\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                                    full.len(),
                                    rest.len(),
                                    rest
                                )
                            }
                            _ => {
                                dels.lock().unwrap().push(req.path.clone());
                                "HTTP/1.1 204 No Content\r\n\r\n".to_string()
                            }
                        };
                        if sock.write_all(out.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
    }
    let port = shim(up_port, |c| c.resume_idle_secs = 1).await;
    // Warm the shim's connection pool with 3 idle keep-alive connections.
    let c = client();
    let warm = (0..3).map(|_| {
        c.get(format!("http://127.0.0.1:{port}/v1/models"))
            .header("x-api-key", "sk-test")
            .send()
    });
    for r in futures_util::future::join_all(warm).await {
        assert_eq!(r.unwrap().status(), 200);
    }
    let t = std::time::Instant::now();
    let text = post(port, &body(true)).await.text().await.unwrap();
    assert_eq!(text, full);
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    for _ in 0..100 {
        if !dels.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(gets.load(Ordering::SeqCst), 1);
    assert_eq!(
        stale_used.load(Ordering::SeqCst),
        0,
        "resume traffic went out on a stale pooled connection"
    );
    let d = dels.lock().unwrap().clone();
    assert_eq!(d.len(), 1, "{d:?}");
    assert!(d[0].ends_with("?reason=ack"), "{d:?}");
}

#[tokio::test]
async fn delete_without_an_answer_is_retried_on_a_new_connection() {
    let full = full_text_stream("cancel retried after a timeout");
    let f1 = full.clone();
    let dels = AtomicUsize::new(0);
    let m = mock(move |_, req| match req.method.as_str() {
        "POST" => Reply::resumable(vec![(0, f1.clone())]),
        // The first DELETE is swallowed: no answer, no close.
        "DELETE" if dels.fetch_add(1, Ordering::SeqCst) == 0 => Reply {
            head: "<HANG>".into(),
            parts: vec![],
            chunked: false,
        },
        "DELETE" => Reply::no_content(),
        other => panic!("unexpected {other} {}", req.path),
    })
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    for _ in 0..300 {
        if requests(&m, "DELETE", "/__as/resume/").len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let d = requests(&m, "DELETE", "/__as/resume/");
    assert_eq!(d.len(), 2, "one retry after the timeout");
    assert_eq!(d[0].path, d[1].path);
    assert!(d[0].path.ends_with("?reason=ack"), "{}", d[0].path);
}

// ------------------------------------------------------------------------------ SQLite log

fn temp_db(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("as-shim-e2e-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir.join("log.sqlite3")
}

fn db_count(path: &std::path::Path, sql: &str) -> i64 {
    match rusqlite::Connection::open(path) {
        Ok(c) => c.query_row(sql, [], |r| r.get(0)).unwrap_or(0),
        Err(_) => 0,
    }
}

#[tokio::test]
async fn sqlite_log_records_resumed_request_and_events() {
    let db = temp_db("resume");
    let full = full_text_stream("logged");
    let cut = full.find("text_delta").unwrap() + 4;
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())]),
        move |_, req| resume_reply(&f2, &req.path, "complete", None, None),
    )
    .await;
    let dbp = db.clone();
    let port = shim(m.port, move |c| {
        c.log_db = Some(dbp.to_string_lossy().into_owned())
    })
    .await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    let r = client()
        .get(format!("http://127.0.0.1:{port}/v1/models"))
        .send()
        .await
        .unwrap();
    let _ = r.text().await;
    wait_until("rows", || {
        db_count(&db, "SELECT count(*) FROM requests") >= 2
    })
    .await;

    let id = requests(&m, "POST", "/")[0].headers["x-as-resume-id"].clone();
    let c = rusqlite::Connection::open(&db).unwrap();
    let mode: String = c
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    type Row = (
        String,
        String,
        Option<String>,
        Option<bool>,
        String,
        i64,
        i64,
        i64,
        String,
        i64,
        String,
        i64,
        bool,
        String,
        Option<i64>,
    );
    let row: Row = c
        .query_row(
            "SELECT method, path, model, client_stream, mode, status, upstream_status, bytes_out, outcome, \
             silent_retries, resume_id, resume_attempts, resume_recovered, upstream_request_id, ttfb_ms \
             FROM requests WHERE path = '/v1/messages'",
            [],
            |r| {
                Ok((
                    r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?,
                    r.get(8)?, r.get(9)?, r.get(10)?, r.get(11)?, r.get(12)?, r.get(13)?, r.get(14)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(row.0, "POST");
    assert_eq!(row.1, "/v1/messages");
    assert_eq!(row.2.as_deref(), Some("m"));
    assert_eq!(row.3, Some(true));
    assert_eq!(row.4, "S");
    assert_eq!((row.5, row.6), (200, 200));
    assert_eq!(row.7, full.len() as i64);
    assert_eq!(row.8, "ok");
    assert_eq!(row.9, 0);
    assert_eq!(row.10, id);
    assert_eq!(row.11, 1);
    assert!(row.12, "resume_recovered");
    assert_eq!(row.13, "req_resume");
    assert!(row.14.is_some(), "ttfb_ms");
    let ev: (String, i64, i64, String, String) = c
        .query_row(
            "SELECT resume_id, attempt, \"offset\", \"trigger\", result FROM resume_events",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        ev,
        (id, 1, cut as i64, "io_error".to_string(), "ok".to_string())
    );
    let pt: (String, Option<String>, Option<bool>) = c
        .query_row(
            "SELECT mode, model, client_stream FROM requests WHERE path = '/v1/models'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(pt, ("passthrough".to_string(), None, None));
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

#[tokio::test]
async fn sqlite_log_records_budget_exhaustion() {
    let db = temp_db("budget");
    let full = full_text_stream("budget");
    let cut = full.find("text_delta").unwrap();
    let f1 = full.clone();
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())]),
        |_, _| {
            Reply::plain(
                "503 Service Unavailable",
                "application/json",
                "",
                r#"{"error":"down"}"#,
            )
        },
    )
    .await;
    let dbp = db.clone();
    let port = shim(m.port, move |c| {
        c.log_db = Some(dbp.to_string_lossy().into_owned());
        c.resume_budget_secs = 1;
    })
    .await;
    assert!(
        post(port, &body(true)).await.bytes().await.is_err(),
        "give up = abort"
    );
    wait_until("row", || {
        db_count(&db, "SELECT count(*) FROM requests") >= 1
    })
    .await;
    let c = rusqlite::Connection::open(&db).unwrap();
    let results: Vec<String> = c
        .prepare("SELECT result FROM resume_events ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(results.len() >= 2, "{results:?}");
    assert!(
        results[..results.len() - 1].iter().all(|r| r == "http_503"),
        "{results:?}"
    );
    assert_eq!(results.last().unwrap(), "budget");
    let (outcome, recovered, err): (String, bool, Option<String>) = c
        .query_row(
            "SELECT outcome, resume_recovered, error FROM requests",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(outcome, "resume_gave_up_after_content");
    assert!(!recovered);
    assert!(err.unwrap().contains("budget"));
    wait_until("cancel DELETE", || {
        requests(&m, "DELETE", "/")
            .iter()
            .any(|d| d.path.ends_with("?reason=cancel"))
    })
    .await;
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

#[tokio::test]
async fn sqlite_log_retention_cleanup_at_startup() {
    let db = temp_db("retention");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    {
        let mut c = as_shim::logdb::open(&db).unwrap();
        let old = |days: i64, path: &str| as_shim::logdb::RequestRow {
            ts_start_ms: now - days * 86_400_000,
            ts_end_ms: now - days * 86_400_000 + 1,
            method: "GET".into(),
            path: path.into(),
            mode: "passthrough".into(),
            outcome: "ok".into(),
            ..Default::default()
        };
        as_shim::logdb::write_rows(&mut c, &[old(40, "/old"), old(3, "/recent")]).unwrap();
    }
    let m = mock(|_, _| Reply::plain("200 OK", "application/json", "", "{}")).await;
    let dbp = db.clone();
    let port = shim(m.port, move |c| {
        c.log_db = Some(dbp.to_string_lossy().into_owned());
        c.log_retention_days = 30;
    })
    .await;
    let _ = client()
        .get(format!("http://127.0.0.1:{port}/v1/models"))
        .send()
        .await
        .unwrap()
        .text()
        .await;
    wait_until("new row", || {
        db_count(
            &db,
            "SELECT count(*) FROM requests WHERE path = '/v1/models'",
        ) == 1
    })
    .await;
    assert_eq!(
        db_count(&db, "SELECT count(*) FROM requests WHERE path = '/old'"),
        0,
        "expired row removed"
    );
    assert_eq!(
        db_count(&db, "SELECT count(*) FROM requests WHERE path = '/recent'"),
        1
    );
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

#[tokio::test]
async fn resume_skips_non_sse_200_and_forwards_bearer_credential() {
    let full = full_text_stream("bearer");
    let cut = full.find("text_delta").unwrap() + 2;
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())]),
        move |n, req| {
            if n == 0 {
                Reply::plain("200 OK", "text/html", "", "<html>captive portal</html>")
            } else {
                resume_reply(&f2, &req.path, "complete", None, None)
            }
        },
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    let r = client()
        .post(format!("http://127.0.0.1:{port}/v1/messages"))
        .header("authorization", "Bearer sk-fake-token")
        .json(&body(true))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.text().await.unwrap(),
        full,
        "the HTML page must never be spliced in"
    );
    let gets = requests(&m, "GET", "/__as/resume/");
    assert_eq!(gets.len(), 2);
    for g in &gets {
        assert_eq!(g.headers["authorization"], "Bearer sk-fake-token");
        assert!(!g.headers.contains_key("x-api-key"));
    }
}

// ------------------------------------------------------------------------------ resume deadline

/// Reads a PATH S response to the end; returns (time of the last chunk, time the stream ended,
/// whether it ended with an error, bytes).
async fn read_timed(
    r: reqwest::Response,
) -> (std::time::Instant, std::time::Instant, bool, Vec<u8>) {
    use futures_util::StreamExt;
    let mut s = r.bytes_stream();
    let mut last = std::time::Instant::now();
    let mut out = Vec::new();
    loop {
        match s.next().await {
            Some(Ok(b)) => {
                if !b.is_empty() {
                    last = std::time::Instant::now();
                    out.extend_from_slice(&b);
                }
            }
            Some(Err(_)) => return (last, std::time::Instant::now(), true, out),
            None => return (last, std::time::Instant::now(), false, out),
        }
    }
}

#[tokio::test]
async fn resume_episode_never_outlasts_idle_plus_budget() {
    // Black hole after content; the first two resume GETs fail, the third answers 200 but its
    // body stays silent. Every wait (including the idle wait on that resumed body) must end at
    // one deadline: last byte + resume_idle_secs + resume_budget_secs.
    let full = full_text_stream("deadline");
    let cut = full.find("text_delta").unwrap() + 4;
    let f1 = full.clone();
    let m = resume_mock(
        move |_| {
            Reply::resumable(vec![
                (0, f1[..cut].to_string()),
                (30_000, f1[cut..].to_string()),
            ])
        },
        move |n, _| {
            if n < 2 {
                Reply::plain("503 Service Unavailable", "application/json", "", "{}")
            } else {
                let mut r = Reply::resumable(vec![(30_000, "event: ping\n\n".into())]);
                r.head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nx-as-resume-state: live\r\ntransfer-encoding: chunked\r\n".into();
                r
            }
        },
    )
    .await;
    let port = shim(m.port, |c| {
        c.resume_idle_secs = 2;
        c.resume_budget_secs = 2;
    })
    .await;
    let r = post(port, &body(true)).await;
    let (last, end, aborted, out) = tokio::time::timeout(Duration::from_secs(20), read_timed(r))
        .await
        .expect("stream must end");
    assert!(aborted, "give up = abort");
    assert_eq!(out, &full.as_bytes()[..cut]);
    let silence = end - last;
    assert!(
        silence >= Duration::from_millis(3700) && silence <= Duration::from_millis(4600),
        "Claude Code saw {silence:?} of silence; the cap is idle 2 s + budget 2 s"
    );
    assert_eq!(requests(&m, "GET", "/__as/resume/").len(), 3);
    wait_until("cancel DELETE", || {
        requests(&m, "DELETE", "/__as/resume/")
            .iter()
            .any(|d| d.path.ends_with("?reason=cancel"))
    })
    .await;
}

#[tokio::test]
async fn resumed_body_wait_is_capped_even_without_idle_detection() {
    // resume_idle_secs = 0: a silent resumed body used to hang forever.
    let full = full_text_stream("no idle detection");
    let cut = full.find("text_delta").unwrap() + 4;
    let f1 = full.clone();
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<RESET>".into())]),
        |_, _| {
            let mut r = Reply::resumable(vec![(30_000, "event: ping\n\n".into())]);
            r.head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nx-as-resume-state: live\r\ntransfer-encoding: chunked\r\n".into();
            r
        },
    )
    .await;
    let port = shim(m.port, |c| {
        c.resume_idle_secs = 0;
        c.resume_budget_secs = 2;
    })
    .await;
    let r = post(port, &body(true)).await;
    let (last, end, aborted, _) = tokio::time::timeout(Duration::from_secs(15), read_timed(r))
        .await
        .expect("a silent resumed body must not hang past the budget");
    assert!(aborted);
    assert!(
        end - last <= Duration::from_millis(2600),
        "{:?}",
        end - last
    );
}

// ------------------------------------------------------------------------------ terminal frame

#[tokio::test]
async fn eof_inside_the_final_frame_is_resumed_not_treated_as_the_end() {
    // A clean EOF before the blank line that ends `message_stop` (or `error`): the client's SSE
    // decoder would drop that unterminated event, so fetch the missing bytes.
    let full = full_text_stream("terminal frame cut");
    let stop_at = full.find("event: message_stop").unwrap();
    for cut in [full.len() - 1, stop_at + "event: message_stop\n".len() + 3] {
        let (f1, f2) = (full.clone(), full.clone());
        let m = resume_mock(
            move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<EOF>".into())]),
            move |_, req| resume_reply(&f2, &req.path, "complete", None, None),
        )
        .await;
        let port = shim(m.port, |_| {}).await;
        let text = post(port, &body(true)).await.text().await.unwrap();
        assert_eq!(
            text, full,
            "cut {cut}: the final blank line must reach the client"
        );
        let gets = requests(&m, "GET", "/__as/resume/");
        assert_eq!(gets.len(), 1, "cut {cut}");
        assert_eq!(offset_of(&gets[0].path), cut);
        wait_until("ack DELETE", || {
            requests(&m, "DELETE", "/__as/resume/")
                .iter()
                .any(|d| d.path.ends_with("?reason=ack"))
        })
        .await;
    }
    // Same for a terminal `error` event (PATH N aggregates it into an error response).
    let mut frames = text_frames("err");
    frames.truncate(4);
    frames.push(
        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
            .to_string(),
    );
    let full = frames.concat();
    let cut = full.len() - 1;
    let (f1, f2) = (full.clone(), full.clone());
    let m = resume_mock(
        move |_| Reply::resumable(vec![(0, f1[..cut].to_string()), (0, "<EOF>".into())]),
        move |_, req| resume_reply(&f2, &req.path, "complete", None, None),
    )
    .await;
    let port = shim(m.port, |_| {}).await;
    assert_eq!(post(port, &body(true)).await.text().await.unwrap(), full);
    assert_eq!(requests(&m, "GET", "/__as/resume/").len(), 1);
}

// ------------------------------------------------------------------------------ cancel while waiting for headers

#[tokio::test]
async fn client_disconnect_while_waiting_for_headers_cancels_the_attempt() {
    for stream in [true, false] {
        // The POST is admitted upstream but its response headers never come.
        let m = resume_mock(
            |_| Reply {
                head: "<HANG>".into(),
                parts: vec![],
                chunked: false,
            },
            |_, _| panic!("no resume expected"),
        )
        .await;
        let port = shim(m.port, |_| {}).await;
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let b = body(stream).to_string();
        let req = format!(
            "POST /v1/messages HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nx-api-key: sk-test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{b}",
            b.len()
        );
        s.write_all(req.as_bytes()).await.unwrap();
        wait_until("POST upstream", || !requests(&m, "POST", "/").is_empty()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(s); // Esc in Claude Code
        wait_until("cancel DELETE", || {
            !requests(&m, "DELETE", "/__as/resume/").is_empty()
        })
        .await;
        let id = requests(&m, "POST", "/")[0].headers["x-as-resume-id"].clone();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let dels = requests(&m, "DELETE", "/__as/resume/");
        assert_eq!(dels.len(), 1, "stream={stream}");
        assert_eq!(dels[0].path, format!("/__as/resume/{id}?reason=cancel"));
        assert_eq!(dels[0].headers["x-api-key"], "sk-test");
    }
}

#[tokio::test]
async fn completed_requests_send_only_the_ack() {
    // The pending-headers guard must stand down once the response arrives.
    for stream in [true, false] {
        let full = full_text_stream("just an ack");
        let f1 = full.clone();
        let m = resume_mock(
            move |_| Reply::resumable(vec![(0, f1.clone())]),
            |_, _| panic!("no resume expected"),
        )
        .await;
        let port = shim(m.port, |_| {}).await;
        let r = post(port, &body(stream)).await;
        assert_eq!(r.status(), 200);
        let _ = r.bytes().await.unwrap();
        wait_until("ack DELETE", || {
            !requests(&m, "DELETE", "/__as/resume/").is_empty()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let dels = requests(&m, "DELETE", "/__as/resume/");
        assert_eq!(dels.len(), 1, "stream={stream}: {dels:?}");
        assert!(dels[0].path.ends_with("?reason=ack"), "{}", dels[0].path);
    }
}

// ------------------------------------------------------------------------------ sanitize

#[tokio::test]
async fn sanitize_strips_synthetic_and_refusal_before_upstream() {
    let m = mock(|_, _| Reply::sse(&[text_frames("ok")[0].as_str(), "<EOF>"])).await;
    let port = shim(m.port, |_| {}).await;
    let mut b = body(true);
    b["messages"][0]["content"] = json!([
        {"type": "text", "text": "hi"},
        {"type": "tool_result", "tool_use_id": "t1",
         "content": "API Error: 400 Anthropic refused this request under its usage policy. Do not retry this request."}
    ]);
    b["messages"].as_array_mut().unwrap().push(json!(
        {"role": "assistant", "model": "<synthetic>",
         "content": [{"type": "text",
                      "text": "API Error: Anthropic refused this request under its usage policy. Do not retry this request."}]}
    ));
    let resp = post(port, &b).await;
    assert_eq!(resp.status(), 200);
    let seen = m.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let sent: Value = serde_json::from_slice(&seen[0].body).unwrap();
    let msgs = sent["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1, "synthetic message must be dropped: {msgs:?}");
    let contents = &msgs[0]["content"];
    assert!(
        !serde_json::to_string(contents)
            .unwrap()
            .contains("Anthropic refused"),
        "marker must be blanked before the request leaves: {contents:?}"
    );
}
