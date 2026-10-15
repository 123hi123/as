//! Anthropic error type <-> HTTP status mapping and error body builders.

use bytes::Bytes;
use serde_json::{json, Map, Value};

/// Error types that are *not* worth a silent retry (the request itself is the problem,
/// or the client must see the condition). Everything else — including unknown,
/// gateway-specific types such as `upstream_error` — is treated as transient.
const NON_TRANSIENT: &[&str] = &[
    "invalid_request_error",
    "authentication_error",
    "permission_error",
    "not_found_error",
    "request_too_large",
    "rate_limit_error",
    "billing_error",
];

/// Error types defined by the Anthropic API.
const STANDARD: &[&str] = &[
    "invalid_request_error",
    "authentication_error",
    "permission_error",
    "not_found_error",
    "request_too_large",
    "rate_limit_error",
    "billing_error",
    "api_error",
    "overloaded_error",
    "timeout_error",
    "internal_server_error",
];

pub fn is_transient(error_type: &str) -> bool {
    !NON_TRANSIENT.contains(&error_type)
}

pub fn is_standard(error_type: &str) -> bool {
    STANDARD.contains(&error_type)
}

/// HTTP status for a non-stream error response built from a stream error frame.
/// Returns `(status, error_type_to_send)`; non-standard types become 529 overloaded_error.
pub fn status_for_error_type(error_type: &str) -> (u16, &str) {
    match error_type {
        "overloaded_error" => (529, "overloaded_error"),
        "api_error" => (500, "api_error"),
        "internal_server_error" => (500, "internal_server_error"),
        "invalid_request_error" => (400, "invalid_request_error"),
        "authentication_error" => (401, "authentication_error"),
        "permission_error" => (403, "permission_error"),
        "not_found_error" => (404, "not_found_error"),
        "request_too_large" => (413, "request_too_large"),
        "rate_limit_error" => (429, "rate_limit_error"),
        "timeout_error" => (504, "timeout_error"),
        "billing_error" => (402, "billing_error"),
        _ => (529, "overloaded_error"),
    }
}

/// Error type for an SSE `event: error` frame synthesised from an HTTP status.
pub fn error_type_for_status(status: u16) -> &'static str {
    match status {
        400 => "invalid_request_error",
        401 => "authentication_error",
        402 => "billing_error",
        403 => "permission_error",
        404 => "not_found_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        408 | 409 => "overloaded_error",
        s if s >= 500 => "overloaded_error",
        _ => "invalid_request_error",
    }
}

pub fn error_value(error_type: &str, message: &str, request_id: Option<&str>) -> Value {
    let mut m = Map::new();
    m.insert("type".into(), Value::String("error".into()));
    m.insert(
        "error".into(),
        json!({ "type": error_type, "message": message }),
    );
    if let Some(id) = request_id {
        m.insert("request_id".into(), Value::String(id.to_string()));
    }
    Value::Object(m)
}

pub fn error_json(error_type: &str, message: &str, request_id: Option<&str>) -> Vec<u8> {
    serde_json::to_vec(&error_value(error_type, message, request_id)).unwrap_or_default()
}

/// A proper `event: error` SSE frame.
pub fn sse_error_frame(error_type: &str, message: &str) -> Bytes {
    let data = serde_json::to_string(&error_value(error_type, message, None)).unwrap_or_default();
    Bytes::from(format!("event: error\ndata: {data}\n\n"))
}

/// Extract `(type, message, request_id)` from an Anthropic error JSON
/// (`{"type":"error","error":{"type":..,"message":..},"request_id":..}`).
pub fn parse_error(v: &Value) -> (String, String, Option<String>) {
    let err = v.get("error");
    let ty = err
        .and_then(|e| e.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("api_error")
        .to_string();
    let msg = err
        .and_then(|e| e.get("message"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let rid = v
        .get("request_id")
        .and_then(|t| t.as_str())
        .map(str::to_string);
    (ty, msg, rid)
}

/// Pull a human readable message from an upstream error body (JSON or HTML).
pub fn body_message(body: &[u8]) -> String {
    if let Ok(v) = serde_json::from_slice::<Value>(body) {
        let (_, msg, _) = parse_error(&v);
        if !msg.is_empty() {
            return truncate(&msg, 300);
        }
        if let Some(m) = v.get("message").and_then(|m| m.as_str()) {
            return truncate(m, 300);
        }
    }
    if let Some(t) = html_title(body) {
        return t;
    }
    let s = String::from_utf8_lossy(body);
    truncate(s.trim(), 200)
}

/// `<title>` of an HTML page (Cloudflare error pages have a descriptive one).
pub fn html_title(body: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(&body[..body.len().min(64 * 1024)]);
    let lower = s.to_ascii_lowercase();
    let start = lower.find("<title")?;
    let open_end = lower[start..].find('>')? + start + 1;
    let end = lower[open_end..].find("</title>")? + open_end;
    let t = s[open_end..end].split_whitespace().collect::<Vec<_>>().join(" ");
    if t.is_empty() {
        None
    } else {
        Some(truncate(&t, 200))
    }
}

/// Truncate to `max` bytes (on a char boundary) and replace control characters with spaces.
pub fn truncate(s: &str, max: usize) -> String {
    let s: String = s
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping() {
        assert_eq!(status_for_error_type("rate_limit_error").0, 429);
        assert_eq!(status_for_error_type("upstream_error"), (529, "overloaded_error"));
        assert_eq!(status_for_error_type("billing_error").0, 402);
        assert_eq!(error_type_for_status(429), "rate_limit_error");
        assert_eq!(error_type_for_status(524), "overloaded_error");
        assert_eq!(error_type_for_status(500), "overloaded_error");
        assert_eq!(error_type_for_status(413), "request_too_large");
        assert_eq!(error_type_for_status(408), "overloaded_error");
        assert!(is_transient("upstream_error"));
        assert!(is_transient("api_error"));
        assert!(is_transient("overloaded_error"));
        assert!(!is_transient("rate_limit_error"));
        assert!(!is_transient("invalid_request_error"));
    }

    #[test]
    fn titles() {
        let html = b"<html><head><title>api.example.com | 524: A timeout occurred</title></head></html>";
        assert_eq!(
            html_title(html).unwrap(),
            "api.example.com | 524: A timeout occurred"
        );
        assert_eq!(
            body_message(br#"{"type":"error","error":{"type":"x","message":"boom"}}"#),
            "boom"
        );
        assert_eq!(body_message(b"line1\nline2\x1b[31m"), "line1 line2 [31m");
    }

    #[test]
    fn frame_shape() {
        let f = sse_error_frame("overloaded_error", "m");
        assert_eq!(
            &f[..],
            b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"m\"}}\n\n"
        );
    }
}
