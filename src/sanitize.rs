//! Strip poisoned history from Claude Code request bodies before they go upstream.
//!
//! Claude Code writes API errors into session history in two shapes that then ride along
//! with every subsequent request and can re-trigger the usage-policy refusal:
//!
//! 1. an assistant message with `"model": "<synthetic>"` whose content is the error text;
//! 2. a user `tool_result` (or text) block containing the literal error string.
//!
//! We drop shape 1 entirely and blank the marker out of shape 2, so request structure
//! (tool_use / tool_result pairing) stays valid while the flagged text never reaches the API.

use serde_json::Value;

/// The literal error text Claude Code stores when Anthropic refuses a request.
pub const REFUSED_MARKER: &str =
    "API Error: Anthropic refused this request under its usage policy. Do not retry this request.";

/// The model field Claude Code puts on synthetic (locally generated) assistant messages.
pub const SYNTHETIC_MODEL: &str = "<synthetic>";

/// Markers that identify the refusal. The second covers the "API Error: 400 …" variant
/// Claude Code stores inside tool_result blocks.
const REFUSED_MARKERS: &[&str] = &[
    REFUSED_MARKER,
    "Anthropic refused this request under its usage policy. Do not retry this request.",
];

/// Sanitize a parsed `/v1/messages` request body in place. Returns true if anything changed.
pub fn sanitize_messages(root: &mut Value) -> bool {
    let Some(obj) = root.as_object_mut() else {
        return false;
    };
    let Some(Value::Array(messages)) = obj.get_mut("messages") else {
        return false;
    };
    let before = messages.len();
    messages.retain(|m| !is_synthetic(m));
    let mut changed = messages.len() != before;
    for m in messages.iter_mut() {
        if scrub_marker(m) {
            changed = true;
        }
    }
    changed
}

/// Is this message one of Claude Code's `<synthetic>` error placeholders?
fn is_synthetic(m: &Value) -> bool {
    m.get("model").and_then(|v| v.as_str()) == Some(SYNTHETIC_MODEL)
}

/// Replace the refused marker in every string under `value`. Returns true if changed.
fn scrub_marker(value: &mut Value) -> bool {
    match value {
        Value::String(s) => {
            let mut hit = false;
            for marker in REFUSED_MARKERS {
                if s.contains(marker) {
                    *s = s.replace(marker, "[error]");
                    hit = true;
                }
            }
            hit
        }
        Value::Array(a) => a.iter_mut().any(scrub_marker),
        Value::Object(o) => o.values_mut().any(scrub_marker),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn drops_synthetic_and_blanks_marker() {
        let mut body = json!({
            "model": "claude-x",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "model": "<synthetic>", "content": [
                    {"type": "text", "text": "API Error: Anthropic refused this request under its usage policy. Do not retry this request."}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "API Error: 400 Anthropic refused this request under its usage policy. Do not retry this request."}
                ]}
            ]
        });
        assert!(sanitize_messages(&mut body));
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        let tr = msgs[1]["content"][0]["content"].as_str().unwrap();
        assert_eq!(tr, "API Error: 400 [error]");
    }

    #[test]
    fn untouched_body_reports_unchanged() {
        let mut body = json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]});
        assert!(!sanitize_messages(&mut body));
    }

    #[test]
    fn body_without_messages_is_a_noop() {
        let mut body = json!({"model": "m"});
        assert!(!sanitize_messages(&mut body));
    }
}
