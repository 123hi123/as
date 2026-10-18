//! Rebuild a non-stream Messages API response from stream events
//! (mirrors the official Anthropic SDK message accumulator).

use crate::{log_debug, log_warn};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Default)]
struct Block {
    value: Value,
    json_buf: String,
    stopped: bool,
}

enum Tok {
    Brace(char),
    Paren(char),
    Sep,
    Delim,
    Str(String),
    Num(String),
    Name(String),
}

fn tokenize(s: &str) -> Vec<Tok> {
    let c: Vec<char> = s.chars().collect();
    let n = c.len();
    let mut out = Vec::new();
    let mut r = 0;
    while r < n {
        let ch = c[r];
        match ch {
            '\\' => r += 1,
            '{' | '}' => {
                out.push(Tok::Brace(ch));
                r += 1;
            }
            '[' | ']' => {
                out.push(Tok::Paren(ch));
                r += 1;
            }
            ':' => {
                out.push(Tok::Sep);
                r += 1;
            }
            ',' => {
                out.push(Tok::Delim);
                r += 1;
            }
            '"' => {
                let mut v = String::new();
                let mut unterminated = false;
                r += 1;
                loop {
                    if r >= n {
                        unterminated = true;
                        break;
                    }
                    if c[r] == '"' {
                        break;
                    }
                    if c[r] == '\\' {
                        r += 1;
                        if r >= n {
                            unterminated = true;
                            break;
                        }
                        v.push('\\');
                        v.push(c[r]);
                    } else {
                        v.push(c[r]);
                    }
                    r += 1;
                }
                r += 1;
                if !unterminated {
                    out.push(Tok::Str(v));
                }
            }
            c0 if c0.is_whitespace() => r += 1,
            c0 if c0.is_ascii_digit() || c0 == '-' || c0 == '.' => {
                let mut v = String::new();
                if c0 == '-' {
                    v.push('-');
                    r += 1;
                }
                while r < n {
                    let d = c[r];
                    let last = v.chars().last();
                    if d.is_ascii_digit()
                        || d == '.'
                        || d == 'e'
                        || d == 'E'
                        || ((d == '-' || d == '+') && matches!(last, Some('e' | 'E')))
                    {
                        v.push(d);
                        r += 1;
                    } else {
                        break;
                    }
                }
                out.push(Tok::Num(v));
            }
            c0 if c0.is_ascii_alphabetic() => {
                let mut v = String::new();
                while r < n && c[r].is_ascii_alphabetic() {
                    v.push(c[r]);
                    r += 1;
                }
                if matches!(v.as_str(), "true" | "false" | "null") {
                    out.push(Tok::Name(v));
                } else {
                    r += 1;
                }
            }
            _ => r += 1,
        }
    }
    out
}

fn strip_incomplete(t: &mut Vec<Tok>) {
    loop {
        let drop_last = match t.last() {
            None => return,
            Some(Tok::Sep | Tok::Delim) => true,
            Some(Tok::Num(v)) if v.ends_with(['.', '-', '+', 'e', 'E']) => true,
            Some(Tok::Num(_) | Tok::Str(_)) => {
                matches!(
                    t.len().checked_sub(2).map(|i| &t[i]),
                    Some(Tok::Delim | Tok::Brace('{'))
                )
            }
            Some(_) => false,
        };
        if !drop_last {
            return;
        }
        t.pop();
    }
}

/// Partial-JSON repair used by the Anthropic SDK for tool input cut off mid-stream
/// (e.g. `max_tokens`): drop the unfinished tail and close open brackets.
pub fn parse_partial_json(s: &str) -> Option<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(s) {
        return Some(v);
    }
    let mut t = tokenize(s);
    strip_incomplete(&mut t);
    let mut closers: Vec<char> = Vec::new();
    for tok in &t {
        match tok {
            Tok::Brace('{') => closers.push('}'),
            Tok::Paren('[') => closers.push(']'),
            Tok::Brace(_) | Tok::Paren(_) => {
                let want = if matches!(tok, Tok::Brace(_)) {
                    '}'
                } else {
                    ']'
                };
                if let Some(i) = closers.iter().rposition(|&c| c == want) {
                    closers.remove(i);
                }
            }
            _ => {}
        }
    }
    let mut out = String::with_capacity(s.len() + closers.len());
    for tok in &t {
        match tok {
            Tok::Brace(c) | Tok::Paren(c) => out.push(*c),
            Tok::Sep => out.push(':'),
            Tok::Delim => out.push(','),
            Tok::Str(v) => {
                out.push('"');
                out.push_str(v);
                out.push('"');
            }
            Tok::Num(v) | Tok::Name(v) => out.push_str(v),
        }
    }
    out.extend(closers.iter().rev());
    crate::sse::parse_json_lenient(&out)
}

fn finalize_input(idx: u64, block: &mut Block) {
    if block.json_buf.is_empty() {
        return;
    }
    let buf = std::mem::take(&mut block.json_buf);
    match parse_partial_json(&buf) {
        Some(input) => {
            if let Some(obj) = block.value.as_object_mut() {
                obj.insert("input".into(), input);
            }
        }
        None => log_warn!(
            "tool input JSON for block {idx} could not be repaired ({} bytes); keeping the start input",
            buf.len()
        ),
    }
}

#[derive(Default)]
pub struct Aggregator {
    message: Option<Map<String, Value>>,
    blocks: BTreeMap<u64, Block>,
    content_started: bool,
    done: bool,
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Continue,
    /// `message_stop` arrived; call [`Aggregator::finish`].
    Done,
}

/// Event types that mean real output has started reaching the client.
pub fn is_content_event(kind: &str) -> bool {
    matches!(
        kind,
        "content_block_start"
            | "content_block_delta"
            | "content_block_stop"
            | "message_delta"
            | "message_stop"
    )
}

fn append_str(target: &mut Map<String, Value>, key: &str, add: &str) {
    match target.get_mut(key) {
        Some(Value::String(s)) => s.push_str(add),
        _ => {
            target.insert(key.to_string(), Value::String(add.to_string()));
        }
    }
}

impl Aggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn content_started(&self) -> bool {
        self.content_started
    }

    pub fn has_message_start(&self) -> bool {
        self.message.is_some()
    }

    /// The stream ended without `message_stop`, but `message_delta` already carried a
    /// `stop_reason` and every block was stopped: the message is complete in substance.
    pub fn is_complete_without_stop(&self) -> bool {
        self.message
            .as_ref()
            .and_then(|m| m.get("stop_reason"))
            .is_some_and(|r| !r.is_null())
            && self.blocks.values().all(|b| b.stopped)
    }

    /// Apply one decoded event (`kind` = data JSON `type`, or the SSE event name).
    /// `error` and `ping` must be handled by the caller; they are ignored here.
    pub fn apply(&mut self, kind: &str, ev: &Value) -> Result<Step, String> {
        if is_content_event(kind) {
            self.content_started = true;
        }
        match kind {
            "message_start" => {
                let msg = ev
                    .get("message")
                    .and_then(|m| m.as_object())
                    .cloned()
                    .ok_or("message_start without a message object")?;
                self.blocks.clear();
                if let Some(Value::Array(existing)) = msg.get("content") {
                    for (i, b) in existing.iter().enumerate() {
                        self.blocks.insert(
                            i as u64,
                            Block {
                                value: b.clone(),
                                json_buf: String::new(),
                                stopped: true,
                            },
                        );
                    }
                }
                self.message = Some(msg);
            }
            "content_block_start" => {
                let idx = ev
                    .get("index")
                    .and_then(|i| i.as_u64())
                    .unwrap_or(self.blocks.len() as u64);
                let block = ev
                    .get("content_block")
                    .cloned()
                    .ok_or("content_block_start without content_block")?;
                if block.get("type").and_then(|t| t.as_str()) == Some("fallback") {
                    if let (Some(m), Some(model)) = (
                        self.message.as_mut(),
                        block.pointer("/to/model").filter(|v| v.is_string()),
                    ) {
                        m.insert("model".into(), model.clone());
                    }
                }
                self.blocks.insert(
                    idx,
                    Block {
                        value: block,
                        json_buf: String::new(),
                        stopped: false,
                    },
                );
            }
            "content_block_delta" => {
                let idx = ev.get("index").and_then(|i| i.as_u64()).unwrap_or(0);
                let Some(delta) = ev.get("delta").and_then(|d| d.as_object()) else {
                    return Ok(Step::Continue);
                };
                let Some(block) = self.blocks.get_mut(&idx) else {
                    log_debug!("content_block_delta for unknown block index {idx}, ignored");
                    return Ok(Step::Continue);
                };
                let Some(obj) = block.value.as_object_mut() else {
                    return Ok(Step::Continue);
                };
                let dtype = delta.get("type").and_then(|t| t.as_str()).unwrap_or("");
                let s = |k: &str| delta.get(k).and_then(|v| v.as_str()).unwrap_or("");
                match dtype {
                    "text_delta" => append_str(obj, "text", s("text")),
                    "thinking_delta" => append_str(obj, "thinking", s("thinking")),
                    "signature_delta" => {
                        obj.insert(
                            "signature".into(),
                            Value::String(s("signature").to_string()),
                        );
                    }
                    "input_json_delta" => block.json_buf.push_str(s("partial_json")),
                    "compaction_delta" => {
                        for k in ["content", "encrypted_content"] {
                            if let Some(v) = delta.get(k) {
                                obj.insert(k.into(), v.clone());
                            }
                        }
                    }
                    "citations_delta" => {
                        let c = delta.get("citation").cloned().unwrap_or(Value::Null);
                        match obj.get_mut("citations") {
                            Some(Value::Array(a)) => a.push(c),
                            _ => {
                                obj.insert("citations".into(), Value::Array(vec![c]));
                            }
                        }
                    }
                    _ => {
                        // Unknown delta: generic merge, strings appended, everything else set.
                        for (k, v) in delta {
                            if k == "type" {
                                continue;
                            }
                            match (obj.get_mut(k), v) {
                                (Some(Value::String(cur)), Value::String(add)) => cur.push_str(add),
                                _ => {
                                    obj.insert(k.clone(), v.clone());
                                }
                            }
                        }
                    }
                }
            }
            "content_block_stop" => {
                let idx = ev.get("index").and_then(|i| i.as_u64()).unwrap_or(0);
                if let Some(block) = self.blocks.get_mut(&idx) {
                    finalize_input(idx, block);
                    block.stopped = true;
                }
            }
            "message_delta" => {
                let msg = self
                    .message
                    .as_mut()
                    .ok_or("message_delta before message_start")?;
                if let Some(delta) = ev.get("delta").and_then(|d| d.as_object()) {
                    for (k, v) in delta {
                        msg.insert(k.clone(), v.clone());
                    }
                }
                for k in ["context_management", "input_transformations"] {
                    if let Some(v) = ev.get(k).filter(|v| !v.is_null()) {
                        msg.insert(k.into(), v.clone());
                    }
                }
                if let Some(usage) = ev.get("usage").and_then(|u| u.as_object()) {
                    if !matches!(msg.get("usage"), Some(Value::Object(_))) {
                        msg.insert("usage".into(), Value::Object(Map::new()));
                    }
                    if let Some(Value::Object(mu)) = msg.get_mut("usage") {
                        for (k, v) in usage {
                            if !v.is_null() {
                                mu.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
            }
            "message_stop" => {
                if self.message.is_none() {
                    return Err("message_stop without message_start".into());
                }
                self.done = true;
                return Ok(Step::Done);
            }
            other => log_debug!("ignoring stream event type {other:?}"),
        }
        Ok(Step::Continue)
    }

    /// Final non-stream Message JSON.
    pub fn finish(mut self) -> Value {
        let mut msg = self.message.take().unwrap_or_default();
        let content: Vec<Value> = std::mem::take(&mut self.blocks)
            .into_iter()
            .map(|(idx, mut b)| {
                finalize_input(idx, &mut b);
                b.value
            })
            .collect();
        msg.insert("content".into(), Value::Array(content));
        Value::Object(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(events: &[Value]) -> Value {
        let mut a = Aggregator::new();
        for e in events {
            let kind = e["type"].as_str().unwrap();
            if a.apply(kind, e).unwrap() == Step::Done {
                return a.finish();
            }
        }
        panic!("no message_stop");
    }

    fn start() -> Value {
        json!({"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":2,"output_tokens":1}}})
    }
    fn stop() -> Value {
        json!({"type":"message_stop"})
    }

    #[test]
    fn text() {
        let m = run(&[
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"ping"}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hel"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"lo"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}),
            stop(),
        ]);
        assert_eq!(
            serde_json::to_string(&m).unwrap(),
            r#"{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[{"type":"text","text":"Hello"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":2,"output_tokens":5}}"#
        );
    }

    #[test]
    fn thinking_signature_redacted() {
        let m = run(&[
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me ","estimated_tokens":3}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"think."}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"c2ln"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"ENCRYPTED"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"ok"}}),
            json!({"type":"content_block_stop","index":2}),
            stop(),
        ]);
        assert_eq!(
            m["content"],
            json!([
                {"type":"thinking","thinking":"Let me think.","signature":"c2ln"},
                {"type":"redacted_thinking","data":"ENCRYPTED"},
                {"type":"text","text":"ok"}
            ])
        );
    }

    #[test]
    fn tool_use_json_and_empty_input() {
        let m = run(&[
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t1","name":"Read","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\": \"/a"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\", \"n\": 3}"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"t2","name":"Noop","input":{}}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"t3","name":"Noop2","input":{}}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":""}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":9}}),
            stop(),
        ]);
        assert_eq!(
            serde_json::to_string(&m["content"]).unwrap(),
            r#"[{"type":"tool_use","id":"t1","name":"Read","input":{"path":"/a","n":3}},{"type":"tool_use","id":"t2","name":"Noop","input":{}},{"type":"tool_use","id":"t3","name":"Noop2","input":{}}]"#
        );
        assert_eq!(m["stop_reason"], "tool_use");
    }

    #[test]
    fn server_tool_and_web_search_and_citations() {
        let m = run(&[
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srv1","name":"web_search","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"rust\"}"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"web_search_tool_result","tool_use_id":"srv1","content":[{"type":"web_search_result","url":"https://r","title":"R","encrypted_content":"e","page_age":null}]}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"text","text":"","citations":null}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"citations_delta","citation":{"type":"web_search_result_location","url":"https://r","title":"R","encrypted_index":"x","cited_text":"c"}}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"Rust is"}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"citations_delta","citation":{"type":"web_search_result_location","url":"https://r2"}}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":20,"server_tool_use":{"web_search_requests":1}}}),
            stop(),
        ]);
        assert_eq!(m["content"][0]["input"], json!({"query":"rust"}));
        assert_eq!(m["content"][1]["content"][0]["url"], "https://r");
        assert_eq!(m["content"][2]["text"], "Rust is");
        assert_eq!(m["content"][2]["citations"].as_array().unwrap().len(), 2);
        assert_eq!(m["content"][2]["citations"][1]["url"], "https://r2");
        assert_eq!(m["usage"]["server_tool_use"]["web_search_requests"], 1);
        assert_eq!(m["usage"]["input_tokens"], 10);
        assert_eq!(m["usage"]["output_tokens"], 20);
    }

    #[test]
    fn message_delta_usage_merge_and_extra_fields() {
        let m = run(&[
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"x"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"pause_turn","stop_sequence":null,"container":{"id":"c1","expires_at":"t"}},"usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":null}}),
            stop(),
        ]);
        assert_eq!(m["stop_reason"], "pause_turn");
        assert_eq!(m["container"]["id"], "c1");
        assert_eq!(m["usage"]["input_tokens"], 12);
        assert_eq!(m["usage"]["output_tokens"], 7);
        assert_eq!(
            m["usage"]["cache_read_input_tokens"], 2,
            "null must not overwrite"
        );
        // key order of the base message is kept; new keys appended
        let keys: Vec<_> = m.as_object().unwrap().keys().cloned().collect();
        assert_eq!(
            keys,
            [
                "id",
                "type",
                "role",
                "model",
                "content",
                "stop_reason",
                "stop_sequence",
                "usage",
                "container"
            ]
        );
    }

    #[test]
    fn unknown_delta_generic_merge_and_unknown_event() {
        let m = run(&[
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"future_block","note":"a","count":1}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"future_delta","note":"b","count":2,"extra":true}}),
            json!({"type":"some_future_event","x":1}),
            json!({"type":"content_block_stop","index":0}),
            stop(),
        ]);
        assert_eq!(
            m["content"][0],
            json!({"type":"future_block","note":"ab","count":2,"extra":true})
        );
    }

    #[test]
    fn partial_json_matches_sdk() {
        let cases = [
            (
                r#"{"file_path":"/a","content":"lo"#,
                json!({"file_path":"/a"}),
            ),
            (r#"{"a":1"#, json!({"a":1})),
            (r#"{"a":[1,2"#, json!({"a":[1]})),
            (r#"{"a":1.5e"#, json!({})),
            (r#"{"a":{"b":"c"},"d"#, json!({"a":{"b":"c"}})),
            (r#"{"a":"x\"y","b":tr"#, json!({"a":"x\"y"})),
            (r#"{"a":true,"#, json!({"a":true})),
            (r#"{"#, json!({})),
            (r#"{"a":"\ud83d"}"#, json!({"a":"\u{fffd}"})),
        ];
        for (input, want) in cases {
            assert_eq!(parse_partial_json(input), Some(want), "{input}");
        }
    }

    #[test]
    fn complete_without_stop() {
        let mut a = Aggregator::new();
        a.apply("message_start", &start()).unwrap();
        a.apply("content_block_start", &json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})).unwrap();
        a.apply(
            "message_delta",
            &json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        )
        .unwrap();
        assert!(!a.is_complete_without_stop(), "block 0 not stopped");
        a.apply(
            "content_block_stop",
            &json!({"type":"content_block_stop","index":0}),
        )
        .unwrap();
        assert!(a.is_complete_without_stop());
    }

    #[test]
    fn content_flag_and_errors() {
        let mut a = Aggregator::new();
        a.apply("message_start", &start()).unwrap();
        assert!(!a.content_started());
        a.apply("content_block_start", &json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})).unwrap();
        assert!(a.content_started());
        let mut b = Aggregator::new();
        assert!(b.apply("message_stop", &stop()).is_err());
    }
}
