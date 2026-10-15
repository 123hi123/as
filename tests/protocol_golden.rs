//! Golden protocol tests: SSE streams shaped like the Anthropic Messages API must aggregate
//! into the same Message JSON that `stream: false` returns (mirrors the official SDK
//! accumulator embedded in Claude Code 2.1.300), and the parser primitives must allow a
//! byte-exact HOLD -> passthrough switch.

use as_shim::aggregate::{is_content_event, Aggregator, Step};
use as_shim::sse::SseParser;
use serde_json::{json, Value};

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x % n.max(1)
    }
}

/// Feed `input` through the parser in chunks of `1..=max_chunk` bytes and aggregate.
fn aggregate_chunked(input: &[u8], max_chunk: u64, seed: u64) -> Result<Value, String> {
    let mut r = Rng(seed | 1);
    let mut p = SseParser::new();
    let mut a = Aggregator::new();
    let mut i = 0;
    let feed = |p: &mut SseParser, a: &mut Aggregator| -> Result<Option<()>, String> {
        while let Some(f) = p.next_frame() {
            let j = f.json();
            let Some(kind) = f.kind(j.as_ref()) else { continue };
            if kind == "ping" || kind == "error" {
                continue;
            }
            if a.apply(&kind, j.as_ref().unwrap_or(&Value::Null))? == Step::Done {
                return Ok(Some(()));
            }
        }
        Ok(None)
    };
    while i < input.len() {
        let end = (i + 1 + r.below(max_chunk) as usize).min(input.len());
        p.push(&input[i..end]);
        i = end;
        if feed(&mut p, &mut a)?.is_some() {
            return Ok(a.finish());
        }
    }
    p.set_eof();
    if feed(&mut p, &mut a)?.is_some() {
        return Ok(a.finish());
    }
    Err("stream ended before message_stop".into())
}

fn sse(events: &[Value], with_event_line: bool, eol: &str) -> Vec<u8> {
    let mut out = String::new();
    for e in events {
        if with_event_line {
            out.push_str(&format!("event: {}{eol}", e["type"].as_str().unwrap()));
        }
        out.push_str(&format!("data: {}{eol}{eol}", serde_json::to_string(e).unwrap()));
    }
    out.into_bytes()
}

/// Run every framing variant and chunking; all must produce `expected` (order-insensitive
/// object equality, i.e. semantic JSON equality).
fn assert_golden(events: &[Value], expected: &Value) {
    for (with_event, eol) in [(true, "\n"), (true, "\r\n"), (false, "\n"), (true, "\r")] {
        let bytes = sse(events, with_event, eol);
        for (max_chunk, seed) in [(u64::MAX, 1), (1, 2), (7, 3), (64, 4)] {
            let got = aggregate_chunked(&bytes, max_chunk.min(bytes.len() as u64), seed)
                .unwrap_or_else(|e| panic!("aggregate failed ({with_event},{eol:?},{max_chunk}): {e}"));
            assert_eq!(
                &got, expected,
                "framing event_line={with_event} eol={eol:?} chunk<={max_chunk}\n got: {got}\nwant: {expected}"
            );
        }
    }
}

// ---------------------------------------------------------------- docs-shaped goldens

#[test]
fn golden_docs_basic_text() {
    let events = [
        json!({"type": "message_start", "message": {"id": "msg_1nZdL29xx5MUA1yADyHTEsnR8uuvGzszyY", "type": "message", "role": "assistant", "content": [], "model": "claude-opus-4-6", "stop_reason": null, "stop_sequence": null, "usage": {"input_tokens": 25, "output_tokens": 1}}}),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "ping"}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "!"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": null}, "usage": {"output_tokens": 15}}),
        json!({"type": "message_stop"}),
    ];
    let want = json!({"id": "msg_1nZdL29xx5MUA1yADyHTEsnR8uuvGzszyY", "type": "message", "role": "assistant",
        "content": [{"type": "text", "text": "Hello!"}], "model": "claude-opus-4-6",
        "stop_reason": "end_turn", "stop_sequence": null, "usage": {"input_tokens": 25, "output_tokens": 15}});
    assert_golden(&events, &want);
}

#[test]
fn golden_docs_tool_use_with_text() {
    let mut events = vec![
        json!({"type":"message_start","message":{"id":"msg_014p7gG3wDgGV9EUtLvnow3U","type":"message","role":"assistant","model":"claude-opus-4-6","stop_sequence":null,"usage":{"input_tokens":472,"output_tokens":2},"content":[],"stop_reason":null}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type": "ping"}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Okay"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":", let's check the weather."}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01T1x1fJ34qAmk2tNTrN7Up6","name":"get_weather","input":{}}}),
    ];
    for part in ["", "{\"location\":", " \"San", " Francisc", "o,", " CA\"", ", ", "\"unit\": \"fah", "renheit\"}"] {
        events.push(json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":part}}));
    }
    events.extend([
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":89}}),
        json!({"type":"message_stop"}),
    ]);
    let want = json!({"id":"msg_014p7gG3wDgGV9EUtLvnow3U","type":"message","role":"assistant","model":"claude-opus-4-6",
        "stop_sequence":null,"usage":{"input_tokens":472,"output_tokens":89},
        "content":[{"type":"text","text":"Okay, let's check the weather."},
                   {"type":"tool_use","id":"toolu_01T1x1fJ34qAmk2tNTrN7Up6","name":"get_weather","input":{"location":"San Francisco, CA","unit":"fahrenheit"}}],
        "stop_reason":"tool_use"});
    assert_golden(&events, &want);
}

#[test]
fn golden_docs_extended_thinking() {
    let events = [
        json!({"type": "message_start", "message": {"id": "msg_01", "type": "message", "role": "assistant", "content": [], "model": "claude-opus-4-6", "stop_reason": null, "stop_sequence": null}}),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "1071 = 2 × 462 + 147\n", "estimated_tokens": 12}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "462 = 3 × 147 + 21"}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "signature_delta", "signature": "EqQBCgIYAhIM1gbcDa9GJwZA2b3hGgxBdjrkzLoky3dl1pkiMOYds"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "content_block_start", "index": 1, "content_block": {"type": "redacted_thinking", "data": "EmwKAhgBEgy3va3pzix/LafPsn4aDFIT"}}),
        json!({"type": "content_block_stop", "index": 1}),
        json!({"type": "content_block_start", "index": 2, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 2, "delta": {"type": "text_delta", "text": "The GCD is 21."}}),
        json!({"type": "content_block_stop", "index": 2}),
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": null}, "usage": {"input_tokens": 50, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 300}}),
        json!({"type": "message_stop"}),
    ];
    let want = json!({"id": "msg_01", "type": "message", "role": "assistant", "model": "claude-opus-4-6",
        "content": [
            {"type": "thinking", "thinking": "1071 = 2 × 462 + 147\n462 = 3 × 147 + 21", "signature": "EqQBCgIYAhIM1gbcDa9GJwZA2b3hGgxBdjrkzLoky3dl1pkiMOYds"},
            {"type": "redacted_thinking", "data": "EmwKAhgBEgy3va3pzix/LafPsn4aDFIT"},
            {"type": "text", "text": "The GCD is 21."}],
        "stop_reason": "end_turn", "stop_sequence": null,
        "usage": {"input_tokens": 50, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 300}});
    assert_golden(&events, &want);
}

#[test]
fn golden_web_search_web_fetch_citations_server_usage() {
    let events = [
        json!({"type":"message_start","message":{"id":"msg_ws","type":"message","role":"assistant","model":"claude-opus-4-6","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":2679,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0},"output_tokens":3,"service_tier":"standard"}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I'll search."}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"server_tool_use","id":"srvtoolu_014hJH82Qum7Td6UV8gDXThB","name":"web_search","input":{}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"latest quantum"}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":" computing breakthroughs 2025\"}"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"content_block_start","index":2,"content_block":{"type":"web_search_tool_result","tool_use_id":"srvtoolu_014hJH82Qum7Td6UV8gDXThB","content":[{"type":"web_search_result","title":"Quantum Computing Breakthroughs in 2025","url":"https://example.com","encrypted_content":"EqgfCioIARgB","page_age":null}]}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"content_block_start","index":3,"content_block":{"type":"server_tool_use","id":"srvtoolu_fetch","name":"web_fetch","input":{}}}),
        json!({"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"{\"url\": \"https://example.com/a\"}"}}),
        json!({"type":"content_block_stop","index":3}),
        json!({"type":"content_block_start","index":4,"content_block":{"type":"web_fetch_tool_result","tool_use_id":"srvtoolu_fetch","content":{"type":"web_fetch_result","url":"https://example.com/a","content":{"type":"document","source":{"type":"text","media_type":"text/plain","data":"body"},"title":"A","citations":{"enabled":true}},"retrieved_at":"2025-08-25T10:30:00Z"}}}),
        json!({"type":"content_block_stop","index":4}),
        json!({"type":"content_block_start","index":5,"content_block":{"type":"text","text":"","citations":[]}}),
        json!({"type":"content_block_delta","index":5,"delta":{"type":"citations_delta","citation":{"type":"web_search_result_location","url":"https://example.com","title":"Quantum Computing Breakthroughs in 2025","encrypted_index":"Eo8BCioIAhgB","cited_text":"IBM unveiled ..."}}}),
        json!({"type":"content_block_delta","index":5,"delta":{"type":"text_delta","text":"IBM unveiled a processor."}}),
        json!({"type":"content_block_stop","index":5}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":10682,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":510,"server_tool_use":{"web_search_requests":1,"web_fetch_requests":1}}}),
        json!({"type":"message_stop"}),
    ];
    let want = json!({"id":"msg_ws","type":"message","role":"assistant","model":"claude-opus-4-6",
        "content":[
            {"type":"text","text":"I'll search."},
            {"type":"server_tool_use","id":"srvtoolu_014hJH82Qum7Td6UV8gDXThB","name":"web_search","input":{"query":"latest quantum computing breakthroughs 2025"}},
            {"type":"web_search_tool_result","tool_use_id":"srvtoolu_014hJH82Qum7Td6UV8gDXThB","content":[{"type":"web_search_result","title":"Quantum Computing Breakthroughs in 2025","url":"https://example.com","encrypted_content":"EqgfCioIARgB","page_age":null}]},
            {"type":"server_tool_use","id":"srvtoolu_fetch","name":"web_fetch","input":{"url":"https://example.com/a"}},
            {"type":"web_fetch_tool_result","tool_use_id":"srvtoolu_fetch","content":{"type":"web_fetch_result","url":"https://example.com/a","content":{"type":"document","source":{"type":"text","media_type":"text/plain","data":"body"},"title":"A","citations":{"enabled":true}},"retrieved_at":"2025-08-25T10:30:00Z"}},
            {"type":"text","text":"IBM unveiled a processor.","citations":[{"type":"web_search_result_location","url":"https://example.com","title":"Quantum Computing Breakthroughs in 2025","encrypted_index":"Eo8BCioIAhgB","cited_text":"IBM unveiled ..."}]}],
        "stop_reason":"end_turn","stop_sequence":null,
        "usage":{"input_tokens":10682,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0},"output_tokens":510,"service_tier":"standard","server_tool_use":{"web_search_requests":1,"web_fetch_requests":1}}});
    assert_golden(&events, &want);
}

#[test]
fn golden_code_execution_container_and_mcp() {
    let events = [
        json!({"type":"message_start","message":{"id":"msg_ce","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"stop_sequence":null,"container":null,"usage":{"input_tokens":5,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srvtoolu_b","name":"bash_code_execution","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"ls -la\"}"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"bash_code_execution_tool_result","tool_use_id":"srvtoolu_b","content":{"type":"bash_code_execution_result","stdout":"total 0\n","stderr":"","return_code":0,"content":[]}}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"content_block_start","index":2,"content_block":{"type":"mcp_tool_use","id":"mcptoolu_1","name":"echo","server_name":"srv","input":{}}}),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"x\":[1,2,{\"y\":null}]}"}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"content_block_start","index":3,"content_block":{"type":"mcp_tool_result","tool_use_id":"mcptoolu_1","is_error":false,"content":[{"type":"text","text":"ok"}]}}),
        json!({"type":"content_block_stop","index":3}),
        json!({"type":"message_delta","delta":{"stop_reason":"pause_turn","stop_sequence":null,"container":{"id":"container_011CPR5CNjB747bTd36fQLFk","expires_at":"2025-05-23T21:13:31.749448Z"}},"usage":{"output_tokens":77}}),
        json!({"type":"message_stop"}),
    ];
    let want = json!({"id":"msg_ce","type":"message","role":"assistant","model":"m",
        "content":[
            {"type":"server_tool_use","id":"srvtoolu_b","name":"bash_code_execution","input":{"command":"ls -la"}},
            {"type":"bash_code_execution_tool_result","tool_use_id":"srvtoolu_b","content":{"type":"bash_code_execution_result","stdout":"total 0\n","stderr":"","return_code":0,"content":[]}},
            {"type":"mcp_tool_use","id":"mcptoolu_1","name":"echo","server_name":"srv","input":{"x":[1,2,{"y":null}]}},
            {"type":"mcp_tool_result","tool_use_id":"mcptoolu_1","is_error":false,"content":[{"type":"text","text":"ok"}]}],
        "stop_reason":"pause_turn","stop_sequence":null,
        "container":{"id":"container_011CPR5CNjB747bTd36fQLFk","expires_at":"2025-05-23T21:13:31.749448Z"},
        "usage":{"input_tokens":5,"output_tokens":77}});
    assert_golden(&events, &want);
}

#[test]
fn golden_refusal_stop_details_and_usage_extras() {
    let events = [
        json!({"type":"message_start","message":{"id":"msg_r","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"stop_sequence":null,"stop_details":null,"usage":{"input_tokens":9,"cache_creation_input_tokens":4,"cache_read_input_tokens":3,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I can't"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"refusal","stop_sequence":null,"stop_details":{"type":"refusal","category":"cyber","explanation":"x"}},
               "usage":{"input_tokens":null,"cache_creation_input_tokens":null,"cache_read_input_tokens":null,"output_tokens":6,
                        "iterations":[{"input_tokens":9,"output_tokens":6}],"output_tokens_details":{"reasoning_tokens":0}}}),
        json!({"type":"message_stop"}),
    ];
    let want = json!({"id":"msg_r","type":"message","role":"assistant","model":"m",
        "content":[{"type":"text","text":"I can't"}],
        "stop_reason":"refusal","stop_sequence":null,"stop_details":{"type":"refusal","category":"cyber","explanation":"x"},
        "usage":{"input_tokens":9,"cache_creation_input_tokens":4,"cache_read_input_tokens":3,"output_tokens":6,
                 "iterations":[{"input_tokens":9,"output_tokens":6}],"output_tokens_details":{"reasoning_tokens":0}}});
    assert_golden(&events, &want);
}

/// Context editing: the API puts `context_management` at the TOP LEVEL of the final
/// `message_delta` event (not inside `delta`); the SDK copies it onto the message
/// (`if(n.context_management!=null)s.context_management=n.context_management`), as it does
/// for `input_transformations`. The non-stream Message carries it as a top-level field.
#[test]
fn golden_context_management_top_level_of_message_delta() {
    let edits = json!({"applied_edits":[
        {"type":"clear_thinking_20251015","cleared_thinking_turns":3,"cleared_input_tokens":15000},
        {"type":"clear_tool_uses_20250919","cleared_tool_uses":8,"cleared_input_tokens":50000}]});
    let events = [
        json!({"type":"message_start","message":{"id":"msg_cm","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"x"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2},
               "context_management": edits.clone(), "input_transformations":[{"type":"t"}]}),
        json!({"type":"message_stop"}),
    ];
    let want = json!({"id":"msg_cm","type":"message","role":"assistant","model":"m",
        "content":[{"type":"text","text":"x"}],"stop_reason":"end_turn","stop_sequence":null,
        "usage":{"input_tokens":1,"output_tokens":2},
        "context_management": edits, "input_transformations":[{"type":"t"}]});
    assert_golden(&events, &want);
}

/// SDK: `compaction_delta` REPLACES `content` (and `encrypted_content` when present);
/// it is not an append.
#[test]
fn golden_compaction_delta_replaces_content() {
    let events = [
        json!({"type":"message_start","message":{"id":"msg_c","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"compaction","content":"","encrypted_content":"old"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"compaction_delta","content":"Summary v1","encrypted_content":"enc1"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"compaction_delta","content":"Summary final"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}),
        json!({"type":"message_stop"}),
    ];
    let want = json!({"id":"msg_c","type":"message","role":"assistant","model":"m",
        "content":[{"type":"compaction","content":"Summary final","encrypted_content":"enc1"}],
        "stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":5}});
    assert_golden(&events, &want);
}

/// SDK: a `fallback` content block switches the message model to `content_block.to.model`.
#[test]
fn golden_fallback_block_sets_model() {
    let events = [
        json!({"type":"message_start","message":{"id":"msg_f","type":"message","role":"assistant","model":"claude-a","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"fallback","from":{"model":"claude-a"},"to":{"model":"claude-b"},"trigger":{"type":"refusal","category":"cyber"}}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"hi"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2}}),
        json!({"type":"message_stop"}),
    ];
    let got = aggregate_chunked(&sse(&events, true, "\n"), u64::MAX, 1).unwrap();
    assert_eq!(got["model"], "claude-b", "{got}");
    assert_eq!(got["content"][0]["type"], "fallback");
    assert_eq!(got["content"][1]["text"], "hi");
}

/// max_tokens hit in the middle of a tool input: the concatenated partial_json is not valid
/// JSON. The SDK parses it with its partial-JSON repair (`{"file_path":"/a","content":"lo`
/// -> `{"file_path":"/a"}`) and the response is still a normal `stop_reason: max_tokens`
/// message; the aggregator must not turn this into an error.
#[test]
fn golden_truncated_tool_input_at_max_tokens() {
    let events = [
        json!({"type":"message_start","message":{"id":"msg_t","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"Write","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/a\",\"content\":\"lo"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"max_tokens","stop_sequence":null},"usage":{"output_tokens":32000}}),
        json!({"type":"message_stop"}),
    ];
    let got = aggregate_chunked(&sse(&events, true, "\n"), u64::MAX, 1).expect("must not fail");
    assert_eq!(got["stop_reason"], "max_tokens");
    assert_eq!(got["content"][0]["input"], json!({"file_path":"/a"}));
}

// ---------------------------------------------------------------- framing robustness

#[test]
fn comments_blank_lines_and_split_multiline_data_aggregate() {
    let input = b": hello\n\n\nevent: message_start\ndata: {\"type\":\"message_start\",\ndata: \"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"x\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\r\n\r\n: keepalive\r\n\r\nevent: content_block_start\r\ndata:{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\r\rdata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"\xe4\xb8\xad\xe6\x96\x87\"}}\n\nevent: message_stop\ndata: {}\n\n";
    for seed in 1..200 {
        let got = aggregate_chunked(input, 3, seed).unwrap();
        assert_eq!(
            got,
            json!({"id":"m","type":"message","role":"assistant","model":"x","content":[{"type":"text","text":"中文"}],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}})
        );
    }
}

// ---------------------------------------------------------------- PATH S hold/flush contract

/// Reference HOLD relay built only from the parser primitives the proxy uses: forward
/// ping/empty frames at once, hold everything else, on the first content frame emit
/// held frames + that frame + `take_remaining()`, then pass raw chunks through.
fn hold_relay(chunks: &[&[u8]]) -> (Vec<u8>, bool) {
    let mut p = SseParser::new();
    let mut out = Vec::new();
    let mut held: Vec<u8> = Vec::new();
    let mut passthrough = false;
    for c in chunks {
        if passthrough {
            out.extend_from_slice(c);
            continue;
        }
        p.push(c);
        while let Some(f) = p.next_frame() {
            let j = f.json();
            let kind = f.kind(j.as_ref()).unwrap_or_default();
            if f.is_empty() || kind == "ping" {
                out.extend_from_slice(&f.raw);
            } else if is_content_event(&kind) {
                out.extend_from_slice(&held);
                out.extend_from_slice(&f.raw);
                out.extend_from_slice(&p.take_remaining());
                passthrough = true;
                break;
            } else {
                held.extend_from_slice(&f.raw);
            }
        }
    }
    (out, passthrough)
}

#[test]
fn hold_flush_is_byte_exact_after_content_under_any_chunking() {
    let head: &[u8] = b"event: message_start\r\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"content\":[]}}\r\n\r\n";
    let pings: &[u8] = b"event: ping\ndata: {\"type\": \"ping\"}\n\n: c\n\n";
    let tail: &[u8] = b"event: content_block_start\rdata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\r\r\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"\xe4\xb8\xad\"}}\n\n\
event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"x\"}}\n\n\
garbage-without-terminator \xff\r";
    let mut input = Vec::new();
    input.extend_from_slice(head);
    input.extend_from_slice(pings);
    input.extend_from_slice(tail);
    let mut want = Vec::new();
    want.extend_from_slice(pings);
    want.extend_from_slice(head);
    want.extend_from_slice(tail);
    let mut r = Rng(7);
    for iter in 0..5000 {
        let max = if iter % 10 == 0 { input.len() as u64 } else { 9 };
        let mut chunks = vec![];
        let mut i = 0;
        while i < input.len() {
            let end = (i + 1 + r.below(max) as usize).min(input.len());
            chunks.push(&input[i..end]);
            i = end;
        }
        let (out, pass) = hold_relay(&chunks);
        assert!(pass);
        assert_eq!(out, want, "chunking {:?}", chunks.iter().map(|c| c.len()).collect::<Vec<_>>());
    }
}
