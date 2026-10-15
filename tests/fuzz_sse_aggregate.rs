//! Deterministic fuzzing (no extra deps) of the SSE parser, aggregator and error helpers:
//! must never panic, must be lossless, and chunking must not change the parse.

use as_shim::aggregate::Aggregator;
use as_shim::errmap;
use as_shim::sse::SseParser;
use serde_json::{json, Map, Value};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }
}

const TOKENS: &[&[u8]] = &[
    b"\n", b"\r", b"\r\n", b"\n\n", b"data:", b"data: ", b"event:", b"event: ", b":", b" ", b"x",
    b"{\"type\":\"error\"}", b"\xff\xfe", b"\xe4\xb8\xad", b"id: 1", b"retry: 5",
];

fn gen_sse(r: &mut Rng) -> Vec<u8> {
    let mut v = vec![];
    for _ in 0..r.below(60) {
        if r.below(8) == 0 {
            v.push(r.below(256) as u8);
        } else {
            v.extend_from_slice(r.pick(TOKENS));
        }
    }
    v
}

type Parsed = (Vec<u8>, Option<String>, Option<String>);

fn parse(input: &[u8], r: &mut Rng, chunked: bool) -> (Vec<Parsed>, Vec<u8>) {
    let mut p = SseParser::new();
    let mut out = vec![];
    let mut i = 0;
    while i < input.len() {
        let step = if chunked { 1 + r.below(5) as usize } else { input.len() };
        let end = (i + step).min(input.len());
        p.push(&input[i..end]);
        i = end;
        while let Some(f) = p.next_frame() {
            let _ = f.kind(f.json().as_ref());
            out.push((f.raw.to_vec(), f.event, f.data));
        }
    }
    p.set_eof();
    while let Some(f) = p.next_frame() {
        out.push((f.raw.to_vec(), f.event, f.data));
    }
    (out, p.take_remaining().to_vec())
}

#[test]
fn sse_random_lossless_and_chunk_invariant() {
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    for iter in 0..30_000 {
        let input = gen_sse(&mut r);
        let (whole, rest_whole) = parse(&input, &mut r, false);
        let (chunked, rest_chunked) = parse(&input, &mut r, true);
        let mut joined: Vec<u8> = whole.iter().flat_map(|f| f.0.clone()).collect();
        joined.extend_from_slice(&rest_whole);
        assert_eq!(joined, input, "iter {iter}: bytes lost or duplicated");
        assert_eq!(whole, chunked, "iter {iter}: chunking changed the parse of {:?}", String::from_utf8_lossy(&input));
        assert_eq!(rest_whole, rest_chunked);

        let mut q = SseParser::new();
        q.push(&input);
        while q.next_frame().is_some() {}
        let _ = q.finish();
        assert!(q.finish().is_none());
    }
}

#[test]
fn sse_every_truncation_point() {
    let full: &[u8] = b"event: message_start\r\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"content\":[]}}\r\n\r\n: c\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\ndata: \"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\ndata: {\"type\":\"error\",\"error\":{\"type\":\"upstream_error\"}}\r\r";
    for cut in 0..=full.len() {
        let mut p = SseParser::new();
        p.push(&full[..cut]);
        while p.next_frame().is_some() {}
        let _ = p.finish();
        let _ = p.finish();
        let _ = p.take_remaining();
    }
}

fn rand_val(r: &mut Rng, depth: u32) -> Value {
    match r.below(if depth > 2 { 6 } else { 9 }) {
        0 => Value::Null,
        1 => json!(r.below(2) == 0),
        2 => json!(r.below(5) as i64 - 2),
        3 => json!(u64::MAX - r.below(3)),
        4 => json!(*r.pick(&["text", "x", "{\"a\":", "}", "", "text_delta"])),
        5 => json!(-1.5),
        6 => Value::Array((0..r.below(3)).map(|_| rand_val(r, depth + 1)).collect()),
        _ => {
            const KEYS: &[&str] = &[
                "type", "index", "text", "thinking", "signature", "partial_json", "citation", "citations",
                "content", "content_block", "delta", "usage", "message", "input", "stop_reason",
            ];
            let mut m = Map::new();
            for _ in 0..r.below(5) {
                m.insert(r.pick(KEYS).to_string(), rand_val(r, depth + 1));
            }
            Value::Object(m)
        }
    }
}

fn rand_event(r: &mut Rng) -> (String, Value) {
    const KINDS: &[&str] = &[
        "message_start", "content_block_start", "content_block_delta", "content_block_stop",
        "message_delta", "message_stop", "ping", "error", "weird",
    ];
    const DELTAS: &[&str] = &[
        "text_delta", "thinking_delta", "signature_delta", "input_json_delta", "citations_delta", "future_delta",
    ];
    let kind = r.pick(KINDS).to_string();
    let mut ev = if r.below(3) == 0 { rand_val(r, 0) } else { json!({}) };
    if let Some(o) = ev.as_object_mut() {
        if r.below(2) == 0 {
            o.insert("index".into(), json!(r.below(3)));
        }
        if kind == "content_block_delta" && r.below(2) == 0 {
            let mut d = rand_val(r, 1);
            if !d.is_object() {
                d = json!({});
            }
            let dm = d.as_object_mut().unwrap();
            dm.insert("type".into(), json!(*r.pick(DELTAS)));
            if r.below(2) == 0 {
                dm.insert("partial_json".into(), json!(*r.pick(&["{", "\"a\"", ":1", "}", ""])));
            }
            o.insert("delta".into(), d);
        }
        if kind == "content_block_start" && r.below(2) == 0 {
            o.insert("content_block".into(), rand_val(r, 1));
        }
        if kind == "message_start" && r.below(2) == 0 {
            o.insert("message".into(), json!({"id": "m", "content": rand_val(r, 1), "usage": rand_val(r, 1)}));
        }
    }
    (kind, ev)
}

#[test]
fn aggregator_random_events_never_panic() {
    let mut r = Rng(0xDEAD_BEEF_CAFE_BABE);
    for _ in 0..30_000 {
        let mut a = Aggregator::new();
        for _ in 0..r.below(25) {
            let (kind, ev) = rand_event(&mut r);
            let _ = a.apply(&kind, &ev);
        }
        serde_json::to_vec(&a.finish()).unwrap();
    }
}

#[test]
fn errmap_random_bodies_never_panic() {
    const ALPHABET: &[u8] = &[b'<', b't', b'i', b'l', b'e', b'>', b'/', 0xe4, 0xb8, 0xad, b'T', b'I', 0xff, b' '];
    let mut r = Rng(12345);
    for _ in 0..20_000 {
        let mut v: Vec<u8> = (0..r.below(80)).map(|_| *r.pick(ALPHABET)).collect();
        if r.below(3) == 0 {
            v.splice(0..0, b"<title>".iter().copied());
        }
        let _ = errmap::body_message(&v);
        let _ = errmap::html_title(&v);
        let s = String::from_utf8_lossy(&v);
        for max in 0..s.len() + 2 {
            let _ = errmap::truncate(&s, max);
        }
    }
}
