//! Incremental Server-Sent-Events frame parser that keeps the raw bytes of every frame,
//! so frames can be relayed byte-for-byte.
//!
//! Handles `\n`, `\r\n` and `\r` line endings, multi-line `data:`, `:` comment lines and
//! frames split across arbitrary chunk boundaries.

use bytes::{Bytes, BytesMut};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Frame {
    /// Exact upstream bytes of this frame, including the terminating blank line.
    pub raw: Bytes,
    /// Value of the last `event:` field, if any.
    pub event: Option<String>,
    /// `data:` lines joined with `\n`; `None` when the frame had no data field.
    pub data: Option<String>,
}

impl Frame {
    /// Parse `data` as JSON (None when absent or not JSON).
    pub fn json(&self) -> Option<Value> {
        self.data.as_deref().and_then(parse_json_lenient)
    }

    /// Dispatch type: the JSON `"type"` field, falling back to the SSE event name.
    pub fn kind(&self, json: Option<&Value>) -> Option<String> {
        if let Some(t) = json.and_then(|v| v.get("type")).and_then(|t| t.as_str()) {
            return Some(t.to_string());
        }
        self.event.clone()
    }

    /// True for frames that carry nothing (only comments / unknown fields).
    pub fn is_empty(&self) -> bool {
        self.event.is_none() && self.data.is_none()
    }
}

/// `serde_json::from_str`, retrying once with lone UTF-16 surrogate escapes (which
/// JavaScript's `JSON.parse` accepts) replaced by U+FFFD.
pub fn parse_json_lenient(s: &str) -> Option<Value> {
    match serde_json::from_str::<Value>(s) {
        Ok(v) => Some(v),
        Err(_) if s.contains("\\u") => {
            serde_json::from_str::<Value>(&fix_lone_surrogates(s)).ok()
        }
        Err(_) => None,
    }
}

fn hex4(b: &[u8]) -> Option<u16> {
    if b.len() < 4 {
        return None;
    }
    u16::from_str_radix(std::str::from_utf8(&b[..4]).ok()?, 16).ok()
}

fn fix_lone_surrogates(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let mut copied = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            i += 1;
            continue;
        }
        if i + 1 < b.len() && b[i + 1] == b'u' {
            if let Some(cu) = hex4(&b[i + 2..]) {
                let next = (i + 6 < b.len() && b[i + 6] == b'\\' && b.get(i + 7) == Some(&b'u'))
                    .then(|| hex4(&b[i + 8..]))
                    .flatten();
                let lone = match cu {
                    0xD800..=0xDBFF => !matches!(next, Some(0xDC00..=0xDFFF)),
                    0xDC00..=0xDFFF => true,
                    _ => false,
                };
                if lone {
                    out.push_str(&s[copied..i]);
                    out.push_str("\\ufffd");
                    i += 6;
                    copied = i;
                    continue;
                }
                if (0xD800..=0xDBFF).contains(&cu) {
                    i += 12;
                    continue;
                }
                i += 6;
                continue;
            }
        }
        i += 2;
    }
    out.push_str(&s[copied.min(s.len())..]);
    out
}

#[derive(Default)]
pub struct SseParser {
    buf: BytesMut,
    /// Offset in `buf` of the next unparsed line; bytes before it belong to the current frame.
    line_start: usize,
    /// `buf[line_start..scan_pos]` is known to contain no line terminator (keeps scanning O(n)).
    scan_pos: usize,
    event: Option<String>,
    data: Option<String>,
    eof: bool,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// Mark end of input: a trailing lone `\r` now counts as a line terminator.
    pub fn set_eof(&mut self) {
        self.eof = true;
    }

    /// At end of input: interpret whatever is left (an unterminated last frame) leniently.
    /// Only used to classify a truncated trailing frame; its `raw` is the leftover bytes.
    pub fn finish(&mut self) -> Option<Frame> {
        self.eof = true;
        if let Some(f) = self.next_frame() {
            return Some(f);
        }
        if self.buf.is_empty() {
            return None;
        }
        let leftover = self.buf.clone().freeze();
        self.buf.extend_from_slice(b"\n\n");
        let f = self.next_frame()?;
        self.buf.clear();
        self.line_start = 0;
        self.scan_pos = 0;
        Some(Frame { raw: leftover, ..f }).filter(|f| !f.is_empty())
    }

    /// Bytes buffered but not yet returned as a complete frame.
    pub fn pending_len(&self) -> usize {
        self.buf.len()
    }

    /// Take every byte not yet returned as a frame (used when switching to raw passthrough).
    pub fn take_remaining(&mut self) -> Bytes {
        self.line_start = 0;
        self.scan_pos = 0;
        self.event = None;
        self.data = None;
        self.buf.split().freeze()
    }

    /// Next complete frame, if one is fully buffered.
    pub fn next_frame(&mut self) -> Option<Frame> {
        loop {
            let from = self.scan_pos.max(self.line_start);
            let Some(found) = self.buf[from..].iter().position(|&b| b == b'\n' || b == b'\r') else {
                self.scan_pos = self.buf.len();
                return None;
            };
            let pos = from - self.line_start + found;
            let rest = &self.buf[self.line_start..];
            let term_len = if rest[pos] == b'\r' {
                if pos + 1 >= rest.len() {
                    if !self.eof {
                        // A lone '\r' at the end: we cannot know yet whether '\n' follows.
                        self.scan_pos = self.line_start + pos;
                        return None;
                    }
                    1
                } else if rest[pos + 1] == b'\n' {
                    2
                } else {
                    1
                }
            } else {
                1
            };
            let line_end = self.line_start + pos;
            let next = line_end + term_len;
            if pos == 0 {
                // Blank line: dispatch.
                let raw = self.buf.split_to(next).freeze();
                self.line_start = 0;
                self.scan_pos = 0;
                return Some(Frame {
                    raw,
                    event: self.event.take(),
                    data: self.data.take(),
                });
            }
            let line = &self.buf[self.line_start..line_end];
            if line[0] != b':' {
                let (name, value) = match line.iter().position(|&b| b == b':') {
                    Some(i) => {
                        let v = &line[i + 1..];
                        let v = v.strip_prefix(b" ").unwrap_or(v);
                        (&line[..i], v)
                    }
                    None => (line, &b""[..]),
                };
                match name {
                    b"event" => self.event = Some(String::from_utf8_lossy(value).into_owned()),
                    b"data" => {
                        let v = String::from_utf8_lossy(value);
                        match &mut self.data {
                            Some(d) => {
                                d.push('\n');
                                d.push_str(&v);
                            }
                            None => self.data = Some(v.into_owned()),
                        }
                    }
                    _ => {}
                }
            }
            self.line_start = next;
            self.scan_pos = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(p: &mut SseParser) -> Vec<Frame> {
        let mut v = vec![];
        while let Some(f) = p.next_frame() {
            v.push(f);
        }
        v
    }

    #[test]
    fn basic_frames() {
        let mut p = SseParser::new();
        p.push(b"event: ping\ndata: {\"type\": \"ping\"}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
        let f = all(&mut p);
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].event.as_deref(), Some("ping"));
        assert_eq!(f[0].data.as_deref(), Some("{\"type\": \"ping\"}"));
        assert_eq!(&f[0].raw[..], b"event: ping\ndata: {\"type\": \"ping\"}\n\n");
        assert_eq!(f[1].kind(f[1].json().as_ref()).as_deref(), Some("message_stop"));
        assert_eq!(p.pending_len(), 0);
    }

    #[test]
    fn split_at_every_byte() {
        let input: &[u8] =
            b"event: a\r\ndata: 1\r\n\r\n: comment\ndata: x\ndata: y\n\nevent: b\rdata: {\"type\":\"z\"}\r\r";
        // Feed one byte at a time; frames must be identical to a single push.
        let mut p = SseParser::new();
        let mut frames = vec![];
        for b in input {
            p.push(std::slice::from_ref(b));
            while let Some(f) = p.next_frame() {
                frames.push(f);
            }
        }
        assert_eq!(frames.len(), 2, "trailing lone CR must wait for more input");
        p.set_eof();
        while let Some(f) = p.next_frame() {
            frames.push(f);
        }
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].event.as_deref(), Some("a"));
        assert_eq!(frames[0].data.as_deref(), Some("1"));
        assert_eq!(&frames[0].raw[..], b"event: a\r\ndata: 1\r\n\r\n");
        assert_eq!(frames[1].event, None);
        assert_eq!(frames[1].data.as_deref(), Some("x\ny"));
        assert_eq!(frames[2].event.as_deref(), Some("b"));
        assert_eq!(frames[2].kind(frames[2].json().as_ref()).as_deref(), Some("z"));
        let joined: Vec<u8> = frames.iter().flat_map(|f| f.raw.to_vec()).collect();
        assert_eq!(joined, input);
    }

    #[test]
    fn crlf_split_between_cr_and_lf() {
        let mut p = SseParser::new();
        p.push(b"data: 1\r");
        assert!(p.next_frame().is_none());
        p.push(b"\n\r");
        assert!(p.next_frame().is_none());
        p.push(b"\n");
        let f = p.next_frame().unwrap();
        assert_eq!(f.data.as_deref(), Some("1"));
        assert_eq!(&f.raw[..], b"data: 1\r\n\r\n");
    }

    #[test]
    fn comments_and_no_space() {
        let mut p = SseParser::new();
        p.push(b": keepalive\n\ndata:{\"type\":\"ping\"}\nid: 7\nretry: 10\n\n");
        let f = all(&mut p);
        assert_eq!(f.len(), 2);
        assert!(f[0].is_empty());
        assert_eq!(f[1].data.as_deref(), Some("{\"type\":\"ping\"}"));
        assert_eq!(f[1].kind(f[1].json().as_ref()).as_deref(), Some("ping"));
    }

    #[test]
    fn data_only_error_kind_from_json() {
        let mut p = SseParser::new();
        p.push(b"data: {\"type\":\"error\",\"error\":{\"type\":\"upstream_error\",\"message\":\"Upstream request failed\"}}\n\n");
        let f = p.next_frame().unwrap();
        assert_eq!(f.event, None);
        assert_eq!(f.kind(f.json().as_ref()).as_deref(), Some("error"));
    }

    #[test]
    fn event_name_fallback_and_remaining() {
        let mut p = SseParser::new();
        p.push(b"event: error\ndata: not json\n\nevent: x\ndata: partial");
        let f = p.next_frame().unwrap();
        assert_eq!(f.kind(f.json().as_ref()).as_deref(), Some("error"));
        assert!(p.next_frame().is_none());
        assert_eq!(&p.take_remaining()[..], b"event: x\ndata: partial");
        assert_eq!(p.pending_len(), 0);
        p.push(b"data: 2\n\n");
        let f = p.next_frame().unwrap();
        assert_eq!(f.event, None);
        assert_eq!(f.data.as_deref(), Some("2"));
    }

    #[test]
    fn finish_unterminated_error() {
        let mut p = SseParser::new();
        p.push(b"event: ping\ndata: {}\n\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}");
        assert!(p.next_frame().is_some());
        assert!(p.next_frame().is_none());
        let f = p.finish().unwrap();
        assert_eq!(f.kind(f.json().as_ref()).as_deref(), Some("error"));
        assert!(p.finish().is_none());
        let mut q = SseParser::new();
        q.push(b": only a comment");
        assert!(q.finish().is_none());
    }

    #[test]
    fn lone_surrogates_are_replaced() {
        let v = parse_json_lenient(r#"{"t":"a\ud83d b \udc00 \ud83d\ude00 \\ud83d"}"#).unwrap();
        assert_eq!(v["t"], "a\u{fffd} b \u{fffd} \u{1f600} \\ud83d");
        assert!(parse_json_lenient("{").is_none());
    }

    #[test]
    fn long_line_is_linear() {
        let mut p = SseParser::new();
        p.push(b"data: ");
        let chunk = vec![b'x'; 4096];
        let t = std::time::Instant::now();
        for _ in 0..4096 {
            p.push(&chunk);
            assert!(p.next_frame().is_none());
        }
        p.push(b"\n\n");
        let f = p.next_frame().unwrap();
        assert_eq!(f.data.unwrap().len(), 4096 * 4096);
        assert!(t.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn empty_data_field() {
        let mut p = SseParser::new();
        p.push(b"data\n\ndata:\ndata:\n\n");
        let f = all(&mut p);
        assert_eq!(f[0].data.as_deref(), Some(""));
        assert_eq!(f[1].data.as_deref(), Some("\n"));
    }
}
