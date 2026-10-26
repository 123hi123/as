//! as-shim ("as" = anthropic to stream): a local proxy that keeps Claude Code's requests to an
//! Anthropic-compatible gateway streaming, so a Cloudflare front never sees 125 s of silence.
//! With a server that implements the as resume protocol v1 (`docs/RESUME.md`), broken streams
//! are resumed byte-exactly; every request is logged to a local SQLite file.

pub mod log;

pub mod aggregate;
pub mod config;
pub mod errmap;
pub mod logdb;
pub mod proxy;
pub mod resume;
pub mod sanitize;
pub mod service;
pub mod setup;
pub mod sse;
pub mod util;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
