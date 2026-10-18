//! SQLite request log: one row per proxied request plus one row per resume attempt.
//!
//! Rows are handed to a dedicated writer thread through a bounded channel, so a slow or broken
//! database never delays or fails a request; problems are only reported on stderr.
//! Never store credentials or bodies here.

use crate::log_warn;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Rows waiting for the writer; when full, new rows are dropped (with one warning).
const QUEUE: usize = 4096;
/// Retention cleanup runs at startup and then this often.
const CLEANUP_EVERY: Duration = Duration::from_secs(3600);
/// At most one write-failure warning per this interval.
const WARN_EVERY: Duration = Duration::from_secs(60);

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS requests (
    id                  INTEGER PRIMARY KEY,
    ts_start_ms         INTEGER NOT NULL,
    ts_end_ms           INTEGER NOT NULL,
    method              TEXT    NOT NULL,
    path                TEXT    NOT NULL,
    model               TEXT,
    client_stream       INTEGER,
    mode                TEXT    NOT NULL,
    status              INTEGER,
    upstream_status     INTEGER,
    duration_ms         INTEGER NOT NULL,
    ttfb_ms             INTEGER,
    bytes_out           INTEGER NOT NULL DEFAULT 0,
    outcome             TEXT    NOT NULL,
    silent_retries      INTEGER NOT NULL DEFAULT 0,
    resume_id           TEXT,
    resume_attempts     INTEGER NOT NULL DEFAULT 0,
    resume_recovered    INTEGER NOT NULL DEFAULT 0,
    upstream_request_id TEXT,
    error               TEXT
);
CREATE INDEX IF NOT EXISTS requests_ts_start_ms ON requests(ts_start_ms);
CREATE TABLE IF NOT EXISTS resume_events (
    id          INTEGER PRIMARY KEY,
    request_row INTEGER NOT NULL,
    resume_id   TEXT    NOT NULL,
    ts_ms       INTEGER NOT NULL,
    attempt     INTEGER NOT NULL,
    "offset"    INTEGER NOT NULL,
    "trigger"   TEXT    NOT NULL,
    result      TEXT    NOT NULL,
    error       TEXT
);
CREATE INDEX IF NOT EXISTS resume_events_request_row ON resume_events(request_row);
CREATE INDEX IF NOT EXISTS resume_events_ts_ms ON resume_events(ts_ms);
PRAGMA user_version = 1;
"#;

/// One finished request (table `requests`) and its resume attempts (table `resume_events`).
#[derive(Debug, Clone, Default)]
pub struct RequestRow {
    pub ts_start_ms: i64,
    pub ts_end_ms: i64,
    pub method: String,
    pub path: String,
    pub model: Option<String>,
    pub client_stream: Option<bool>,
    /// `N`, `S` or `passthrough`.
    pub mode: String,
    /// Status sent to Claude Code.
    pub status: Option<u16>,
    pub upstream_status: Option<u16>,
    pub duration_ms: i64,
    pub ttfb_ms: Option<i64>,
    pub bytes_out: u64,
    pub outcome: String,
    pub silent_retries: u32,
    pub resume_id: Option<String>,
    pub resume_attempts: u32,
    pub resume_recovered: bool,
    pub upstream_request_id: Option<String>,
    pub error: Option<String>,
    pub events: Vec<ResumeEventRow>,
}

#[derive(Debug, Clone)]
pub struct ResumeEventRow {
    pub resume_id: String,
    pub ts_ms: i64,
    pub attempt: u32,
    pub offset: u64,
    /// `io_error` / `eof_no_stop` / `idle` / `pre_content`.
    pub trigger: &'static str,
    /// `ok` / `http_<code>` / `io_error` / `budget`.
    pub result: String,
    pub error: Option<String>,
}

enum Msg {
    Row(Box<RequestRow>),
    Flush(SyncSender<()>),
}

/// Cheap, cloneable handle used by request handlers.
#[derive(Clone)]
pub struct LogSink {
    tx: SyncSender<Msg>,
    full_warned: Arc<AtomicBool>,
}

impl LogSink {
    /// Queue a row; never blocks.
    pub fn send(&self, row: RequestRow) {
        match self.tx.try_send(Msg::Row(Box::new(row))) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                if !self.full_warned.swap(true, Ordering::Relaxed) {
                    log_warn!("request log: queue full, dropping rows");
                }
            }
            // The writer already reported why it stopped.
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    /// Block until every row queued before this call is written (or `timeout` passes).
    pub fn flush(&self, timeout: Duration) -> bool {
        let (tx, rx) = mpsc::sync_channel(1);
        let deadline = Instant::now() + timeout;
        let mut msg = Msg::Flush(tx);
        loop {
            match self.tx.try_send(msg) {
                Ok(()) => break,
                Err(TrySendError::Disconnected(_)) => return false,
                Err(TrySendError::Full(m)) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    msg = m;
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .is_ok()
    }
}

/// Start the writer thread. The database is opened (and cleaned) on that thread, so a slow
/// filesystem does not delay startup.
pub fn start(path: PathBuf, retention_days: u64) -> LogSink {
    let (tx, rx) = mpsc::sync_channel(QUEUE);
    let spawned = std::thread::Builder::new()
        .name("as-shim-logdb".into())
        .spawn(move || writer(path, retention_days, rx));
    if let Err(e) = spawned {
        log_warn!("request log disabled: cannot start writer thread: {e}");
    }
    LogSink {
        tx,
        full_warned: Arc::new(AtomicBool::new(false)),
    }
}

fn writer(path: PathBuf, retention_days: u64, rx: Receiver<Msg>) {
    let mut conn = match open(&path) {
        Ok(c) => c,
        Err(e) => {
            log_warn!("request log disabled: {e}");
            return;
        }
    };
    let mut last_warn: Option<Instant> = None;
    let mut warn = |what: &str, e: &dyn std::fmt::Display| {
        if last_warn.is_none_or(|t| t.elapsed() >= WARN_EVERY) {
            last_warn = Some(Instant::now());
            log_warn!("request log: {what} failed: {e}");
        }
    };
    if let Err(e) = cleanup(&conn, retention_days, crate::util::unix_ms()) {
        warn("retention cleanup", &e);
    }
    let mut next_cleanup = Instant::now() + CLEANUP_EVERY;
    loop {
        let wait = next_cleanup.saturating_duration_since(Instant::now());
        match rx.recv_timeout(wait) {
            Ok(first) => {
                let mut rows = Vec::new();
                let mut flushes = Vec::new();
                let mut next = Some(first);
                while let Some(m) = next.take() {
                    match m {
                        Msg::Row(r) => rows.push(*r),
                        Msg::Flush(ack) => flushes.push(ack),
                    }
                    if rows.len() < 256 {
                        next = rx.try_recv().ok();
                    }
                }
                if !rows.is_empty() {
                    if let Err(e) = write_rows(&mut conn, &rows) {
                        warn("write", &e);
                    }
                }
                for f in flushes {
                    let _ = f.try_send(());
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Err(e) = cleanup(&conn, retention_days, crate::util::unix_ms()) {
                    warn("retention cleanup", &e);
                }
                next_cleanup = Instant::now() + CLEANUP_EVERY;
            }
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Open (creating if needed) the log database in WAL mode with the current schema.
pub fn open(path: &Path) -> Result<Connection, String> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    }
    {
        // Create the file private to the user; SQLite gives -wal / -shm the same mode.
        use std::os::unix::fs::OpenOptionsExt;
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path);
    }
    let fail = |e: rusqlite::Error| format!("cannot open {}: {e}", path.display());
    let conn = Connection::open(path).map_err(fail)?;
    conn.busy_timeout(Duration::from_secs(5)).map_err(fail)?;
    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
        .map_err(fail)?;
    if !mode.eq_ignore_ascii_case("wal") {
        log_warn!("request log: journal_mode is {mode}, not wal");
    }
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(fail)?;
    conn.execute_batch(SCHEMA).map_err(fail)?;
    Ok(conn)
}

fn opt_i64(v: Option<u16>) -> Option<i64> {
    v.map(i64::from)
}

fn sat_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Insert rows (and their resume events) in one transaction.
pub fn write_rows(conn: &mut Connection, rows: &[RequestRow]) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    {
        let mut ins = tx.prepare_cached(
            "INSERT INTO requests (ts_start_ms, ts_end_ms, method, path, model, client_stream, mode, \
             status, upstream_status, duration_ms, ttfb_ms, bytes_out, outcome, silent_retries, \
             resume_id, resume_attempts, resume_recovered, upstream_request_id, error) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
        )?;
        let mut ev = tx.prepare_cached(
            "INSERT INTO resume_events (request_row, resume_id, ts_ms, attempt, \"offset\", \"trigger\", result, error) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for r in rows {
            ins.execute(params![
                r.ts_start_ms,
                r.ts_end_ms,
                r.method,
                r.path,
                r.model,
                r.client_stream,
                r.mode,
                opt_i64(r.status),
                opt_i64(r.upstream_status),
                r.duration_ms,
                r.ttfb_ms,
                sat_i64(r.bytes_out),
                r.outcome,
                r.silent_retries,
                r.resume_id,
                r.resume_attempts,
                r.resume_recovered,
                r.upstream_request_id,
                r.error,
            ])?;
            let row_id = tx.last_insert_rowid();
            for e in &r.events {
                ev.execute(params![
                    row_id,
                    e.resume_id,
                    e.ts_ms,
                    e.attempt,
                    sat_i64(e.offset),
                    e.trigger,
                    e.result,
                    e.error,
                ])?;
            }
        }
    }
    tx.commit()
}

/// Delete rows older than `retention_days` (0 = keep forever). Returns deleted request rows.
pub fn cleanup(conn: &Connection, retention_days: u64, now_ms: i64) -> rusqlite::Result<usize> {
    if retention_days == 0 {
        return Ok(0);
    }
    let days = i64::try_from(retention_days.min(100 * 365)).unwrap_or(36_500);
    let cutoff = now_ms.saturating_sub(days.saturating_mul(86_400_000));
    conn.execute(
        "DELETE FROM resume_events WHERE ts_ms < ?1 \
         OR request_row IN (SELECT id FROM requests WHERE ts_start_ms < ?1)",
        [cutoff],
    )?;
    conn.execute("DELETE FROM requests WHERE ts_start_ms < ?1", [cutoff])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("as-shim-logdb-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d.join("sub").join("log.sqlite3")
    }

    fn row(ts: i64) -> RequestRow {
        RequestRow {
            ts_start_ms: ts,
            ts_end_ms: ts + 5,
            method: "POST".into(),
            path: "/v1/messages".into(),
            mode: "S".into(),
            outcome: "ok".into(),
            status: Some(200),
            events: vec![ResumeEventRow {
                resume_id: "0".repeat(32),
                ts_ms: ts + 1,
                attempt: 1,
                offset: 10,
                trigger: "io_error",
                result: "ok".into(),
                error: None,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn schema_wal_insert_and_retention() {
        let p = tmp("ret");
        let mut c = open(&p).unwrap();
        let mode: String = c
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        let now = 100 * 86_400_000;
        write_rows(&mut c, &[row(now - 31 * 86_400_000), row(now - 86_400_000)]).unwrap();
        let count = |c: &Connection, t: &str| -> i64 {
            c.query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(count(&c, "requests"), 2);
        assert_eq!(count(&c, "resume_events"), 2);
        assert_eq!(cleanup(&c, 0, now).unwrap(), 0, "0 = keep forever");
        assert_eq!(cleanup(&c, 30, now).unwrap(), 1);
        assert_eq!(count(&c, "requests"), 1);
        assert_eq!(count(&c, "resume_events"), 1);
        let off: i64 = c
            .query_row("SELECT offset FROM resume_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(off, 10, "unquoted column names work in queries");
        drop(c);
        // Re-opening an existing database keeps the data.
        let c = open(&p).unwrap();
        assert_eq!(count(&c, "requests"), 1);
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let _ = std::fs::remove_dir_all(p.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn writer_thread_writes_and_flushes() {
        let p = tmp("thread");
        let sink = start(p.clone(), 30);
        sink.send(row(crate::util::unix_ms()));
        assert!(sink.flush(Duration::from_secs(10)));
        let c = Connection::open(&p).unwrap();
        let n: i64 = c
            .query_row("SELECT count(*) FROM requests", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        let _ = std::fs::remove_dir_all(p.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn unopenable_path_is_not_fatal() {
        let sink = start(PathBuf::from("/proc/as-shim-nope/log.sqlite3"), 30);
        sink.send(row(1));
        assert!(!sink.flush(Duration::from_secs(5)));
    }
}
