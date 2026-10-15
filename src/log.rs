//! Minimal stderr logger (journald friendly). Never log headers, keys or bodies.

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ERROR: u8 = 0;
pub const WARN: u8 = 1;
pub const INFO: u8 = 2;
pub const DEBUG: u8 = 3;

static LEVEL: AtomicU8 = AtomicU8::new(INFO);

pub fn parse_level(s: &str) -> Option<u8> {
    match s.trim().to_ascii_lowercase().as_str() {
        "error" => Some(ERROR),
        "warn" | "warning" => Some(WARN),
        "info" => Some(INFO),
        "debug" | "trace" => Some(DEBUG),
        _ => None,
    }
}

pub fn set_level(level: u8) {
    LEVEL.store(level, Ordering::Relaxed);
}

#[inline]
pub fn enabled(level: u8) -> bool {
    level <= LEVEL.load(Ordering::Relaxed)
}

fn under_journald() -> bool {
    std::env::var_os("JOURNAL_STREAM").is_some()
}

/// UTC timestamp `YYYY-MM-DDTHH:MM:SS.mmmZ` without pulling in a date crate.
fn timestamp() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs() as i64;
    let ms = now.subsec_millis();
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // civil_from_days (Howard Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y,
        m,
        d,
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60,
        ms
    )
}

pub fn write(level: u8, args: fmt::Arguments<'_>) {
    let (prio, name) = match level {
        ERROR => (3, "ERROR"),
        WARN => (4, "WARN"),
        INFO => (6, "INFO"),
        _ => (7, "DEBUG"),
    };
    let line = if under_journald() {
        // systemd parses the <N> syslog priority prefix and adds its own timestamp.
        format!("<{prio}>{args}\n")
    } else {
        format!("{} {:5} {args}\n", timestamp(), name)
    };
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

#[macro_export]
macro_rules! log_error { ($($t:tt)*) => { if $crate::log::enabled($crate::log::ERROR) { $crate::log::write($crate::log::ERROR, format_args!($($t)*)) } } }
#[macro_export]
macro_rules! log_warn { ($($t:tt)*) => { if $crate::log::enabled($crate::log::WARN) { $crate::log::write($crate::log::WARN, format_args!($($t)*)) } } }
#[macro_export]
macro_rules! log_info { ($($t:tt)*) => { if $crate::log::enabled($crate::log::INFO) { $crate::log::write($crate::log::INFO, format_args!($($t)*)) } } }
#[macro_export]
macro_rules! log_debug { ($($t:tt)*) => { if $crate::log::enabled($crate::log::DEBUG) { $crate::log::write($crate::log::DEBUG, format_args!($($t)*)) } } }

#[cfg(test)]
mod tests {
    #[test]
    fn timestamp_shape() {
        let t = super::timestamp();
        assert_eq!(t.len(), 24, "{t}");
        assert!(t.ends_with('Z'));
    }

    #[test]
    fn levels() {
        assert_eq!(super::parse_level("WARN"), Some(super::WARN));
        assert_eq!(super::parse_level("nope"), None);
    }
}
