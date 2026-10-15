//! Configuration file (`~/.config/as-shim/config.toml`) and URL helpers.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

pub const DEFAULT_PORT: u16 = 8787;

fn d_listen() -> String {
    format!("127.0.0.1:{DEFAULT_PORT}")
}
fn d_silent_retries() -> u32 {
    1
}
fn d_silent_retry_max_elapsed_secs() -> u64 {
    60
}
fn d_silent_retry_delay_ms() -> u64 {
    1000
}
fn d_connect_timeout_secs() -> u64 {
    30
}
fn d_max_body_mib() -> u64 {
    64
}
fn d_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    /// Upstream Anthropic-compatible base URL (no trailing slash), e.g. https://api.example.com
    pub upstream: String,
    #[serde(default = "d_listen")]
    pub listen: String,
    /// How many times a stream that dies before any content is silently re-sent.
    #[serde(default = "d_silent_retries")]
    pub silent_retries: u32,
    /// No silent retry once this much time has passed since the request arrived.
    #[serde(default = "d_silent_retry_max_elapsed_secs")]
    pub silent_retry_max_elapsed_secs: u64,
    /// Pause before a silent retry.
    #[serde(default = "d_silent_retry_delay_ms")]
    pub silent_retry_delay_ms: u64,
    #[serde(default = "d_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "d_max_body_mib")]
    pub max_body_mib: u64,
    #[serde(default = "d_log_level")]
    pub log_level: String,
    /// Honour HTTP(S)_PROXY / ALL_PROXY / NO_PROXY from the environment (off by default so the
    /// service and a foreground `run` behave the same).
    #[serde(default)]
    pub use_env_proxy: bool,
    /// ANTHROPIC_BASE_URL that Claude Code used before `setup` pointed it at the shim.
    /// Empty string = the key was absent. Used by `uninstall` to restore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_claude_base_url: Option<String>,
    /// settings.json that `setup` modified (so `uninstall` restores the same file).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_settings_path: Option<String>,
}

impl Config {
    pub fn new(upstream: String) -> Self {
        Config {
            upstream,
            listen: d_listen(),
            silent_retries: d_silent_retries(),
            silent_retry_max_elapsed_secs: d_silent_retry_max_elapsed_secs(),
            silent_retry_delay_ms: d_silent_retry_delay_ms(),
            connect_timeout_secs: d_connect_timeout_secs(),
            max_body_mib: d_max_body_mib(),
            log_level: d_log_level(),
            use_env_proxy: false,
            original_claude_base_url: None,
            claude_settings_path: None,
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let cfg: Config = toml::from_str(text).map_err(|e| format!("invalid config: {e}"))?;
        Ok(cfg)
    }

    /// Keys in the file that this version does not know (likely typos); they are ignored.
    pub fn unknown_keys(text: &str) -> Vec<String> {
        const KNOWN: &[&str] = &[
            "upstream",
            "listen",
            "silent_retries",
            "silent_retry_max_elapsed_secs",
            "silent_retry_delay_ms",
            "connect_timeout_secs",
            "max_body_mib",
            "log_level",
            "use_env_proxy",
            "original_claude_base_url",
            "claude_settings_path",
        ];
        match text.parse::<toml::Table>() {
            Ok(t) => t
                .keys()
                .filter(|k| !KNOWN.contains(&k.as_str()))
                .cloned()
                .collect(),
            Err(_) => vec![],
        }
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    pub fn load_optional(path: &Path) -> Result<Option<Self>, String> {
        if path.exists() {
            Self::load(path).map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn to_toml(&self) -> String {
        let body = toml::to_string(self).unwrap_or_default();
        format!(
            "# as-shim (anthropic to stream) configuration\n\
             # Edit, then `as-shim set-upstream <URL>` or restart the service to apply.\n\
             {body}"
        )
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        }
        crate::util::write_atomic(path, self.to_toml().as_bytes(), Some(0o600))
            .map_err(|e| format!("cannot write {}: {e}", path.display()))
    }

    /// Validate everything `run` depends on.
    pub fn validate(&self) -> Result<(), String> {
        normalize_upstream(&self.upstream)?;
        parse_listen(&self.listen)?;
        if crate::log::parse_level(&self.log_level).is_none() {
            return Err(format!(
                "log_level must be error|warn|info|debug, got {:?}",
                self.log_level
            ));
        }
        if self.max_body_mib == 0 {
            return Err("max_body_mib must be > 0".into());
        }
        Ok(())
    }

    pub fn port(&self) -> Option<u16> {
        parse_listen(&self.listen).ok().map(|a| a.port())
    }
}

/// `listen` must be a loopback socket address.
pub fn parse_listen(s: &str) -> Result<SocketAddr, String> {
    let addr: SocketAddr = s
        .trim()
        .parse()
        .map_err(|_| format!("listen must look like 127.0.0.1:8787, got {s:?}"))?;
    if !addr.ip().is_loopback() {
        return Err(format!(
            "listen address {addr} is not loopback; as-shim only listens on 127.0.0.1"
        ));
    }
    Ok(addr)
}

/// Validate an http(s) base URL and strip trailing slashes.
pub fn normalize_upstream(s: &str) -> Result<String, String> {
    let t = s.trim().trim_end_matches('/');
    if t.is_empty() {
        return Err("upstream URL is empty".into());
    }
    if t.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(format!("upstream URL contains whitespace or control characters: {t:?}"));
    }
    let url = reqwest::Url::parse(t).map_err(|e| format!("invalid URL {t:?}: {e}"))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err("upstream URL must not contain user:password@ credentials".into());
    }
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(format!("upstream must be http:// or https://, got {t:?}"));
    }
    if url.host_str().map(|h| h.is_empty()).unwrap_or(true) {
        return Err(format!("upstream URL has no host: {t:?}"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(format!(
            "upstream URL must not contain a query or fragment: {t:?}"
        ));
    }
    Ok(t.to_string())
}

/// True when the URL points at this machine (127.0.0.0/8, ::1, localhost).
pub fn is_local_url(s: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(s.trim()) else {
        return false;
    };
    match url.host() {
        Some(url::Host::Domain(d)) => d.trim_end_matches('.').eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback() || ip.is_unspecified(),
        Some(url::Host::Ipv6(ip)) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|v4| v4.is_loopback() || v4.is_unspecified())
        }
        None => false,
    }
}

/// Port of a URL (explicit or scheme default).
pub fn url_port(s: &str) -> Option<u16> {
    reqwest::Url::parse(s.trim())
        .ok()
        .and_then(|u| u.port_or_known_default())
}

pub fn shim_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

pub fn home() -> PathBuf {
    home_dir()
}

/// `$XDG_CONFIG_HOME` or `~/.config`.
pub fn xdg_config_home() -> PathBuf {
    match std::env::var_os("XDG_CONFIG_HOME") {
        Some(p) if !p.is_empty() && Path::new(&p).is_absolute() => PathBuf::from(p),
        _ => home_dir().join(".config"),
    }
}

/// Claude Code user settings: `$CLAUDE_CONFIG_DIR/settings.json` or `~/.claude/settings.json`.
pub fn claude_settings_path() -> PathBuf {
    match std::env::var_os("CLAUDE_CONFIG_DIR").filter(|p| !p.is_empty()) {
        Some(d) => PathBuf::from(d).join("settings.json"),
        None => home_dir().join(".claude").join("settings.json"),
    }
}

pub fn default_config_path() -> PathBuf {
    if let Some(p) = std::env::var_os("AS_SHIM_CONFIG").filter(|p| !p.is_empty()) {
        return PathBuf::from(p);
    }
    xdg_config_home().join("as-shim").join("config.toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize() {
        assert_eq!(
            normalize_upstream(" https://a.example.com/ ").unwrap(),
            "https://a.example.com"
        );
        assert_eq!(
            normalize_upstream("http://h:8080/prefix//").unwrap(),
            "http://h:8080/prefix"
        );
        assert!(normalize_upstream("ftp://x").is_err());
        assert!(normalize_upstream("nonsense").is_err());
        assert!(normalize_upstream("https://x/?a=1").is_err());
        assert!(normalize_upstream("https://gw.example.com/x\ny").is_err());
        assert!(normalize_upstream("https://u:p@gw.example.com").is_err());
    }

    #[test]
    fn local() {
        assert!(is_local_url("http://127.0.0.1:8787"));
        assert!(is_local_url("http://localhost:1"));
        assert!(is_local_url("http://[::1]:1"));
        assert!(is_local_url("http://localhost.:8787"));
        assert!(is_local_url("http://[::ffff:127.0.0.1]:8787"));
        assert!(!is_local_url("https://api.example.com"));
    }

    #[test]
    fn listen_loopback_only() {
        assert!(parse_listen("127.0.0.1:8787").is_ok());
        assert!(parse_listen("0.0.0.0:8787").is_err());
        assert!(parse_listen("192.168.1.2:8787").is_err());
    }

    #[test]
    fn roundtrip_defaults() {
        let c = Config::parse("upstream = \"https://x.example\"\n").unwrap();
        assert_eq!(c.listen, "127.0.0.1:8787");
        assert_eq!(c.silent_retries, 1);
        assert_eq!(c.silent_retry_max_elapsed_secs, 60);
        assert_eq!(c.connect_timeout_secs, 30);
        assert_eq!(c.max_body_mib, 64);
        let back = Config::parse(&c.to_toml()).unwrap();
        assert_eq!(back, c);
        assert!(Config::parse("listen = \"127.0.0.1:1\"\n").is_err());
        assert_eq!(
            Config::unknown_keys("upstream = \"https://x\"\nsilent_retry = 2\n"),
            vec!["silent_retry".to_string()]
        );
    }
}
