//! `setup`, `status`, `set-upstream` and `uninstall`.

use crate::config::{self, Config, DEFAULT_PORT};
use crate::service;
use serde_json::{Map, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct SetupOpts {
    pub upstream: Option<String>,
    pub port: Option<u16>,
    pub yes: bool,
    pub no_service: bool,
    pub no_claude: bool,
}

// ------------------------------------------------------------------------------ prompts

/// Questions are asked on /dev/tty so `curl ... | bash` still works.
struct Prompter {
    tty: Option<(BufReader<std::fs::File>, std::fs::File)>,
}

impl Prompter {
    fn new(non_interactive: bool) -> Self {
        if non_interactive {
            return Prompter { tty: None };
        }
        let tty = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")
            .ok()
            .and_then(|r| r.try_clone().ok().map(|w| (BufReader::new(r), w)));
        Prompter { tty }
    }

    fn interactive(&self) -> bool {
        self.tty.is_some()
    }

    /// Ask a question; `None` when not interactive (or on EOF).
    fn ask(&mut self, question: &str) -> Option<String> {
        let (r, w) = self.tty.as_mut()?;
        let _ = write!(w, "{question}");
        let _ = w.flush();
        let mut line = String::new();
        match r.read_line(&mut line) {
            Ok(0) | Err(_) => {
                let _ = writeln!(w);
                None
            }
            Ok(_) => Some(line.trim().to_string()),
        }
    }

    fn confirm(&mut self, question: &str, default_yes: bool) -> bool {
        let hint = if default_yes { "[Y/n]" } else { "[y/N]" };
        loop {
            match self.ask(&format!("{question} {hint} ")) {
                None => return default_yes,
                Some(a) if a.is_empty() => return default_yes,
                Some(a) => match a.to_ascii_lowercase().as_str() {
                    "y" | "yes" => return true,
                    "n" | "no" => return false,
                    _ => continue,
                },
            }
        }
    }
}

// ------------------------------------------------------------------------------ Claude settings

pub struct ClaudeSettings {
    pub path: PathBuf,
    pub root: Map<String, Value>,
    pub exists: bool,
}

impl ClaudeSettings {
    pub fn load(path: &Path) -> Result<Self, String> {
        match std::fs::read_to_string(path) {
            Ok(text) if text.trim().is_empty() => Ok(ClaudeSettings {
                path: path.to_path_buf(),
                root: Map::new(),
                exists: true,
            }),
            Ok(text) => match serde_json::from_str::<Value>(&text) {
                Ok(Value::Object(root)) => Ok(ClaudeSettings {
                    path: path.to_path_buf(),
                    root,
                    exists: true,
                }),
                Ok(_) => Err(format!("{} is not a JSON object", path.display())),
                Err(e) => Err(format!("{} is not valid JSON: {e}", path.display())),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ClaudeSettings {
                path: path.to_path_buf(),
                root: Map::new(),
                exists: false,
            }),
            Err(e) => Err(format!("cannot read {}: {e}", path.display())),
        }
    }

    pub fn base_url(&self) -> Option<String> {
        self.root
            .get("env")
            .and_then(|e| e.get("ANTHROPIC_BASE_URL"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    /// `Some(url)` sets env.ANTHROPIC_BASE_URL, `None` removes it.
    pub fn set_base_url(&mut self, url: Option<&str>) {
        if !matches!(self.root.get("env"), Some(Value::Object(_))) {
            if url.is_none() {
                return;
            }
            self.root.insert("env".into(), Value::Object(Map::new()));
        }
        if let Some(Value::Object(env)) = self.root.get_mut("env") {
            match url {
                Some(u) => {
                    env.insert("ANTHROPIC_BASE_URL".into(), Value::String(u.to_string()));
                }
                None => {
                    env.shift_remove("ANTHROPIC_BASE_URL");
                }
            }
        }
    }

    /// Back up the current file (if any), then write atomically. Returns the backup path.
    pub fn save(&self) -> Result<Option<PathBuf>, String> {
        let backup = if self.exists {
            let mut b = self.path.clone().into_os_string();
            b.push(format!(".bak-as-shim-{}", crate::util::unix_now()));
            let mut b = PathBuf::from(b);
            let mut n = 1;
            while b.exists() {
                let mut c = self.path.clone().into_os_string();
                c.push(format!(".bak-as-shim-{}-{n}", crate::util::unix_now()));
                b = PathBuf::from(c);
                n += 1;
            }
            std::fs::copy(&self.path, &b)
                .map_err(|e| format!("cannot back up {}: {e}", self.path.display()))?;
            Some(b)
        } else {
            None
        };
        let mut text = serde_json::to_string_pretty(&Value::Object(self.root.clone()))
            .map_err(|e| e.to_string())?;
        text.push('\n');
        crate::util::write_atomic(&self.path, text.as_bytes(), Some(0o600))
            .map_err(|e| format!("cannot write {}: {e}", self.path.display()))?;
        Ok(backup)
    }
}

/// True when `url` is this shim (loopback host and the shim's port).
fn points_to_shim(url: &str, port: u16) -> bool {
    config::is_local_url(url) && config::url_port(url) == Some(port)
}

// ------------------------------------------------------------------------------ health

pub struct Health {
    pub version: String,
    pub upstream: String,
}

/// GET /__as/health with a plain blocking socket.
pub fn health(port: u16, timeout: Duration) -> Result<Health, String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let mut s = TcpStream::connect_timeout(&addr, timeout).map_err(|e| e.to_string())?;
    let _ = s.set_read_timeout(Some(timeout));
    let _ = s.set_write_timeout(Some(timeout));
    write!(
        s,
        "GET /__as/health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    s.take(64 * 1024)
        .read_to_end(&mut buf)
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or("malformed HTTP response")?;
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(format!(
            "unexpected response: {}",
            head.lines().next().unwrap_or("")
        ));
    }
    let v: Value =
        serde_json::from_str(body.trim()).map_err(|_| "not an as-shim health response")?;
    if v.get("ok") != Some(&Value::Bool(true)) {
        return Err("not an as-shim health response".into());
    }
    Ok(Health {
        version: v["version"].as_str().unwrap_or("?").to_string(),
        upstream: v["upstream"].as_str().unwrap_or("").to_string(),
    })
}

fn wait_healthy(port: u16, upstream: &str, secs: u64) -> Result<Health, String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let last = match health(port, Duration::from_millis(800)) {
            Ok(h) if h.upstream == upstream => return Ok(h),
            Ok(h) => format!("still serving the old upstream {}", h.upstream),
            Err(e) => e,
        };
        if std::time::Instant::now() >= deadline {
            return Err(last);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn port_free(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn next_free_port(from: u16) -> Option<u16> {
    (from.saturating_add(1)..=from.saturating_add(200)).find(|&p| port_free(p))
}

// ------------------------------------------------------------------------------ setup

fn say(msg: impl AsRef<str>) {
    eprintln!("{}", msg.as_ref());
}

fn current_exe() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|e| format!("cannot locate own binary: {e}"))?;
    Ok(std::fs::canonicalize(&exe).unwrap_or(exe))
}

fn env_base_url() -> Option<String> {
    std::env::var("ANTHROPIC_BASE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn setup(opts: SetupOpts) -> Result<(), String> {
    let mut p = Prompter::new(opts.yes);
    let cfg_path = config::default_config_path();
    let existing = Config::load_optional(&cfg_path)?;
    let settings_path = existing
        .as_ref()
        .and_then(|c| c.claude_settings_path.clone())
        .map(PathBuf::from)
        .unwrap_or_else(config::claude_settings_path);
    let settings = ClaudeSettings::load(&settings_path);
    let cc_url = settings.as_ref().ok().and_then(|s| s.base_url());

    say(format!(
        "as-shim {} setup (anthropic to stream)",
        crate::VERSION
    ));
    say(format!("  config:          {}", cfg_path.display()));
    say(format!("  Claude settings: {}", settings_path.display()));
    if let Err(e) = &settings {
        say(format!("  warning: {e}"));
    }

    // 1) upstream
    let default_upstream = cc_url
        .clone()
        .filter(|u| !config::is_local_url(u))
        .or_else(|| existing.as_ref().map(|c| c.upstream.clone()))
        .or_else(|| env_base_url().filter(|u| !config::is_local_url(u)))
        .and_then(|u| config::normalize_upstream(&u).ok());

    // 2) port (needed to reject an upstream that is the shim itself)
    let mut port = opts
        .port
        .or_else(|| existing.as_ref().and_then(|c| c.port()))
        .unwrap_or(DEFAULT_PORT);

    let upstream = loop {
        let candidate = match (&opts.upstream, p.interactive()) {
            (Some(u), _) => u.clone(),
            (None, true) => {
                let q = match &default_upstream {
                    Some(d) => format!("Upstream (Anthropic-compatible base URL) [{d}]: "),
                    None => "Upstream (Anthropic-compatible base URL, e.g. https://api.example.com): ".to_string(),
                };
                match (p.ask(&q), &default_upstream) {
                    (Some(a), _) if !a.is_empty() => a,
                    (_, Some(d)) => d.clone(),
                    (Some(_), None) => continue,
                    (None, None) => return Err("no upstream URL given".into()),
                }
            }
            (None, false) => default_upstream.clone().ok_or(
                "no upstream URL: pass --upstream https://your-gateway (or set ANTHROPIC_BASE_URL in Claude Code settings)",
            )?,
        };
        let err = match config::normalize_upstream(&candidate) {
            Ok(u) if points_to_shim(&u, port) => {
                format!("{u} is the shim's own address; enter the real gateway URL")
            }
            Ok(u) => break u,
            Err(e) => e,
        };
        if opts.upstream.is_some() || !p.interactive() {
            return Err(err);
        }
        say(format!("  {err}"));
    };

    let ours_on = |port: u16| health(port, Duration::from_millis(500)).is_ok();
    loop {
        if !port_free(port) && !ours_on(port) {
            if opts.port.is_some() {
                return Err(format!("port {port} is in use by another program"));
            }
            let next = next_free_port(port).ok_or("no free port found")?;
            say(format!("  port {port} is in use; proposing {next}"));
            port = next;
        }
        if opts.port.is_some() || !p.interactive() {
            break;
        }
        match p.ask(&format!("Local port [{port}]: ")) {
            Some(a) if !a.is_empty() => match a.parse::<u16>() {
                Ok(n) if n > 0 => {
                    port = n;
                    if !port_free(port) && !ours_on(port) {
                        continue;
                    }
                    break;
                }
                _ => {
                    say("  not a port number");
                    continue;
                }
            },
            _ => break,
        }
    }
    if points_to_shim(&upstream, port) {
        return Err(format!("{upstream} is the shim's own address"));
    }

    // 3) config
    let mut cfg = existing
        .clone()
        .unwrap_or_else(|| Config::new(upstream.clone()));
    cfg.upstream = upstream.clone();
    cfg.listen = format!("127.0.0.1:{port}");
    cfg.validate()?;
    cfg.save(&cfg_path)?;
    say(format!("  wrote {}", cfg_path.display()));

    // 4) service
    let mut service_running = false;
    let service_note = if opts.no_service {
        format!(
            "not installed (--no-service); start it yourself: {} run",
            current_exe()
                .map(|e| e.display().to_string())
                .unwrap_or_else(|_| "as-shim".into())
        )
    } else if service::available() {
        let exe = current_exe()?;
        let unit = service::install(&exe, &cfg_path)?;
        service_running = true;
        say(format!(
            "  installed systemd user service {}",
            unit.display()
        ));
        if service::linger_enabled() == Some(false) {
            say("  hint: the service runs while you are logged in. For cron/headless use run:");
            say("        loginctl enable-linger \"$USER\"");
        }
        format!("{} (enabled, started)", unit.display())
    } else {
        let exe = current_exe()?;
        let note = format!(
            "systemd --user is not available; run it yourself, e.g.: nohup {} run >/tmp/as-shim.log 2>&1 &",
            exe.display()
        );
        say(format!("  {note}"));
        note
    };

    // 5) health
    let healthy = match wait_healthy(port, &upstream, if service_running { 8 } else { 1 }) {
        Ok(h) => {
            say(format!(
                "  health check ok (as-shim {} on 127.0.0.1:{port})",
                h.version
            ));
            true
        }
        Err(e) => {
            if service_running {
                say(format!("  health check FAILED on 127.0.0.1:{port}: {e}"));
                say(format!(
                    "  see: journalctl --user -u {} -n 50",
                    service::unit_file_name()
                ));
            } else {
                say(format!("  shim not running on 127.0.0.1:{port} yet ({e})"));
            }
            false
        }
    };

    // 6) Claude Code
    let shim = config::shim_url(port);
    let mut claude_note = "unchanged (--no-claude)".to_string();
    if !opts.no_claude {
        match settings {
            Err(e) => {
                claude_note = format!("unchanged ({e})");
            }
            Ok(mut s) => {
                let already = s.base_url().as_deref() == Some(shim.as_str());
                let default_yes = healthy || !service_running;
                let go = if already {
                    true
                } else if opts.yes {
                    default_yes
                } else {
                    p.confirm(
                        &format!("Point Claude Code to the shim ({shim})?"),
                        default_yes,
                    )
                };
                if !go {
                    claude_note = format!(
                        "unchanged; to use the shim set env.ANTHROPIC_BASE_URL={shim} in {}",
                        s.path.display()
                    );
                    if opts.yes && !default_yes {
                        claude_note.push_str(" (skipped because the health check failed)");
                    }
                } else {
                    let current = s.base_url();
                    let original = match &current {
                        Some(u)
                            if points_to_shim(u, port)
                                || config::is_local_url(u)
                                    && cfg.original_claude_base_url.is_some() =>
                        {
                            cfg.original_claude_base_url
                                .clone()
                                .or_else(|| Some(upstream.clone()))
                        }
                        Some(u) => Some(u.clone()),
                        None => Some(String::new()),
                    };
                    if !already {
                        s.set_base_url(Some(&shim));
                        let backup = s.save()?;
                        if let Some(b) = backup {
                            say(format!("  backed up Claude settings to {}", b.display()));
                        }
                    }
                    cfg.original_claude_base_url = original;
                    cfg.claude_settings_path = Some(s.path.to_string_lossy().into_owned());
                    cfg.save(&cfg_path)?;
                    claude_note =
                        format!("env.ANTHROPIC_BASE_URL = {shim} in {}", s.path.display());
                    say(format!("  Claude Code now uses {shim}"));
                    if let Some(e) = env_base_url().filter(|e| e != &shim) {
                        say(format!(
                            "  warning: your shell exports ANTHROPIC_BASE_URL={e}; it may override settings.json — unset it or set it to {shim}"
                        ));
                    }
                    let has_timeout = s
                        .root
                        .get("env")
                        .and_then(|e| e.get("API_TIMEOUT_MS"))
                        .is_some()
                        || std::env::var_os("API_TIMEOUT_MS").is_some();
                    if !has_timeout {
                        say("  note: Claude Code gives up on a non-stream request after 300 s (API_TIMEOUT_MS).");
                        say("        For very long generations add \"API_TIMEOUT_MS\": \"900000\" to env in settings.json.");
                    }
                }
            }
        }
    }

    say("");
    say("== as-shim summary ==");
    say(format!("  upstream:     {upstream}"));
    say(format!("  listen:       127.0.0.1:{port}"));
    say(format!("  config:       {}", cfg_path.display()));
    if let Some(p) = cfg.log_db_path() {
        say(format!("  request log:  {} (SQLite)", p.display()));
    }
    say(format!("  service:      {service_note}"));
    say(format!(
        "  health:       {}",
        if healthy { "ok" } else { "not reachable" }
    ));
    say(format!("  Claude Code:  {claude_note}"));
    say("  commands:     as-shim status | as-shim set-upstream <URL> | as-shim uninstall");
    if service_running && !healthy {
        return Err("the service did not become healthy".into());
    }
    Ok(())
}

// ------------------------------------------------------------------------------ status

pub fn status() -> Result<(), String> {
    let cfg_path = config::default_config_path();
    println!("as-shim {}", crate::VERSION);
    println!("config:       {}", cfg_path.display());
    let cfg = match Config::load_optional(&cfg_path)? {
        Some(c) => c,
        None => {
            println!("              (missing — run `as-shim setup`)");
            return Ok(());
        }
    };
    if let Ok(text) = std::fs::read_to_string(&cfg_path) {
        for k in Config::unknown_keys(&text) {
            println!("              warning: unknown key {k:?} is ignored");
        }
    }
    println!("upstream:     {}", cfg.upstream);
    println!("listen:       {}", cfg.listen);
    if cfg.resume {
        println!(
            "resume:       on (idle {}s, budget {}s; used when the server supports it)",
            cfg.resume_idle_secs, cfg.resume_budget_secs
        );
    } else {
        println!("resume:       off");
    }
    match cfg.log_db_path() {
        Some(p) if cfg.log_retention_days == 0 => {
            println!("request log:  {} (kept forever)", p.display())
        }
        Some(p) => println!(
            "request log:  {} (kept {} days)",
            p.display(),
            cfg.log_retention_days
        ),
        None => println!("request log:  off"),
    }
    println!(
        "service:      {}",
        if service::is_installed() {
            service::state()
        } else {
            format!("not installed ({})", service::unit_path().display())
        }
    );
    let port = cfg.port().unwrap_or(DEFAULT_PORT);
    match health(port, Duration::from_secs(2)) {
        Ok(h) if h.upstream == cfg.upstream => println!("health:       ok (version {})", h.version),
        Ok(h) => println!(
            "health:       running but serving upstream {} (restart to apply the config)",
            h.upstream
        ),
        Err(e) => println!("health:       not reachable on 127.0.0.1:{port} ({e})"),
    }
    let sp = cfg
        .claude_settings_path
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(config::claude_settings_path);
    match ClaudeSettings::load(&sp) {
        Ok(s) => {
            let v = s.base_url();
            let via = match &v {
                Some(u) if points_to_shim(u, port) => " (via the shim)",
                Some(_) => " (NOT via the shim)",
                None => " (not set)",
            };
            println!(
                "Claude Code:  {} ANTHROPIC_BASE_URL={}{via}",
                sp.display(),
                v.unwrap_or_default()
            );
        }
        Err(e) => println!("Claude Code:  {e}"),
    }
    if let Some(e) = env_base_url() {
        println!("shell env:    ANTHROPIC_BASE_URL={e} (may override settings.json)");
    }
    Ok(())
}

// ------------------------------------------------------------------------------ set-upstream

pub fn set_upstream(url: &str) -> Result<(), String> {
    let cfg_path = config::default_config_path();
    let mut cfg = Config::load_optional(&cfg_path)?
        .ok_or("no config yet — run `as-shim setup --upstream <URL>` first")?;
    let up = config::normalize_upstream(url)?;
    let port = cfg.port().unwrap_or(DEFAULT_PORT);
    if points_to_shim(&up, port) {
        return Err(format!("{up} is the shim's own address"));
    }
    cfg.upstream = up.clone();
    cfg.save(&cfg_path)?;
    println!("upstream set to {up} in {}", cfg_path.display());
    if service::is_installed() && service::available() {
        service::restart()?;
        match wait_healthy(port, &up, 8) {
            Ok(_) => println!("service restarted, health ok"),
            Err(e) => return Err(format!("service restarted but health check failed: {e}")),
        }
    } else {
        println!("restart `as-shim run` to apply");
    }
    Ok(())
}

// ------------------------------------------------------------------------------ uninstall

pub fn uninstall(yes: bool) -> Result<(), String> {
    let mut p = Prompter::new(yes);
    if !yes
        && !p.confirm(
            "Remove the as-shim service and config, and restore Claude Code settings?",
            true,
        )
    {
        return Err("aborted".into());
    }
    let cfg_path = config::default_config_path();
    let cfg = Config::load_optional(&cfg_path).unwrap_or_else(|e| {
        say(format!("warning: {e}"));
        None
    });
    let port = cfg.as_ref().and_then(|c| c.port()).unwrap_or(DEFAULT_PORT);

    match service::remove() {
        Ok(true) => say(format!("removed service {}", service::unit_file_name())),
        Ok(false) => say("no systemd service installed"),
        Err(e) => say(format!("warning: could not remove the service: {e}")),
    }

    let sp = cfg
        .as_ref()
        .and_then(|c| c.claude_settings_path.clone())
        .map(PathBuf::from)
        .unwrap_or_else(config::claude_settings_path);
    match ClaudeSettings::load(&sp) {
        Ok(mut s) => match s.base_url() {
            Some(u) if points_to_shim(&u, port) => {
                let original = cfg
                    .as_ref()
                    .and_then(|c| c.original_claude_base_url.clone());
                match original.as_deref() {
                    Some("") => s.set_base_url(None),
                    Some(o) => s.set_base_url(Some(o)),
                    None => match cfg.as_ref() {
                        Some(c) => s.set_base_url(Some(&c.upstream)),
                        None => s.set_base_url(None),
                    },
                }
                let backup = s.save()?;
                let now = s.base_url();
                say(format!(
                    "restored Claude Code ANTHROPIC_BASE_URL in {} to {}{}",
                    sp.display(),
                    now.as_deref().unwrap_or("(unset)"),
                    backup
                        .map(|b| format!(" (backup: {})", b.display()))
                        .unwrap_or_default()
                ));
            }
            Some(u) => say(format!(
                "Claude Code ANTHROPIC_BASE_URL is {u} (not the shim); left unchanged"
            )),
            None => say("Claude Code ANTHROPIC_BASE_URL is not set; left unchanged"),
        },
        Err(e) => say(format!("warning: Claude settings not changed: {e}")),
    }

    if cfg_path.exists() {
        std::fs::remove_file(&cfg_path)
            .map_err(|e| format!("cannot remove {}: {e}", cfg_path.display()))?;
        if let Some(dir) = cfg_path.parent() {
            let _ = std::fs::remove_dir(dir);
        }
        say(format!("removed {}", cfg_path.display()));
    }
    if let Ok(exe) = current_exe() {
        say(format!("to remove the binary: rm {}", exe.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_edit_keeps_order_and_other_keys() {
        let dir = std::env::temp_dir().join(format!("as-shim-setup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");
        std::fs::write(
            &path,
            r#"{"model":"opus","env":{"A":"1","ANTHROPIC_BASE_URL":"https://gw.example","B":"2"},"hooks":{}}"#,
        )
        .unwrap();
        let mut s = ClaudeSettings::load(&path).unwrap();
        assert_eq!(s.base_url().as_deref(), Some("https://gw.example"));
        s.set_base_url(Some("http://127.0.0.1:8787"));
        let backup = s.save().unwrap().unwrap();
        assert!(std::fs::read_to_string(&backup)
            .unwrap()
            .contains("gw.example"));
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys, ["model", "env", "hooks"]);
        let env: Vec<_> = v["env"].as_object().unwrap().keys().cloned().collect();
        assert_eq!(env, ["A", "ANTHROPIC_BASE_URL", "B"]);
        assert_eq!(v["env"]["ANTHROPIC_BASE_URL"], "http://127.0.0.1:8787");
        let mut s = ClaudeSettings::load(&path).unwrap();
        s.set_base_url(None);
        s.save().unwrap();
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(v["env"].get("ANTHROPIC_BASE_URL").is_none());
        let fresh = dir.join("sub").join("settings.json");
        let mut s = ClaudeSettings::load(&fresh).unwrap();
        assert!(!s.exists);
        s.set_base_url(Some("http://127.0.0.1:1"));
        assert!(s.save().unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shim_detection() {
        assert!(points_to_shim("http://127.0.0.1:8787", 8787));
        assert!(points_to_shim("http://localhost:8787/", 8787));
        assert!(!points_to_shim("http://127.0.0.1:9000", 8787));
        assert!(!points_to_shim("https://gw.example:8787", 8787));
    }
}
