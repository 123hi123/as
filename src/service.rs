//! systemd --user unit management.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub fn service_name() -> String {
    std::env::var("AS_SHIM_SERVICE_NAME")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '@'))
        })
        .unwrap_or_else(|| "as-shim".to_string())
}

pub fn unit_file_name() -> String {
    format!("{}.service", service_name())
}

/// Directory the systemd user manager reads units from. Taken from the manager's own
/// environment, which can differ from this process's HOME / XDG_CONFIG_HOME.
fn unit_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("AS_SHIM_UNIT_DIR").filter(|d| !d.is_empty()) {
        return PathBuf::from(d);
    }
    let env = Command::new("systemctl")
        .args(["--user", "show-environment"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        .unwrap_or_default();
    let get = |k: &str| {
        env.lines()
            .find_map(|l| l.strip_prefix(k).and_then(|v| v.strip_prefix('=')))
            .filter(|v| v.starts_with('/'))
            .map(PathBuf::from)
    };
    get("XDG_CONFIG_HOME")
        .or_else(|| get("HOME").map(|h| h.join(".config")))
        .unwrap_or_else(crate::config::xdg_config_home)
        .join("systemd")
        .join("user")
}

pub fn unit_path() -> PathBuf {
    unit_dir().join(unit_file_name())
}

fn systemctl(args: &[&str]) -> Result<String, String> {
    let out = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("cannot run systemctl: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        Err(if err.is_empty() {
            format!("systemctl --user {} failed ({})", args.join(" "), out.status)
        } else {
            err
        })
    }
}

/// True when a systemd user manager is reachable.
pub fn available() -> bool {
    if std::env::var_os("AS_SHIM_NO_SYSTEMD").is_some() {
        return false;
    }
    match Command::new("systemctl")
        .args(["--user", "show-environment"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) => s.success(),
        Err(_) => false,
    }
}

fn systemd_quote(s: &str) -> String {
    let needs = s.is_empty() || s.chars().any(|c| c.is_whitespace() || matches!(c, '"' | '\'' | '\\' | '%' | '$' | ';'));
    if !needs {
        return s.to_string();
    }
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '%' => out.push_str("%%"),
            '$' => out.push_str("$$"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

pub fn unit_text(exe: &Path, config: &Path) -> String {
    let exe = systemd_quote(&exe.to_string_lossy());
    let cfg = systemd_quote(&config.to_string_lossy());
    format!(
        "[Unit]\n\
         Description=as-shim (anthropic to stream) local proxy for Claude Code\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exe} run --config {cfg}\n\
         Restart=on-failure\n\
         RestartSec=2\n\
         NoNewPrivileges=yes\n\
         LockPersonality=yes\n\
         RestrictRealtime=yes\n\
         RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

pub fn is_installed() -> bool {
    unit_path().exists()
}

/// Write the unit, reload, enable and (re)start it.
pub fn install(exe: &Path, config: &Path) -> Result<PathBuf, String> {
    let path = unit_path();
    let text = unit_text(exe, config);
    crate::util::write_atomic(&path, text.as_bytes(), Some(0o644))
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    systemctl(&["daemon-reload"])?;
    let name = unit_file_name();
    systemctl(&["enable", &name])?;
    systemctl(&["restart", &name])?;
    Ok(path)
}

pub fn restart() -> Result<(), String> {
    systemctl(&["restart", &unit_file_name()]).map(|_| ())
}

/// Stop, disable and delete the unit. Returns whether a unit file existed.
pub fn remove() -> Result<bool, String> {
    let path = unit_path();
    let existed = path.exists();
    if available() {
        let name = unit_file_name();
        let _ = systemctl(&["disable", "--now", &name]);
        if existed {
            std::fs::remove_file(&path)
                .map_err(|e| format!("cannot remove {}: {e}", path.display()))?;
        }
        let _ = systemctl(&["daemon-reload"]);
        let _ = systemctl(&["reset-failed", &name]);
    } else if existed {
        std::fs::remove_file(&path).map_err(|e| format!("cannot remove {}: {e}", path.display()))?;
    }
    Ok(existed)
}

/// `active` / `inactive` / `failed` / ... or a description of why it is unknown.
pub fn state() -> String {
    if !available() {
        return "systemd --user not available".into();
    }
    let name = unit_file_name();
    let active = Command::new("systemctl")
        .args(["--user", "is-active", &name])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let enabled = Command::new("systemctl")
        .args(["--user", "is-enabled", &name])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    format!(
        "{name}: {}, {}",
        if active.is_empty() { "unknown" } else { &active },
        if enabled.is_empty() { "not-found" } else { &enabled }
    )
}

/// Whether lingering is enabled for the current user (services run without a login session).
pub fn linger_enabled() -> Option<bool> {
    let user = std::env::var("USER").ok().filter(|u| !u.is_empty())?;
    let out = Command::new("loginctl")
        .args(["show-user", &user, "--property=Linger", "--value"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    match String::from_utf8_lossy(&out.stdout).trim() {
        "yes" => Some(true),
        "no" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting() {
        assert_eq!(systemd_quote("/usr/bin/as-shim"), "/usr/bin/as-shim");
        assert_eq!(systemd_quote("/home/a b/x"), "\"/home/a b/x\"");
        assert_eq!(systemd_quote("/h/100%"), "\"/h/100%%\"");
        let u = unit_text(Path::new("/home/user/.local/bin/as-shim"), Path::new("/c/config.toml"));
        assert!(u.contains("ExecStart=/home/user/.local/bin/as-shim run --config /c/config.toml\n"));
        assert!(u.contains("Restart=on-failure"));
    }
}
