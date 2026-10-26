//! `as-shim update`: replace the running binary with the latest GitHub release.
//!
//! Mirrors install.sh: downloads `as-shim-<target>.tar.gz` plus `SHA256SUMS` from the
//! release, verifies the checksum, extracts, atomically replaces the current binary and
//! restarts the systemd user service when one is installed.

use crate::config::{Config, DEFAULT_PORT};
use std::path::{Path, PathBuf};
use std::process::Command;

const REPO: &str = "123hi123/as";

pub fn update() -> Result<(), String> {
    let target = match std::env::consts::ARCH {
        "x86_64" => "x86_64-unknown-linux-musl",
        "aarch64" => "aarch64-unknown-linux-musl",
        other => return Err(format!("unsupported architecture: {other}")),
    };
    let exe = current_exe()?;
    let base = std::env::var("AS_SHIM_DOWNLOAD_BASE")
        .ok()
        .map(|s| s.trim_end_matches('/').to_string())
        .unwrap_or_else(|| format!("https://github.com/{REPO}/releases/latest/download"));
    let asset = format!("as-shim-{target}.tar.gz");

    eprintln!("==> downloading {asset}");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start runtime: {e}"))?;
    let asset_url = format!("{base}/{asset}");
    let sums_url = format!("{base}/SHA256SUMS");
    let (asset_bytes, sums) = rt
        .block_on(async { tokio::try_join!(fetch(&asset_url), fetch(&sums_url)) })
        .map_err(|e| format!("download failed: {e}"))?;

    eprintln!("==> verifying checksum");
    let tmp = std::env::temp_dir().join(format!("as-shim-update-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).map_err(|e| format!("cannot create {}: {e}", tmp.display()))?;
    let asset_path = tmp.join(&asset);
    std::fs::write(&asset_path, &asset_bytes)
        .map_err(|e| format!("cannot write {}: {e}", asset_path.display()))?;
    let sums_path = tmp.join("SHA256SUMS");
    std::fs::write(&sums_path, &sums).map_err(|e| format!("cannot write SHA256SUMS: {e}"))?;

    let want = String::from_utf8_lossy(&sums)
        .lines()
        .find(|l| {
            let mut it = l.split_whitespace();
            let hash = it.next().unwrap_or("");
            let name = it.next().unwrap_or("");
            hash.len() == 64 && (name == asset || name == format!("*{asset}"))
        })
        .and_then(|l| l.split_whitespace().next())
        .ok_or_else(|| format!("SHA256SUMS has no entry for {asset}"))?
        .to_string();
    let got = local_sha256(&asset_path)?;
    if want != got {
        return Err(format!(
            "checksum mismatch for {asset} (expected {want}, got {got})"
        ));
    }

    eprintln!("==> extracting");
    let out = Command::new("tar")
        .args(["-xzf"])
        .arg(&asset_path)
        .arg("-C")
        .arg(&tmp)
        .arg("as-shim")
        .output()
        .map_err(|e| format!("cannot run tar: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "tar failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let new_bin = tmp.join("as-shim");
    if !new_bin.is_file() {
        return Err("archive does not contain as-shim".into());
    }

    let target_path = tmp.join("as-shim.installed");
    std::fs::copy(&new_bin, &target_path).map_err(|e| format!("cannot stage new binary: {e}"))?;
    make_executable(&target_path)?;
    replace_running(&target_path, &exe)?;
    let _ = std::fs::remove_dir_all(&tmp);

    eprintln!("==> installed {}", exe.display());

    let cfg = Config::load_optional(&crate::config::default_config_path()).unwrap_or(None);
    let port = cfg.as_ref().and_then(|c| c.port()).unwrap_or(DEFAULT_PORT);
    if crate::service::is_installed() && crate::service::available() {
        crate::service::restart()?;
        let upstream = cfg.as_ref().map(|c| c.upstream.clone()).unwrap_or_default();
        match super::setup::wait_healthy(port, &upstream, 8) {
            Ok(_) => eprintln!("==> service restarted, health ok"),
            Err(e) => return Err(format!("service restarted but health check failed: {e}")),
        }
    } else {
        eprintln!("==> no systemd service managed by as-shim; restart `as-shim run` yourself if it is running");
    }
    Ok(())
}

async fn fetch(url: &str) -> Result<Vec<u8>, String> {
    let resp = reqwest::get(url)
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| format!("download failed: {url}: {e}"))?;
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("download failed: {url}: {e}"))?;
    Ok(bytes.to_vec())
}

fn current_exe() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|e| format!("cannot locate own binary: {e}"))?;
    Ok(std::fs::canonicalize(&exe).unwrap_or(exe))
}

/// sha256 via the same tools install.sh relies on.
fn local_sha256(path: &Path) -> Result<String, String> {
    if let Ok(out) = Command::new("sha256sum").arg(path).output() {
        if out.status.success() {
            let line = String::from_utf8_lossy(&out.stdout);
            return Ok(line.split_whitespace().next().unwrap_or("").to_string());
        }
    }
    let out = Command::new("shasum")
        .args(["-a", "256"])
        .arg(path)
        .output()
        .map_err(|e| format!("need sha256sum or shasum: {e}"))?;
    if !out.status.success() {
        return Err("sha256sum and shasum both failed".into());
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string())
}

/// Replace a possibly-running binary. Plain rename can hit ETXTBSY on some setups,
/// so unlink the target first (a running process keeps its inode alive).
fn replace_running(new: &Path, target: &Path) -> Result<(), String> {
    let _ = std::fs::remove_file(target);
    match std::fs::rename(new, target) {
        Ok(()) => Ok(()),
        // Cross-device tmp dirs (e.g. TMPDIR on another mount): copy over. The target
        // was unlinked first, so a plain copy cannot hit ETXTBSY on a running binary.
        Err(_) => std::fs::copy(new, target)
            .map(|_| ())
            .map_err(|e| format!("cannot replace {}: {e}", target.display())),
    }
}

fn make_executable(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let mut perm = std::fs::metadata(path)
        .map_err(|e| e.to_string())?
        .permissions();
    perm.set_mode(0o755);
    std::fs::set_permissions(path, perm).map_err(|e| e.to_string())
}
