use as_shim::config::{self, Config};
use as_shim::{log_error, log_info, log_warn, proxy, setup};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "as-shim",
    version,
    about = "as = anthropic to stream: local proxy that keeps Claude Code requests streaming so Cloudflare does not 524 them"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Interactive install: upstream, port, config, systemd user service, Claude Code settings
    Setup {
        /// Upstream Anthropic-compatible base URL (skips the question)
        #[arg(long)]
        upstream: Option<String>,
        /// Local port (default 8787)
        #[arg(long)]
        port: Option<u16>,
        /// Do not ask; accept defaults
        #[arg(long, short = 'y')]
        yes: bool,
        /// Do not install the systemd user service
        #[arg(long)]
        no_service: bool,
        /// Do not touch Claude Code settings.json
        #[arg(long)]
        no_claude: bool,
    },
    /// Run the proxy in the foreground (used by the systemd service)
    Run {
        /// Override the upstream from the config
        #[arg(long)]
        upstream: Option<String>,
        /// Override the listen address (loopback only), e.g. 127.0.0.1:8787
        #[arg(long)]
        listen: Option<String>,
        /// Config file (default: $XDG_CONFIG_HOME/as-shim/config.toml)
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show config, service state and health
    Status,
    /// Change the upstream and restart the service
    SetUpstream {
        /// New upstream base URL
        url: String,
    },
    /// Remove the service and config, restore Claude Code settings
    Uninstall {
        /// Do not ask for confirmation
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.cmd {
        Cmd::Setup {
            upstream,
            port,
            yes,
            no_service,
            no_claude,
        } => setup::setup(setup::SetupOpts {
            upstream,
            port,
            yes,
            no_service,
            no_claude,
        }),
        Cmd::Run {
            upstream,
            listen,
            config,
        } => run(upstream, listen, config),
        Cmd::Status => setup::status(),
        Cmd::SetUpstream { url } => setup::set_upstream(&url),
        Cmd::Uninstall { yes } => setup::uninstall(yes),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("as-shim: {e}");
            ExitCode::FAILURE
        }
    }
}

fn resume_summary(cfg: &Config) -> String {
    if cfg.resume {
        let cap = match cfg.resume_max_silence_secs() {
            Some(s) => format!("gives up {s}s after the last byte"),
            None => format!(
                "idle detection off; gives up {}s after the last byte",
                cfg.resume_budget_secs
            ),
        };
        format!(
            "on (idle {}s, budget {}s: {cap})",
            cfg.resume_idle_secs, cfg.resume_budget_secs
        )
    } else {
        "off".into()
    }
}

fn log_summary(cfg: &Config) -> String {
    match cfg.log_db_path() {
        Some(p) if cfg.log_retention_days == 0 => format!("{} (kept forever)", p.display()),
        Some(p) => format!("{} (kept {} days)", p.display(), cfg.log_retention_days),
        None => "off".into(),
    }
}

fn run(
    upstream: Option<String>,
    listen: Option<String>,
    config_path: Option<PathBuf>,
) -> Result<(), String> {
    let path = config_path.unwrap_or_else(config::default_config_path);
    let mut cfg = match Config::load_optional(&path)? {
        Some(c) => {
            if let Ok(text) = std::fs::read_to_string(&path) {
                for k in Config::unknown_keys(&text) {
                    eprintln!(
                        "as-shim: warning: unknown config key {k:?} in {} is ignored",
                        path.display()
                    );
                }
            }
            c
        }
        None => Config::new(upstream.clone().ok_or_else(|| {
            format!(
                "no config at {} and no --upstream; run `as-shim setup` first",
                path.display()
            )
        })?),
    };
    if let Some(u) = upstream {
        cfg.upstream = u;
    }
    if let Some(l) = listen {
        cfg.listen = l;
    }
    cfg.upstream = config::normalize_upstream(&cfg.upstream)?;
    cfg.validate()?;
    if let Some(level) = as_shim::log::parse_level(&cfg.log_level) {
        as_shim::log::set_level(level);
    }
    let addr = config::parse_listen(&cfg.listen)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start runtime: {e}"))?;
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| format!("cannot listen on {addr}: {e}"))?;
        log_info!(
            "as-shim {} listening on http://{addr} -> {}",
            as_shim::VERSION,
            cfg.upstream
        );
        log_info!(
            "resume: {}; request log: {}",
            resume_summary(&cfg),
            log_summary(&cfg)
        );
        if let Some(w) = cfg.resume_watchdog_warning() {
            log_warn!("{w}");
        }
        let shutdown = async {
            let mut term =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(e) => {
                        log_warn!("cannot install SIGTERM handler: {e}");
                        let _ = tokio::signal::ctrl_c().await;
                        return;
                    }
                };
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            log_info!("shutting down (waiting up to 30s for open requests)");
        };
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn({
            let cfg = cfg.clone();
            async move {
                proxy::serve(listener, &cfg, async {
                    let _ = rx.await;
                })
                .await
            }
        });
        tokio::pin!(server);
        tokio::select! {
            r = &mut server => {
                return match r {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(format!("server error: {e}")),
                    Err(e) => Err(format!("server task failed: {e}")),
                };
            }
            _ = shutdown => {}
        }
        let _ = tx.send(());
        match tokio::time::timeout(std::time::Duration::from_secs(30), server).await {
            Ok(_) => {}
            Err(_) => log_error!("open requests did not finish within 30s; exiting"),
        }
        Ok(())
    })
}
