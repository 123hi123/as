#!/usr/bin/env bash
# as-shim installer — "as" = anthropic to stream.
#   curl -fsSL https://raw.githubusercontent.com/123hi123/as/main/install.sh | bash
#   bash install.sh --uninstall
# Env: AS_SHIM_VERSION (tag, default latest), AS_SHIM_BIN_DIR (default ~/.local/bin),
#      AS_SHIM_DOWNLOAD_BASE (override release URL), AS_SHIM_UPSTREAM, AS_SHIM_PORT,
#      AS_SHIM_NONINTERACTIVE=1 (no questions)
set -euo pipefail

REPO="123hi123/as"
BIN_DIR="${AS_SHIM_BIN_DIR:-$HOME/.local/bin}"
BIN="$BIN_DIR/as-shim"

info() { printf '\033[1;32m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33mwarning:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

have_tty() { [ -r /dev/tty ] && [ -w /dev/tty ] && { : </dev/tty; } 2>/dev/null; }

run_with_tty() {
  if [ "${AS_SHIM_NONINTERACTIVE:-}" != "1" ] && have_tty; then
    "$@" </dev/tty
  else
    "$@" </dev/null
  fi
}

uninstall() {
  if [ -x "$BIN" ]; then
    info "running as-shim uninstall"
    "$BIN" uninstall --yes </dev/null || warn "as-shim uninstall reported an error"
    rm -f "$BIN"
    info "removed $BIN"
  elif command -v as-shim >/dev/null 2>&1; then
    local found
    found="$(command -v as-shim)"
    "$found" uninstall --yes </dev/null || warn "as-shim uninstall reported an error"
    rm -f "$found" || die "cannot remove $found"
    info "removed $found"
  else
    warn "as-shim not found in $BIN_DIR or PATH; nothing to do"
  fi
}

case "${1:-}" in
  --uninstall|uninstall) uninstall; exit 0 ;;
  ""|--install|install) ;;
  -h|--help)
    sed -n '2,7p' "$0" 2>/dev/null || true
    exit 0 ;;
  *) die "unknown argument: $1 (use --uninstall)" ;;
esac

[ "$(uname -s)" = "Linux" ] || die "as-shim supports Linux only (got $(uname -s))"
case "$(uname -m)" in
  x86_64|amd64) TARGET="x86_64-unknown-linux-musl" ;;
  aarch64|arm64) TARGET="aarch64-unknown-linux-musl" ;;
  *) die "unsupported architecture: $(uname -m) (x86_64 and aarch64 only)" ;;
esac

if [ -n "${AS_SHIM_DOWNLOAD_BASE:-}" ]; then
  BASE="${AS_SHIM_DOWNLOAD_BASE%/}"
elif [ -n "${AS_SHIM_VERSION:-}" ]; then
  BASE="https://github.com/$REPO/releases/download/$AS_SHIM_VERSION"
else
  BASE="https://github.com/$REPO/releases/latest/download"
fi

if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL --retry 3 --proto '=http,https' -o "$2" "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -q -O "$2" "$1"; }
else
  die "need curl or wget"
fi
if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  die "need sha256sum or shasum"
fi
command -v tar >/dev/null 2>&1 || die "need tar"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/as-shim-install.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

ASSET="as-shim-$TARGET.tar.gz"
info "downloading $ASSET"
fetch "$BASE/$ASSET" "$TMP/$ASSET" || die "download failed: $BASE/$ASSET"
fetch "$BASE/SHA256SUMS" "$TMP/SHA256SUMS" || die "download failed: $BASE/SHA256SUMS"

want="$(awk -v f="$ASSET" '{n=$2; sub(/^\*/, "", n)} n==f {print $1; exit}' "$TMP/SHA256SUMS")"
[ -n "$want" ] || die "SHA256SUMS has no entry for $ASSET"
got="$(sha256 "$TMP/$ASSET")"
[ "$want" = "$got" ] || die "checksum mismatch for $ASSET (expected $want, got $got)"
info "checksum ok"

mkdir -p "$TMP/x"
tar -xzf "$TMP/$ASSET" -C "$TMP/x" as-shim || die "archive does not contain as-shim"
[ -f "$TMP/x/as-shim" ] || die "archive does not contain as-shim"

mkdir -p "$BIN_DIR"
install -m 0755 "$TMP/x/as-shim" "$BIN.new"
mv -f "$BIN.new" "$BIN"
info "installed $("$BIN" --version) to $BIN"

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) warn "$BIN_DIR is not in PATH; add: export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

args=(setup)
[ -n "${AS_SHIM_UPSTREAM:-}" ] && args+=(--upstream "$AS_SHIM_UPSTREAM")
[ -n "${AS_SHIM_PORT:-}" ] && args+=(--port "$AS_SHIM_PORT")
[ "${AS_SHIM_NONINTERACTIVE:-}" = "1" ] && args+=(--yes)

info "running: as-shim ${args[*]}"
run_with_tty "$BIN" "${args[@]}"
