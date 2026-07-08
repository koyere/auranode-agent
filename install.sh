#!/usr/bin/env bash
# AuraNode Agent — official installer
# Repository:    https://github.com/koyere/auranode-agent
# Documentation: https://docs.auranode.app/agent/install
#
# Usage:
#   curl -fsSL https://get.auranode.app/agent | AURANODE_TOKEN=ant_xxx sudo -E bash
#
# The binary is verified with SHA256 against the release's checksums.txt on GitHub.

set -euo pipefail

# ─── Configuration ─────────────────────────────────────────────────────────────
GITHUB_REPO="koyere/auranode-agent"
PROJECT="auranode-agent"
INSTALL_DIR="/usr/local/bin"
BINARY_NAME="auranode-agent"
SERVICE_NAME="auranode-agent"
SERVICE_USER="auranode"
CONFIG_DIR="/etc/auranode"
DATA_DIR="/var/lib/auranode"
LOG_DIR="/var/log/auranode"
ENV_FILE="${CONFIG_DIR}/agent.env"
AGENT_VERSION="${AURANODE_AGENT_VERSION:-latest}"
BACKEND_URL="${AURANODE_BACKEND_URL:-wss://api.auranode.app/ws/agent}"
HELPER_SERVICE="auranode-agent-helper"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[auranode]${NC} $*"; }
warn()  { echo -e "${YELLOW}[auranode]${NC} $*"; }
error() { echo -e "${RED}[auranode] ERROR:${NC} $*" >&2; exit 1; }

# ─── Mode parsing ──────────────────────────────────────────────────────────────
# Default: full install. --enable-privileged / --disable-privileged manage the
# optional, opt-in privileged helper (bounded whitelist mode, NOT full root).
MODE="install"
for arg in "$@"; do
  case "$arg" in
    --enable-privileged)  MODE="enable-privileged" ;;
    --disable-privileged) MODE="disable-privileged" ;;
  esac
done

# write_helper_unit installs the root helper unit. The helper runs as root and WITHOUT
# the agent's hardening (it needs to write to the system: apt, systemctl), but it only
# runs actions from a whitelist with validated arguments (it is NOT unrestricted sudo).
write_helper_unit() {
  cat > "/etc/systemd/system/${HELPER_SERVICE}.service" <<EOF
[Unit]
Description=AuraNode Agent — Privileged Helper (bounded whitelist)
Documentation=https://docs.auranode.app/agent/privileged
After=${SERVICE_NAME}.service
PartOf=${SERVICE_NAME}.service

[Service]
Type=simple
User=root
Group=root
ExecStart=${INSTALL_DIR}/${BINARY_NAME} privileged-helper
Restart=on-failure
RestartSec=10s
TimeoutStopSec=15s

# The socket lives in /run/auranode (created by systemd, owned by root).
RuntimeDirectory=auranode
RuntimeDirectoryMode=0755

# The helper DOES need to escalate for apt/systemctl (hence this separate unit).
NoNewPrivileges=no

MemoryMax=512M

StandardOutput=journal
StandardError=journal
SyslogIdentifier=${HELPER_SERVICE}

[Install]
WantedBy=multi-user.target
EOF
}

enable_privileged() {
  [[ -x "${INSTALL_DIR}/${BINARY_NAME}" ]] || error "The AuraNode agent is not installed. Install it first before enabling privileged mode."
  id -u "$SERVICE_USER" &>/dev/null || error "User ${SERVICE_USER} does not exist. Is the agent installed?"

  # Guard: the binary must support privileged mode (v1.5.0+). An old binary would
  # ignore the subcommand and start a normal agent as root.
  if ! timeout 5 "${INSTALL_DIR}/${BINARY_NAME}" version 2>/dev/null | grep -q "privileged-capable"; then
    error "Your agent version does not support privileged mode. Update it first:
  curl -fsSL https://get.auranode.app/agent | AURANODE_TOKEN=ant_xxx sudo -E bash"
  fi

  info "Enabling bounded privileged mode (root helper)..."
  write_helper_unit
  systemctl daemon-reload
  systemctl enable --now "$HELPER_SERVICE"
  # Restart the agent so it detects the socket and reports 'available' to the panel.
  systemctl restart "$SERVICE_NAME" 2>/dev/null || true

  echo ""
  info "═══════════════════════════════════════════════"
  info "✓ Privileged mode AVAILABLE on this server"
  info "═══════════════════════════════════════════════"
  warn "This does NOT grant the panel full root. The helper ONLY runs actions from a"
  warn "whitelist with validated arguments (no shell):"
  echo "   • Refresh package indexes                (apt update)"
  echo "   • Upgrade packages                       (apt upgrade)"
  echo "   • Install package(s)                     (apt install <pkg>)"
  echo "   • Remove orphaned packages               (apt autoremove)"
  echo "   • Status/start/stop/reload/restart services (systemctl)"
  echo ""
  warn "Guards: it cannot manage the agent itself or stop critical services"
  warn "(ssh, dbus, network, journald...). Every action is audited."
  echo ""
  info "Last step: in the panel, the OWNER must ENABLE privileged mode for this"
  info "server (with confirmation). To revert: re-run with --disable-privileged."
}

disable_privileged() {
  info "Disabling privileged mode..."
  systemctl disable --now "$HELPER_SERVICE" 2>/dev/null || true
  rm -f "/etc/systemd/system/${HELPER_SERVICE}.service"
  systemctl daemon-reload
  systemctl restart "$SERVICE_NAME" 2>/dev/null || true
  info "✓ Privileged mode disabled. The root helper was removed."
}

# ─── Pre-flight checks ─────────────────────────────────────────────────────────
[[ "${EUID}" -ne 0 ]] && error "This script must be run as root (use sudo)."

for cmd in curl tar sha256sum systemctl; do
  command -v "$cmd" >/dev/null 2>&1 || error "Required command not found: $cmd"
done

# Privileged helper management modes (no token or reinstall required).
case "$MODE" in
  enable-privileged)  enable_privileged;  exit 0 ;;
  disable-privileged) disable_privileged; exit 0 ;;
esac

TOKEN="${AURANODE_TOKEN:-}"
# Update mode: if no token was provided but the agent is already installed, reuse the
# token already stored on this machine. This lets users update later (months after the
# install) by just re-running the installer, without needing the token again.
if [[ -z "$TOKEN" && -f "$ENV_FILE" ]]; then
  TOKEN="$(grep -oP '^AURANODE_TOKEN=\K.*' "$ENV_FILE" 2>/dev/null || true)"
  [[ -n "$TOKEN" ]] && info "Reusing the token already stored on this server (update mode)."
fi
[[ -z "$TOKEN" ]] && error "AURANODE_TOKEN is not set. Get your token at https://panel.auranode.app"
if ! echo "$TOKEN" | grep -qE '^ant_[A-Za-z0-9_-]{32,}$'; then
  error "Invalid token format. Check the token in your AuraNode panel."
fi

# ─── Architecture detection ────────────────────────────────────────────────────
case "$(uname -m)" in
  x86_64)        ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  armv7l)        ARCH="armv7" ;;
  *)             error "Unsupported architecture: $(uname -m)" ;;
esac

# ─── Resolve version ───────────────────────────────────────────────────────────
if [[ "$AGENT_VERSION" == "latest" ]]; then
  info "Looking up the latest available version..."
  AGENT_VERSION=$(curl -fsSL "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" \
    | grep '"tag_name"' | sed -E 's/.*"(v[^"]+)".*/\1/')
  [[ -z "$AGENT_VERSION" ]] && error "Could not determine the latest version. Is there a published release yet?"
fi
VERSION_NO_V="${AGENT_VERSION#v}"
info "Version to install: ${AGENT_VERSION} (linux/${ARCH})"

# ─── Download and verify ───────────────────────────────────────────────────────
BASE_URL="https://github.com/${GITHUB_REPO}/releases/download/${AGENT_VERSION}"
TARBALL="${PROJECT}_${VERSION_NO_V}_linux_${ARCH}.tar.gz"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

info "Downloading ${TARBALL}..."
curl -fsSL "${BASE_URL}/${TARBALL}"       -o "${TMP_DIR}/${TARBALL}" \
  || error "Could not download the binary: ${BASE_URL}/${TARBALL}"
curl -fsSL "${BASE_URL}/checksums.txt"    -o "${TMP_DIR}/checksums.txt" \
  || error "Could not download checksums.txt"

info "Verifying integrity (SHA256)..."
EXPECTED=$(grep " ${TARBALL}\$" "${TMP_DIR}/checksums.txt" | awk '{print $1}')
ACTUAL=$(sha256sum "${TMP_DIR}/${TARBALL}" | awk '{print $1}')
[[ -z "$EXPECTED" ]] && error "Could not find the checksum for ${TARBALL} in checksums.txt"
[[ "$EXPECTED" != "$ACTUAL" ]] && error "SHA256 verification FAILED. The file may be corrupt or tampered with."
info "✓ SHA256 verified"

tar -xzf "${TMP_DIR}/${TARBALL}" -C "${TMP_DIR}"
[[ -f "${TMP_DIR}/${BINARY_NAME}" ]] || error "The binary was not found inside the archive."

# ─── System user and directories ───────────────────────────────────────────────
if ! id -u "$SERVICE_USER" &>/dev/null; then
  info "Creating system user: ${SERVICE_USER}..."
  useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi
# Read-only access to the systemd journal so the agent can collect system logs
# (standard for monitoring agents). This grants journal READ only, never root.
if getent group systemd-journal >/dev/null 2>&1; then
  usermod -aG systemd-journal "$SERVICE_USER" 2>/dev/null || true
fi
mkdir -p "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR"
chown "${SERVICE_USER}:${SERVICE_USER}" "$DATA_DIR" "$LOG_DIR"
chmod 750 "$CONFIG_DIR"

# ─── Install binary ────────────────────────────────────────────────────────────
install -m 755 -o root -g root "${TMP_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
info "Binary installed at ${INSTALL_DIR}/${BINARY_NAME}"

# ─── Environment file (protected token) ────────────────────────────────────────
cat > "$ENV_FILE" <<EOF
AURANODE_TOKEN=${TOKEN}
AURANODE_BACKEND_URL=${BACKEND_URL}
AURANODE_DB_PATH=${DATA_DIR}/buffer.db
EOF
chmod 600 "$ENV_FILE"
chown "root:${SERVICE_USER}" "$ENV_FILE"

# ─── systemd service ───────────────────────────────────────────────────────────
cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=AuraNode Agent
Documentation=https://docs.auranode.app/agent
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
EnvironmentFile=${ENV_FILE}
ExecStart=${INSTALL_DIR}/${BINARY_NAME}
Restart=on-failure
RestartSec=10s
TimeoutStopSec=30s

# Read-only journal access for log collection (no privilege escalation).
SupplementaryGroups=systemd-journal

# Hardening
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=${DATA_DIR} ${LOG_DIR}
ProtectHome=yes
CapabilityBoundingSet=
AmbientCapabilities=

# Resources
MemoryMax=256M
CPUQuota=20%

StandardOutput=journal
StandardError=journal
SyslogIdentifier=auranode-agent

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
# restart (not `enable --now`): on an update, `--now` only starts the service if it is
# stopped and does NOT reload the new binary in an already-active service. restart does.
systemctl restart "$SERVICE_NAME"
# If the privileged helper is installed, it also runs the updated binary:
# restart it so it picks up the new version.
if systemctl list-unit-files "${HELPER_SERVICE}.service" >/dev/null 2>&1 \
   && systemctl is-enabled "$HELPER_SERVICE" >/dev/null 2>&1; then
  systemctl restart "$HELPER_SERVICE" 2>/dev/null || true
fi

echo ""
info "═══════════════════════════════════════════════"
info "✓ Installation complete — ${AGENT_VERSION}"
info "  Status: systemctl status ${SERVICE_NAME}"
info "  Logs:   journalctl -u ${SERVICE_NAME} -f"
info "═══════════════════════════════════════════════"
info "The server should appear in your panel within a few seconds."
echo ""
info "Optional — bounded privileged mode (apt/systemctl from the panel, NOT full root):"
info "  curl -fsSL https://get.auranode.app/agent | sudo bash -s -- --enable-privileged"
