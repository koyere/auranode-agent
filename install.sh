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

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[auranode]${NC} $*"; }
warn()  { echo -e "${YELLOW}[auranode]${NC} $*"; }
error() { echo -e "${RED}[auranode] ERROR:${NC} $*" >&2; exit 1; }

# ─── Pre-flight checks ─────────────────────────────────────────────────────────
[[ "${EUID}" -ne 0 ]] && error "This script must be run as root (use sudo)."

for cmd in curl tar sha256sum systemctl; do
  command -v "$cmd" >/dev/null 2>&1 || error "Required command not found: $cmd"
done

TOKEN="${AURANODE_TOKEN:-}"
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
systemctl enable --now "$SERVICE_NAME"

echo ""
info "═══════════════════════════════════════════════"
info "✓ Installation complete — ${AGENT_VERSION}"
info "  Status: systemctl status ${SERVICE_NAME}"
info "  Logs:   journalctl -u ${SERVICE_NAME} -f"
info "═══════════════════════════════════════════════"
info "The server should appear in your panel within a few seconds."
