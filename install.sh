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

# write_helper_unit instala el unit del helper root. El helper corre como root y SIN
# el endurecimiento del agente (necesita escribir el sistema: apt, systemctl), pero
# solo ejecuta acciones de una whitelist con argumentos validados (no es sudo libre).
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

# El socket vive en /run/auranode (lo crea systemd, propiedad root).
RuntimeDirectory=auranode
RuntimeDirectoryMode=0755

# El helper SÍ necesita escalar para apt/systemctl (de ahí este unit separado).
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
  [[ -x "${INSTALL_DIR}/${BINARY_NAME}" ]] || error "El agente AuraNode no está instalado. Instálalo primero antes de habilitar el modo privilegiado."
  id -u "$SERVICE_USER" &>/dev/null || error "No existe el usuario ${SERVICE_USER}. ¿Está instalado el agente?"

  # Guarda: el binario debe soportar el modo privilegiado (v1.5.0+). Un binario
  # antiguo ignoraría el subcomando y arrancaría un agente normal como root.
  if ! timeout 5 "${INSTALL_DIR}/${BINARY_NAME}" version 2>/dev/null | grep -q "privileged-capable"; then
    error "Tu versión del agente no soporta el modo privilegiado. Actualízalo primero:
  curl -fsSL https://get.auranode.app/agent | AURANODE_TOKEN=ant_xxx sudo -E bash"
  fi

  info "Habilitando el modo privilegiado acotado (helper root)..."
  write_helper_unit
  systemctl daemon-reload
  systemctl enable --now "$HELPER_SERVICE"
  # Reiniciar el agente para que detecte el socket y reporte 'disponible' al panel.
  systemctl restart "$SERVICE_NAME" 2>/dev/null || true

  echo ""
  info "═══════════════════════════════════════════════"
  info "✓ Modo privilegiado DISPONIBLE en este servidor"
  info "═══════════════════════════════════════════════"
  warn "Esto NO da root libre al panel. El helper SOLO ejecuta acciones de una"
  warn "whitelist con argumentos validados (sin shell):"
  echo "   • Actualizar índices de paquetes        (apt update)"
  echo "   • Actualizar paquetes                    (apt upgrade)"
  echo "   • Instalar paquete(s)                    (apt install <pkg>)"
  echo "   • Limpiar paquetes huérfanos             (apt autoremove)"
  echo "   • Estado/arrancar/parar/recargar/reiniciar servicios (systemctl)"
  echo ""
  warn "Guardas: no se puede gestionar el propio agente ni detener servicios"
  warn "críticos (ssh, dbus, red, journald...). Toda acción queda auditada."
  echo ""
  info "Último paso: en el panel, el OWNER debe ACTIVAR el modo privilegiado para"
  info "este servidor (con confirmación). Para revertir: re-ejecuta con --disable-privileged."
}

disable_privileged() {
  info "Deshabilitando el modo privilegiado..."
  systemctl disable --now "$HELPER_SERVICE" 2>/dev/null || true
  rm -f "/etc/systemd/system/${HELPER_SERVICE}.service"
  systemctl daemon-reload
  systemctl restart "$SERVICE_NAME" 2>/dev/null || true
  info "✓ Modo privilegiado deshabilitado. El helper root fue eliminado."
}

# ─── Pre-flight checks ─────────────────────────────────────────────────────────
[[ "${EUID}" -ne 0 ]] && error "This script must be run as root (use sudo)."

for cmd in curl tar sha256sum systemctl; do
  command -v "$cmd" >/dev/null 2>&1 || error "Required command not found: $cmd"
done

# Modos de gestión del helper privilegiado (no requieren token ni reinstalar).
case "$MODE" in
  enable-privileged)  enable_privileged;  exit 0 ;;
  disable-privileged) disable_privileged; exit 0 ;;
esac

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
echo ""
info "Optional — bounded privileged mode (apt/systemctl from the panel, NOT full root):"
info "  curl -fsSL https://get.auranode.app/agent | sudo bash -s -- --enable-privileged"
