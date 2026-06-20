#!/usr/bin/env bash
# AuraNode Agent — Instalador oficial
# Repositorio: https://github.com/koyere/auranode-agent
# Documentación: https://docs.auranode.app/agent/install
#
# Uso:
#   curl -fsSL https://get.auranode.app/agent | AURANODE_TOKEN=ant_xxx sudo -E bash
#
# El binario se verifica con SHA256 contra checksums.txt del release en GitHub.

set -euo pipefail

# ─── Configuración ─────────────────────────────────────────────────────────────
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

# ─── Verificaciones previas ────────────────────────────────────────────────────
[[ "${EUID}" -ne 0 ]] && error "Este script debe ejecutarse como root (usa sudo)."

for cmd in curl tar sha256sum systemctl; do
  command -v "$cmd" >/dev/null 2>&1 || error "Comando requerido no encontrado: $cmd"
done

TOKEN="${AURANODE_TOKEN:-}"
[[ -z "$TOKEN" ]] && error "AURANODE_TOKEN no está definido. Obtén tu token en https://panel.auranode.app"
if ! echo "$TOKEN" | grep -qE '^ant_[A-Za-z0-9_-]{32,}$'; then
  error "Formato de token inválido. Verifica el token en tu panel de AuraNode."
fi

# ─── Detección de arquitectura ─────────────────────────────────────────────────
case "$(uname -m)" in
  x86_64)        ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  armv7l)        ARCH="armv7" ;;
  *)             error "Arquitectura no soportada: $(uname -m)" ;;
esac

# ─── Resolver versión ──────────────────────────────────────────────────────────
if [[ "$AGENT_VERSION" == "latest" ]]; then
  info "Consultando última versión disponible..."
  AGENT_VERSION=$(curl -fsSL "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" \
    | grep '"tag_name"' | sed -E 's/.*"(v[^"]+)".*/\1/')
  [[ -z "$AGENT_VERSION" ]] && error "No se pudo determinar la última versión. ¿Existe ya un release publicado?"
fi
VERSION_NO_V="${AGENT_VERSION#v}"
info "Versión a instalar: ${AGENT_VERSION} (linux/${ARCH})"

# ─── Descarga y verificación ───────────────────────────────────────────────────
BASE_URL="https://github.com/${GITHUB_REPO}/releases/download/${AGENT_VERSION}"
TARBALL="${PROJECT}_${VERSION_NO_V}_linux_${ARCH}.tar.gz"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

info "Descargando ${TARBALL}..."
curl -fsSL "${BASE_URL}/${TARBALL}"       -o "${TMP_DIR}/${TARBALL}" \
  || error "No se pudo descargar el binario: ${BASE_URL}/${TARBALL}"
curl -fsSL "${BASE_URL}/checksums.txt"    -o "${TMP_DIR}/checksums.txt" \
  || error "No se pudo descargar checksums.txt"

info "Verificando integridad (SHA256)..."
EXPECTED=$(grep " ${TARBALL}\$" "${TMP_DIR}/checksums.txt" | awk '{print $1}')
ACTUAL=$(sha256sum "${TMP_DIR}/${TARBALL}" | awk '{print $1}')
[[ -z "$EXPECTED" ]] && error "No se encontró el checksum de ${TARBALL} en checksums.txt"
[[ "$EXPECTED" != "$ACTUAL" ]] && error "Verificación SHA256 FALLIDA. El archivo puede estar corrupto o comprometido."
info "✓ SHA256 verificado"

tar -xzf "${TMP_DIR}/${TARBALL}" -C "${TMP_DIR}"
[[ -f "${TMP_DIR}/${BINARY_NAME}" ]] || error "El binario no se encontró dentro del archivo."

# ─── Usuario del sistema y directorios ─────────────────────────────────────────
if ! id -u "$SERVICE_USER" &>/dev/null; then
  info "Creando usuario del sistema: ${SERVICE_USER}..."
  useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi
mkdir -p "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR"
chown "${SERVICE_USER}:${SERVICE_USER}" "$DATA_DIR" "$LOG_DIR"
chmod 750 "$CONFIG_DIR"

# ─── Instalar binario ──────────────────────────────────────────────────────────
install -m 755 -o root -g root "${TMP_DIR}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
info "Binario instalado en ${INSTALL_DIR}/${BINARY_NAME}"

# ─── Archivo de entorno (token protegido) ──────────────────────────────────────
cat > "$ENV_FILE" <<EOF
AURANODE_TOKEN=${TOKEN}
AURANODE_BACKEND_URL=${BACKEND_URL}
AURANODE_DB_PATH=${DATA_DIR}/buffer.db
EOF
chmod 600 "$ENV_FILE"
chown "root:${SERVICE_USER}" "$ENV_FILE"

# ─── Servicio systemd ──────────────────────────────────────────────────────────
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

# Recursos
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
info "✓ Instalación completada — ${AGENT_VERSION}"
info "  Estado: systemctl status ${SERVICE_NAME}"
info "  Logs:   journalctl -u ${SERVICE_NAME} -f"
info "═══════════════════════════════════════════════"
info "El servidor debería aparecer en tu panel en unos segundos."
