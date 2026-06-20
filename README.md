<div align="center">

# AuraNode Agent

Agente ligero en Go que conecta tu VPS con [AuraNode](https://auranode.app) —
la plataforma de control inteligente multi-VPS.

[![release](https://img.shields.io/github/v/release/koyere/auranode-agent)](https://github.com/koyere/auranode-agent/releases)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

</div>

> **Este repositorio es público a propósito.** Los sysadmins desconfían —con razón— del
> software de monitoreo de código cerrado. Aquí puedes auditar exactamente qué ejecuta
> el agente en tus servidores antes de instalarlo.

## Instalación

```bash
curl -fsSL https://get.auranode.app/agent | AURANODE_TOKEN=ant_xxx sudo -E bash
```

Obtén tu token registrando un servidor en [panel.auranode.app](https://panel.auranode.app).
El instalador descarga el binario del último release, **verifica su SHA256** contra
`checksums.txt`, e instala el agente como servicio systemd sin privilegios.

### Instalación manual (sin `| bash`)

```bash
VERSION=$(curl -fsSL https://api.github.com/repos/koyere/auranode-agent/releases/latest \
  | grep tag_name | cut -d '"' -f4)
ARCH=amd64   # o arm64 / armv7
BASE=https://github.com/koyere/auranode-agent/releases/download/$VERSION
curl -fsSLO $BASE/auranode-agent_${VERSION#v}_linux_${ARCH}.tar.gz
curl -fsSLO $BASE/checksums.txt
sha256sum -c --ignore-missing checksums.txt
tar -xzf auranode-agent_${VERSION#v}_linux_${ARCH}.tar.gz
sudo install -m755 auranode-agent /usr/local/bin/
```

Luego define `AURANODE_TOKEN` en `/etc/auranode/agent.env` y crea el servicio systemd
(ver [`install.sh`](install.sh) como referencia).

### Docker

```bash
docker run -d --name auranode-agent --restart unless-stopped \
  --pid host --network host \
  -e AURANODE_TOKEN=ant_xxx \
  -v auranode-data:/var/lib/auranode \
  ghcr.io/koyere/auranode-agent:latest
```

Imágenes multi-arch (`linux/amd64`, `linux/arm64`) publicadas en cada release.

## Configuración

El agente se configura por variables de entorno (en `/etc/auranode/agent.env`):

| Variable | Default | Descripción |
|---|---|---|
| `AURANODE_TOKEN` | — (requerido) | Token del agente (`ant_…`) |
| `AURANODE_BACKEND_URL` | `wss://api.auranode.app/ws/agent` | Endpoint WebSocket del backend |
| `AURANODE_DB_PATH` | `/var/lib/auranode/buffer.db` | Buffer offline (bbolt) |
| `AURANODE_METRICS_INTERVAL` | `60` | Segundos entre métricas |
| `AURANODE_HEARTBEAT_INTERVAL` | `30` | Segundos entre heartbeats |

## Qué hace el agente

- Recolecta métricas (CPU, RAM, disco, red, load, procesos) y las envía al backend.
- Mantiene una conexión WebSocket con reconexión automática y buffer offline.
- Ejecuta comandos que **tú** confirmas en el panel (queda todo en el audit log).
- Evalúa reglas de automatización localmente (offline-capable).
- Soporta túneles/port-forwarding y migraciones entre VPS.

## Seguridad

Corre sin privilegios, con el token en un archivo `600`, comunicación TLS verificada y
binarios con checksum SHA256. Ver [SECURITY.md](SECURITY.md) y el bloque de hardening
en [`install.sh`](install.sh).

## Desarrollo

```bash
go build ./cmd/auranode-agent
go test ./...
```

Los releases se publican automáticamente con [GoReleaser](https://goreleaser.com) al
empujar un tag `v*` (ver [`.github/workflows/release.yml`](.github/workflows/release.yml)).

## Licencia

[MIT](LICENSE) © Koyere Dev
