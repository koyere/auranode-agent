<div align="center">

# AuraNode Agent

Lightweight Go agent that connects your VPS to [AuraNode](https://auranode.app) —
the intelligent multi-VPS control platform.

[![release](https://img.shields.io/github/v/release/koyere/auranode-agent)](https://github.com/koyere/auranode-agent/releases)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

</div>

> **This repository is public on purpose.** Sysadmins rightly distrust closed-source
> monitoring software. Here you can audit exactly what the agent runs on your servers
> before installing it.

## Installation

```bash
curl -fsSL https://get.auranode.app/agent | AURANODE_TOKEN=ant_xxx sudo -E bash
```

Get your token by registering a server at [panel.auranode.app](https://panel.auranode.app).
The installer downloads the binary from the latest release, **verifies its SHA256**
against `checksums.txt`, and installs the agent as an unprivileged systemd service.

### Manual installation (without `| bash`)

```bash
VERSION=$(curl -fsSL https://api.github.com/repos/koyere/auranode-agent/releases/latest \
  | grep tag_name | cut -d '"' -f4)
ARCH=amd64   # or arm64 / armv7
BASE=https://github.com/koyere/auranode-agent/releases/download/$VERSION
curl -fsSLO $BASE/auranode-agent_${VERSION#v}_linux_${ARCH}.tar.gz
curl -fsSLO $BASE/checksums.txt
sha256sum -c --ignore-missing checksums.txt
tar -xzf auranode-agent_${VERSION#v}_linux_${ARCH}.tar.gz
sudo install -m755 auranode-agent /usr/local/bin/
```

Then set `AURANODE_TOKEN` in `/etc/auranode/agent.env` and create the systemd service
(see [`install.sh`](install.sh) for reference).

### Docker

```bash
docker run -d --name auranode-agent --restart unless-stopped \
  --pid host --network host \
  -e AURANODE_TOKEN=ant_xxx \
  -v auranode-data:/var/lib/auranode \
  ghcr.io/koyere/auranode-agent:latest
```

Multi-arch images (`linux/amd64`, `linux/arm64`) are published on every release.

## Configuration

The agent is configured via environment variables (in `/etc/auranode/agent.env`):

| Variable | Default | Description |
|---|---|---|
| `AURANODE_TOKEN` | — (required) | Agent token (`ant_…`) |
| `AURANODE_BACKEND_URL` | `wss://api.auranode.app/ws/agent` | Backend WebSocket endpoint |
| `AURANODE_DB_PATH` | `/var/lib/auranode/buffer.db` | Offline buffer (bbolt) |
| `AURANODE_METRICS_INTERVAL` | `60` | Seconds between metrics |
| `AURANODE_HEARTBEAT_INTERVAL` | `30` | Seconds between heartbeats |

## What the agent does

- Collects metrics (CPU, RAM, disk, network, load, processes) and sends them to the backend.
- Keeps a WebSocket connection with automatic reconnection and an offline buffer.
- Runs commands that **you** confirm in the panel (everything is recorded in the audit log).
- Evaluates automation rules locally (offline-capable).
- Supports tunnels/port-forwarding and VPS-to-VPS migrations.

## Security

Runs unprivileged, with the token in a `600` file, verified TLS communication, and
SHA256-checksummed binaries. See [SECURITY.md](SECURITY.md) and the hardening block in
[`install.sh`](install.sh).

## Development

```bash
go build ./cmd/auranode-agent
go test ./...
```

Releases are published automatically with [GoReleaser](https://goreleaser.com) when a
`v*` tag is pushed (see [`.github/workflows/release.yml`](.github/workflows/release.yml)).

## License

[MIT](LICENSE) © Koyere Dev
