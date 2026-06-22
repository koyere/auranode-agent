# Security Policy — AuraNode Agent

Agent security is a top priority: it runs on our users' servers. This repository is
**public and auditable** precisely so that anyone can review exactly what the agent runs.

## Reporting a vulnerability

- Email: **security@auranode.app**
- Acknowledgement: **48 hours**
- Initial assessment: **7 days**

Please include reproduction steps and the estimated impact. Do not open a public issue
for vulnerabilities.

### Safe harbor

We will not take legal action against good-faith researchers who follow this policy and
avoid data destruction or service disruption. Valid reports are publicly acknowledged
(Hall of Fame) if the reporter wishes.

## Scope

**In scope:**
- The agent code in this repository
- The `install.sh` installation script
- The systemd service template and the `Dockerfile`

**Out of scope:**
- Users' VPS (their own configuration)
- The AuraNode backend / panel (report those to security@auranode.app as well)

## Agent security guarantees

- **Unprivileged:** runs as the `auranode` system user (not root), with
  `NoNewPrivileges`, `ProtectSystem=strict`, an empty `CapabilityBoundingSet=` and
  CPU/memory limits in systemd.
- **Protected token:** the token lives in `/etc/auranode/agent.env` with `600`
  permissions, never in process arguments (`ps aux`) or logs.
- **Encrypted communication:** TLS with certificate verification against the backend.
  There is no `InsecureSkipVerify` mode.
- **Binary integrity:** each release includes `checksums.txt` (SHA256) and the installer
  verifies the hash before installing.
- **Audited remote execution:** commands come from a human action confirmed in the panel
  and are recorded in the audit log; the blast radius is bounded by running unprivileged.

## Manual binary verification

```bash
VERSION=v1.3.0
ARCH=amd64
BASE=https://github.com/koyere/auranode-agent/releases/download/$VERSION
curl -fsSLO $BASE/auranode-agent_${VERSION#v}_linux_${ARCH}.tar.gz
curl -fsSLO $BASE/checksums.txt
sha256sum -c --ignore-missing checksums.txt
```
