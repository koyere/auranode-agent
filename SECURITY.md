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

## Bounded privileged mode (optional, since v1.5.0)

Privileged tasks (package updates, service restarts) are **never** run by the agent
itself. They are handled by a **separate** root helper that is **off by default** and
requires two independent opt-ins: the operator installing it on the box
(`--enable-privileged`) **and** the account owner enabling it in the panel.

- **Separate unit, not the agent.** `auranode-agent-helper.service` runs as root; the
  unprivileged agent only forwards requests to it over a local Unix socket.
- **Fixed whitelist, no shell.** Only `apt`/`dnf` (update/upgrade/install/autoremove)
  and `systemctl` (status/start/stop/reload/restart) actions exist. They are executed
  with an explicit `argv` (no `bash -c`); arguments are validated against strict
  allowlists. There is no arbitrary-command path — this is **not** unrestricted sudo.
- **Guards.** The helper refuses to manage the agent itself or to stop critical units
  (`ssh`, `dbus`, networking, journald, logind). It re-validates every request
  server-side (defense in depth), runs one action at a time with a timeout, and
  exposes its socket only to the local `auranode` group (root-owned, `0660`).
- **Auditable & reversible.** Every action is recorded in the audit log; the helper can
  be removed at any time with `--disable-privileged`.

## Manual binary verification

```bash
VERSION=v1.5.0
ARCH=amd64
BASE=https://github.com/koyere/auranode-agent/releases/download/$VERSION
curl -fsSLO $BASE/auranode-agent_${VERSION#v}_linux_${ARCH}.tar.gz
curl -fsSLO $BASE/checksums.txt
sha256sum -c --ignore-missing checksums.txt
```
