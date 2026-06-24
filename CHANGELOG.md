# Changelog

All notable versions of the AuraNode agent are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/) and
[SemVer](https://semver.org/).

## [1.7.0] — 2026-06-24

### Fixed — Disk and log metric accuracy

- **Disk usage no longer counts pseudo filesystems.** The agent previously reported every
  mount, including the read-only `squashfs` mounts of snaps (`/snap/...`), which always sit
  at 100%. That made the dashboard and the AI report a "disk full" while the real root
  filesystem was far below capacity. The collector now ignores virtual/pseudo filesystems
  (`squashfs`, `tmpfs`, `devtmpfs`, `overlay`, …) and system paths (`/snap`, `/proc`,
  `/sys`, `/dev`, `/run`), reporting only real storage.
- **The agent no longer collects its own logs.** Its systemd units (`auranode-agent`,
  `auranode-agent-helper`) emit self-referential noise (e.g. WebSocket reconnections) that
  polluted server diagnostics. They are now always excluded from journal collection.

To apply on an existing install, re-run the installer (`curl … | sudo bash`) or restart the
service after updating the binary.

## [1.6.0] — 2026-06-24

### Added — System log collection (read-only)

- **The agent now collects system logs and streams them to the panel.** It follows the
  systemd journal (`journalctl -f -o json`), maps each entry to a service (unit) and a
  level (from syslog priority), batches them and sends them to the backend, where they
  appear under **Logs** in the dashboard (filterable by service and level). The backend
  can narrow collection to specific units via the `log_services` config; empty means all.
- **Read-only journal access, no privilege escalation.** The installer adds the
  unprivileged `auranode` user to the **`systemd-journal`** group and sets
  `SupplementaryGroups=systemd-journal` on the service. This grants **read** access to
  the journal only — the agent is still not root, keeps `NoNewPrivileges`,
  `ProtectSystem=strict` and an empty `CapabilityBoundingSet`. This is the standard
  approach for monitoring agents.
- **Graceful when unavailable.** On systems without `journalctl` (non-systemd) or
  without journal access, the collector simply produces nothing — it never fails the
  agent. To enable log collection on an existing install, re-run the installer
  (`curl … | sudo bash`) so the group membership and updated unit are applied.

## [1.5.1] — 2026-06-24

### Added
- **Automation `webhook` action implemented.** When an automation rule with a
  `webhook` action fires, the agent now sends a `POST` (JSON, 10s timeout) to the
  configured URL with the trigger context: `rule_id`, `metric`, `operator`,
  `threshold`, `value` and `fired_at` (RFC3339). The result is reported back as the
  rule's exit status: `0` on a `2xx` response, the HTTP status code otherwise (e.g.
  `404`, `500`), or `1` on a transport error — so the panel's "Executions" tab shows
  a meaningful outcome. (Previously this action was a no-op.)

## [1.5.0] — 2026-06-24

### Added — Bounded privileged mode (opt-in, OFF by default)

This release adds an **optional** way to run a small, fixed set of administrative
tasks (package updates, service restarts) from the panel. It is designed to be the
opposite of "give the panel root": every part is opt-in, bounded, validated, and
audited. If you do nothing, **nothing changes** — the agent keeps running exactly as
before.

**The agent itself does not gain any privileges.** It still runs as the unprivileged
`auranode` user with `NoNewPrivileges`, `ProtectSystem=strict` and an empty
`CapabilityBoundingSet`. Privileged tasks are handled by a **separate** helper:

- **Separate root helper, separate unit.** Privileged actions run in
  `auranode-agent-helper.service` (a distinct systemd unit running as root), not in
  the agent. The agent talks to it over a local Unix socket and only acts as a bridge.
  The helper is installed **only** if the server operator runs
  `curl … | sudo bash -s -- --enable-privileged` on the machine itself — it is never
  installed automatically.
- **Two independent opt-ins.** The helper must be installed on the box (local consent)
  **and** the account **owner** must enable privileged mode for that server in the
  panel (with an explicit confirmation). Neither step alone does anything.
- **Not arbitrary sudo — a fixed whitelist.** The only actions that exist are:
  `apt`/`dnf` update, upgrade, install, autoremove; and `systemctl`
  status/start/stop/reload/restart. There is no "run this command" path.
- **No shell, no injection.** Commands are executed with an explicit `argv`
  (no `bash -c`), so shell metacharacters are inert. Arguments are validated with
  strict allowlists (package names, unit names).
- **Guards against foot-guns.** The helper refuses to manage the agent itself
  (`auranode-agent*`) and refuses to **stop** critical units (`ssh`, `dbus`,
  networking, `systemd-journald`, `systemd-logind`, …) that would lock you out.
- **Defense in depth.** The helper re-validates every request against the same
  whitelist server-side, so a compromised agent still cannot run anything off-list.
- **Locked-down socket.** The socket lives at `/run/auranode/helper.sock`, owned by
  root and group-restricted to the `auranode` user (mode `0660`); one action runs at a
  time; each action has a timeout and bounded output.
- **Fully audited.** Enabling/disabling the mode and every action (with its arguments
  and exit code) are written to the audit log.
- **`auranode-agent version` subcommand** advertises `privileged-capable`, so the
  installer refuses to enable the helper on an incompatible binary.

**Backward compatible.** Agents without the helper report it as unavailable and the
panel keeps the feature hidden/disabled; the new `sys_action` protocol messages are
ignored by older agents. To revert at any time: `… | sudo bash -s -- --disable-privileged`.

## [1.4.0] — 2026-06-23

### Added
- **Web terminal (PTY):** the agent can now open an interactive shell (a PTY running
  `bash` as the agent's own unprivileged user) on request from the backend, streaming
  stdin/stdout and honoring terminal resizes. This powers the browser-based web terminal
  in the dashboard. One session per agent; the shell inherits the agent's hardened
  context (no privilege escalation). Fully backward compatible: older agents ignore the
  new `pty_*` protocol messages and simply do not offer a terminal.

## [1.3.0] — 2026-06-22

### Added
- **Delta continuous sync (migrations Type C):** when the backend requests a sync,
  the destination agent scans the files already present under the target path and
  reports them in its manifest, so the source only transfers new or changed files
  (compared by size + mtime). Fully backward compatible: older agents ignore the flag
  and perform a full transfer.

## [1.2.1] — 2026-06-20

### Fixed
- **Tunnel half-close deadlock:** when one direction of a stream closed (e.g. the
  request finished while the response was still flowing), the stream was removed from
  the map and the credits (`tunnel_window`) of the still-active direction were lost →
  the reader ran out of credit and the connection hung. Closing now only affects its
  own direction (true half-close) and the stream is removed when both directions end.

## [1.2.0] — 2026-06-20

### Added / Improved
- **Credit-based flow control on tunnels** (port forwarding): each direction of each
  stream has an in-flight byte window; the receiver grants credit (`tunnel_window`) as
  it drains and the sender stops reading its local TCP when it runs out, applying real
  backpressure to the origin. Previously a sustained slow consumer saturated the buffer
  and reset the stream; now it throttles without losing bytes.
- Capability negotiation: flow control is only enabled if both ends support it
  (backward compatible; falls back to the previous mode).

## [1.1.0] — 2026-06-20

### Added
- **Update check (check-and-notify):** the agent polls GitHub Releases every 6h and,
  if a newer version exists, logs it and notifies the backend (the panel shows "update
  available"). The agent does **not** self-replace, to preserve the service hardening.
- **Multi-arch Docker images** on GHCR (`ghcr.io/koyere/auranode-agent`),
  `linux/amd64` and `linux/arm64`, published automatically on every release.

## [1.0.0] — 2026-06-20

First public release of the agent.

### Added
- Metric collection: CPU, RAM/swap, disk, network (delta/s), load avg and top-10
  processes (via gopsutil).
- WebSocket connection to the backend with exponential reconnection (backoff 2s → 5min).
- Heartbeat and metrics with intervals configurable from the backend.
- Persistent on-disk offline buffer (bbolt), drained on reconnect.
- Remote command execution (`exec`) with timeout and bounded output.
- Local rules engine: condition + duration, cooldown and daily maximum.
- Port forwarding / tunnels (Type 1 local-CLI, Type 2 remote and dest=CLI).
- VPS-to-VPS migrations (Type B: directory, relay mode) with resume.
- Installer with SHA256 verification, hardened systemd service and Dockerfile.
