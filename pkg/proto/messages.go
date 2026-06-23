// Package proto defines the AuraNode agent↔backend protocol message types.
// The types must stay in sync with backend/internal/websocket/messages.go.
package proto

// ─── Message types ────────────────────────────────────────────────────────────

const (
	// Agent → Backend
	TypeAgentInfo       = "agent_info"
	TypeHeartbeat       = "heartbeat"
	TypeMetrics         = "metrics"
	TypeMetricsBatch    = "metrics_batch"
	TypeLogStream       = "log_stream"
	TypeExecAck         = "exec_ack"
	TypeExecRunning     = "exec_running"
	TypeExecResult      = "exec_result"
	TypeRuleFired       = "rule_fired"
	TypeAlert           = "alert"
	TypeFSResponse      = "fs_response"
	TypeUpdateAvailable = "update_available"
	TypeError           = "error"

	// Backend → Agent
	TypeConfig    = "config"
	TypeExec      = "exec"
	TypeRuleSync  = "rule_sync"
	TypeFSRequest = "fs_request"
	TypeAgentPing = "ping"

	// ── Port forwarding (tunnels) ──────────────────────────────────────────────
	TypeTunnelStart   = "tunnel_start"
	TypeTunnelStop    = "tunnel_stop"
	TypeTunnelOpen    = "tunnel_open"
	TypeTunnelOpenAck = "tunnel_open_ack"
	TypeTunnelData    = "tunnel_data"
	TypeTunnelClose   = "tunnel_close"
	TypeTunnelStatus  = "tunnel_status"
	TypeTunnelWindow  = "tunnel_window" // credit-based flow control (receiver→sender)
	// Terminal web (PTY interactivo)
	TypePTYStart  = "pty_start"  // backend→agente
	TypePTYData   = "pty_data"   // ambos sentidos (stdin backend→agente, stdout agente→backend)
	TypePTYResize = "pty_resize" // backend→agente
	TypePTYClose  = "pty_close"  // ambos sentidos
)

// ─── File-manager operations (SFTP) ──────────────────────────────────────────
const (
	FSOpList   = "list"
	FSOpStat   = "stat"
	FSOpRead   = "read"
	FSOpWrite  = "write"
	FSOpMkdir  = "mkdir"
	FSOpRename = "rename"
	FSOpDelete = "delete"
	FSOpChmod  = "chmod"
	FSOpChown  = "chown"
)

// ─── Envelope base ────────────────────────────────────────────────────────────

type Envelope struct {
	Type      string `json:"type"`
	AgentID   string `json:"agent_id,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// ─── Agent → Backend ──────────────────────────────────────────────────────────

type AgentInfo struct {
	Envelope
	Version    string `json:"version"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	Hostname   string `json:"hostname"`
	CPUCores   int    `json:"cpu_cores"`
	TotalRAMMB int64  `json:"total_ram_mb"`
	Kernel     string `json:"kernel"`
}

type Heartbeat struct {
	Envelope
}

// UpdateAvailable is sent by the agent when it detects a newer version on
// GitHub Releases. The agent does NOT self-replace (check-and-notify model): the
// backend records it so the panel can show that an update is available.
type UpdateAvailable struct {
	Envelope
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version"`
}

type CPUMetrics struct {
	UsagePercent float64 `json:"usage_percent"`
	Cores        int     `json:"cores"`
}

type RAMMetrics struct {
	TotalMB     int64 `json:"total_mb"`
	UsedMB      int64 `json:"used_mb"`
	FreeMB      int64 `json:"free_mb"`
	SwapTotalMB int64 `json:"swap_total_mb"`
	SwapUsedMB  int64 `json:"swap_used_mb"`
}

type DiskMetric struct {
	Mount       string  `json:"mount"`
	TotalGB     float64 `json:"total_gb"`
	UsedGB      float64 `json:"used_gb"`
	UsedPercent float64 `json:"used_percent"`
}

type NetworkMetric struct {
	Interface string `json:"interface"`
	RxBytes   int64  `json:"rx_bytes"`
	TxBytes   int64  `json:"tx_bytes"`
}

type LoadAvg struct {
	M1  float64 `json:"1m"`
	M5  float64 `json:"5m"`
	M15 float64 `json:"15m"`
}

type ProcessInfo struct {
	PID        int     `json:"pid"`
	Name       string  `json:"name"`
	CPUPercent float64 `json:"cpu_percent"`
	RAMMB      int64   `json:"ram_mb"`
}

type Metrics struct {
	Envelope
	CPU          CPUMetrics      `json:"cpu"`
	RAM          RAMMetrics      `json:"ram"`
	Disk         []DiskMetric    `json:"disk"`
	Network      []NetworkMetric `json:"network"`
	LoadAvg      LoadAvg         `json:"load_avg"`
	TopProcesses []ProcessInfo   `json:"top_processes"`
}

type MetricsBatch struct {
	Envelope
	Count   int       `json:"count"`
	Entries []Metrics `json:"entries"`
}

type LogLine struct {
	TS      int64  `json:"ts"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type LogStream struct {
	Envelope
	Service string    `json:"service"`
	Lines   []LogLine `json:"lines"`
}

type ExecAck struct {
	Envelope
	CommandID string `json:"command_id"`
}

type ExecRunning struct {
	Envelope
	CommandID      string `json:"command_id"`
	PID            int    `json:"pid"`
	ElapsedSeconds int    `json:"elapsed_seconds"`
	OutputPreview  string `json:"output_preview"`
}

type ExecResult struct {
	Envelope
	CommandID  string `json:"command_id"`
	ExitStatus int    `json:"exit_status"`
	Output     string `json:"output"`
	Stderr     string `json:"stderr"`
	DurationMS int64  `json:"duration_ms"`
	Async      bool   `json:"async"`
}

type RuleFired struct {
	Envelope
	RuleID       string  `json:"rule_id"`
	TriggerValue float64 `json:"trigger_value"`
	ActionTaken  string  `json:"action_taken"`
	ExitStatus   int     `json:"exit_status"`
}

type AgentAlert struct {
	Envelope
	AlertType    string  `json:"alert_type"`
	Metric       string  `json:"metric"`
	CurrentValue float64 `json:"current_value"`
	Threshold    float64 `json:"threshold"`
	Severity     string  `json:"severity"`
}

type AgentError struct {
	Envelope
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ─── File manager (SFTP) ─────────────────────────────────────────────────────

type FSEntry struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	IsDir      bool   `json:"is_dir"`
	IsSymlink  bool   `json:"is_symlink,omitempty"`
	LinkTarget string `json:"link_target,omitempty"`
	Size       int64  `json:"size"`
	Mode       string `json:"mode"`
	ModeOctal  string `json:"mode_octal"`
	Owner      string `json:"owner"`
	Group      string `json:"group"`
	ModTime    int64  `json:"mod_time"`
}

// FSResponse: Agent → Backend.
type FSResponse struct {
	Envelope
	RequestID string    `json:"request_id"`
	OK        bool      `json:"ok"`
	Error     string    `json:"error,omitempty"`
	Entries   []FSEntry `json:"entries,omitempty"`
	Stat      *FSEntry  `json:"stat,omitempty"`
	Content   string    `json:"content,omitempty"`
	Truncated bool      `json:"truncated,omitempty"`
}

// FSRequest: Backend → Agent.
type FSRequest struct {
	Envelope
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
	Path      string `json:"path"`
	NewPath   string `json:"new_path,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Owner     string `json:"owner,omitempty"`
	Group     string `json:"group,omitempty"`
	Content   string `json:"content,omitempty"`
	MaxBytes  int64  `json:"max_bytes,omitempty"`
}

// ─── Port forwarding (tunnels) ───────────────────────────────────────────────

type TunnelStart struct {
	Envelope
	TunnelID  string `json:"tunnel_id"`
	LocalPort int    `json:"local_port"`
	BindAddr  string `json:"bind_addr,omitempty"` // listener interface; "" = 127.0.0.1
}

type TunnelStop struct {
	Envelope
	TunnelID string `json:"tunnel_id"`
}

type TunnelOpen struct {
	Envelope
	TunnelID string `json:"tunnel_id"`
	StreamID string `json:"stream_id"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	// FC indicates the source supports credit-based flow control. Gating is only
	// enabled if BOTH ends announce it (compatible with older versions).
	FC bool `json:"fc,omitempty"`
}

type TunnelOpenAck struct {
	Envelope
	TunnelID string `json:"tunnel_id"`
	StreamID string `json:"stream_id"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	FC       bool   `json:"fc,omitempty"` // the dest supports credit-based flow control
}

type TunnelData struct {
	Envelope
	TunnelID string `json:"tunnel_id"`
	StreamID string `json:"stream_id"`
	Data     string `json:"data"`
}

type TunnelClose struct {
	Envelope
	TunnelID string `json:"tunnel_id"`
	StreamID string `json:"stream_id"`
	Error    string `json:"error,omitempty"`
}

// TunnelWindow is the credit-based flow control: the receiver, after writing
// `bytes` to its local connection, grants that credit to the sender on the opposite
// end so it can send the same amount. It keeps in-flight bytes per stream and
// direction bounded by the window, applying real (TCP) backpressure to the origin.
type TunnelWindow struct {
	Envelope
	TunnelID string `json:"tunnel_id"`
	StreamID string `json:"stream_id"`
	Bytes    int    `json:"bytes"`
}

type TunnelStatus struct {
	Envelope
	TunnelID string `json:"tunnel_id"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
}

// ─── Backend → Agent ──────────────────────────────────────────────────────────

type RuleDefinition struct {
	RuleID                   string  `json:"rule_id"`
	Enabled                  bool    `json:"enabled"`
	TriggerMetric            string  `json:"trigger_metric"`
	TriggerOp                string  `json:"trigger_op"`
	TriggerValue             float64 `json:"trigger_value"`
	ConditionDurationSeconds int     `json:"condition_duration_seconds"`
	ActionType               string  `json:"action_type"`
	ActionCommand            string  `json:"action_command,omitempty"`
	ActionWebhookURL         string  `json:"action_webhook_url,omitempty"`
	CooldownSeconds          int     `json:"cooldown_seconds"`
	MaxPerDay                int     `json:"max_executions_per_day,omitempty"`
}

type AgentConfig struct {
	Envelope
	MetricsIntervalSeconds   int              `json:"metrics_interval_seconds"`
	HeartbeatIntervalSeconds int              `json:"heartbeat_interval_seconds"`
	LogBufferSize            int              `json:"log_buffer_size"`
	LogServices              []string         `json:"log_services"`
	RebootLoopDetected       bool             `json:"reboot_loop_detected"`
	Rules                    []RuleDefinition `json:"rules"`
}

type ExecCommand struct {
	Envelope
	CommandID             string `json:"command_id"`
	Command               string `json:"command"`
	Async                 bool   `json:"async"`
	DisplayTimeoutSeconds int    `json:"display_timeout_seconds"`
	HardTimeoutSeconds    int    `json:"hard_timeout_seconds"`
	DiscordUserID         string `json:"discord_user_id,omitempty"`
}

type RuleSync struct {
	Envelope
	Rules []RuleDefinition `json:"rules"`
}

// ─── VPS-to-VPS migrations (Type B: directory, relay mode) ───────────────────
//
// Control plane (agent ↔ backend) and data plane (source ↔ dest, which the
// backend forwards opaquely by migration_id). All messages share the umbrella
// MigrationMsg struct (optional fields depending on the type); it is a relay
// protocol with many message types and one struct per type would add a lot of noise.
const (
	// Control (Backend → agente)
	TypeMigrationEstimateReq = "migration_estimate_req" // backend→source: estimate size
	TypeMigrationPrepare     = "migration_prepare"      // backend→dest: get ready to receive
	TypeMigrationStart       = "migration_start"        // backend→source: start transferring
	TypeMigrationCancel      = "migration_cancel"       // backend→both: abort

	// Control (agent → backend)
	TypeMigrationEstimateRes   = "migration_estimate_res"   // source→backend: estimated size
	TypeMigrationReceiverReady = "migration_receiver_ready" // dest→backend: ready + free space + manifest
	TypeMigrationProgress      = "migration_progress"       // source→backend: progress (every ~5s)
	TypeMigrationDone          = "migration_done"           // source→backend: transfer finished
	TypeMigrationFailed        = "migration_failed"         // source/dest→backend: error

	// Data (source ↔ dest, RELAYED by the backend)
	TypeMigrationFile      = "migration_file"       // source→dest: file header
	TypeMigrationChunk     = "migration_chunk"      // source→dest: chunk (base64 + crc32)
	TypeMigrationFileDone  = "migration_file_done"  // source→dest: end of file
	TypeMigrationFileAck   = "migration_file_ack"   // dest→source: file verification
	TypeMigrationWindowAck = "migration_window_ack" // dest→source: flow control (bytes written)
)

// MigrationFileInfo describes a file (header or resume-manifest entry).
type MigrationFileInfo struct {
	Path   string `json:"path"`             // ruta relativa a source_path/dest_path
	Size   int64  `json:"size"`             // bytes
	Mode   uint32 `json:"mode,omitempty"`   // unix permissions
	Mtime  int64  `json:"mtime,omitempty"`  // epoch segundos
	Sha256 string `json:"sha256,omitempty"` // hex; presente al completarse
}

// MigrationWarning is a notice in the final report (file changed/disappeared, etc.).
type MigrationWarning struct {
	Code    string `json:"code"`
	File    string `json:"file,omitempty"`
	Message string `json:"message"`
}

// MigrationMsg is the common envelope for all migration messages.
type MigrationMsg struct {
	Envelope
	MigrationID string `json:"migration_id"`

	// estimate_req / prepare / start
	SourcePath     string   `json:"source_path,omitempty"`
	DestPath       string   `json:"dest_path,omitempty"`
	ExcludePaths   []string `json:"exclude_paths,omitempty"`
	ChunkSize      int      `json:"chunk_size,omitempty"`
	WindowBytes    int64    `json:"window_bytes,omitempty"`
	VerifyChecksum bool     `json:"verify_checksum,omitempty"`
	// Delta (continuous sync, Type C): in prepare it makes the dest scan the files
	// already present under dest_path and return them in the manifest; the source then
	// skips those matching on size+mtime (only transfers changed/new files).
	Delta bool `json:"delta,omitempty"`

	// estimate_res / receiver_ready / progress
	TotalBytes     int64               `json:"total_bytes,omitempty"`
	TotalFiles     int                 `json:"total_files,omitempty"`
	AvailableBytes int64               `json:"available_bytes,omitempty"`
	Completed      []MigrationFileInfo `json:"completed,omitempty"` // resume manifest

	// progress
	BytesTransferred int64  `json:"bytes_transferred,omitempty"`
	FilesCompleted   int    `json:"files_completed,omitempty"`
	CurrentFile      string `json:"current_file,omitempty"`
	SpeedBytesPerSec int64  `json:"speed_bytes_per_sec,omitempty"`

	// file / chunk / acks (plano de datos)
	FileID     uint32             `json:"file_id,omitempty"`
	File       *MigrationFileInfo `json:"file,omitempty"` // migration_file
	Offset     int64              `json:"offset,omitempty"`
	Data       string             `json:"data,omitempty"` // base64 (migration_chunk)
	CRC32      uint32             `json:"crc32,omitempty"`
	OK         bool               `json:"ok,omitempty"`          // migration_file_ack
	AckedBytes int64              `json:"acked_bytes,omitempty"` // migration_window_ack

	// done / failed
	Status   string             `json:"status,omitempty"` // completed | completed_with_warnings
	Warnings []MigrationWarning `json:"warnings,omitempty"`
	Code     string             `json:"code,omitempty"`
	Message  string             `json:"message,omitempty"`
}

// --- Terminal web (PTY) ---

type PTYStart struct {
	Envelope
	SessionID string `json:"session_id"`
	Cols      uint16 `json:"cols"`
	Rows      uint16 `json:"rows"`
}

type PTYData struct {
	Envelope
	SessionID string `json:"session_id"`
	Data      string `json:"data"` // base64
}

type PTYResize struct {
	Envelope
	SessionID string `json:"session_id"`
	Cols      uint16 `json:"cols"`
	Rows      uint16 `json:"rows"`
}

type PTYClose struct {
	Envelope
	SessionID string `json:"session_id"`
	Error     string `json:"error,omitempty"`
}

