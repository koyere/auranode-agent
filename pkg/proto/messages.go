// Package proto defines the AuraNode agent↔backend protocol message types.
// The types must stay in sync with backend/internal/websocket/messages.go.
package proto

import "encoding/json"

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
	TypeSysActionResult = "sys_action_result" // resultado de una acción privilegiada
	TypeError           = "error"

	// Backend → Agent
	TypeConfig    = "config"
	TypeExec      = "exec"
	TypeRuleSync  = "rule_sync"
	TypeFSRequest = "fs_request"
	TypeAgentPing = "ping"
	TypeSysAction = "sys_action" // ejecutar una acción privilegiada de la whitelist

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

	// ── Gestión de bases de datos (Parte 3 · D1+) ───────────────────────────────
	// El agente actúa como cliente de BD (nunca admin del sistema): se conecta a los
	// motores locales con drivers Go puros y responde detección/exploración/consulta.
	TypeDBRequest  = "db_request"  // backend→agente
	TypeDBResponse = "db_response" // agente→backend
)

// ─── Database ops ─────────────────────────────────────────────────────────────
const (
	DBOpDetect    = "detect"    // sondear motores locales (sin credenciales)
	DBOpTest      = "test"      // probar una conexión con credenciales
	DBOpDatabases = "databases" // listar BDs + estado del motor + usuarios/roles
	DBOpTables    = "tables"    // listar tablas de una BD (tamaño, filas estimadas)
	DBOpQuery     = "query"     // ejecutar un statement SQL acotado (D2, consola SQL)
	DBOpAdmin     = "admin"     // gestión: crear/eliminar BD/usuario, contraseña, grants (D3)
	DBOpDump      = "dump"       // crear un dump comprimido de una BD (D4, backups)
	DBOpDumps     = "dumps"      // listar los dumps existentes (D4)
	DBOpRestore   = "restore"    // restaurar una BD desde un dump (D4)
	DBOpDumpDel   = "dump_delete" // eliminar un dump (D4)
	DBOpRedis     = "redis"      // estado de Redis (INFO, memoria, nº de claves)
)

// ─── Database admin actions (op=admin, D3) ────────────────────────────────────
const (
	DBAdminCreateDatabase = "create_database" // crear una base de datos
	DBAdminDropDatabase   = "drop_database"   // eliminar una base de datos
	DBAdminCreateUser     = "create_user"     // crear un usuario/rol con login + contraseña
	DBAdminDropUser       = "drop_user"       // eliminar un usuario/rol
	DBAdminChangePassword = "change_password" // cambiar la contraseña de un usuario/rol
	DBAdminGrant          = "grant"           // conceder privilegios básicos sobre una BD
	DBAdminRevoke         = "revoke"          // revocar todos los privilegios sobre una BD
)

// Niveles de privilegio para grant (mapeados a grants concretos por motor).
const (
	DBPrivReadOnly  = "readonly"  // solo lectura (SELECT)
	DBPrivReadWrite = "readwrite" // lectura + DML (SELECT/INSERT/UPDATE/DELETE)
	DBPrivAll       = "all"       // todos los privilegios
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
	// MachineID is the host's stable identifier (Linux machine-id). It lets the panel
	// detect the same server registered twice within a tenant.
	MachineID string `json:"machine_id"`
	// PrivilegedAvailable indica si el helper root está instalado en la VPS
	// (modo privilegiado acotado disponible para activar desde el panel).
	PrivilegedAvailable bool `json:"privileged_available"`
}

// SysAction: Backend → Agent. Solicita ejecutar una acción privilegiada de la
// whitelist. El agente la reenvía al helper root (que revalida).
type SysAction struct {
	Envelope
	ActionID string            `json:"action_id"`
	Action   string            `json:"action"`
	Args     map[string]string `json:"args,omitempty"`
}

// SysActionResult: Agent → Backend. Resultado de una acción privilegiada.
type SysActionResult struct {
	Envelope
	ActionID   string `json:"action_id"`
	OK         bool   `json:"ok"`
	Rejected   bool   `json:"rejected,omitempty"`
	ExitStatus int    `json:"exit_status"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	Error      string `json:"error,omitempty"`
	DurationMS int64  `json:"duration_ms"`
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

// ─── Database management (Parte 3 · D1+) ──────────────────────────────────────

// DBConn son las credenciales/parámetros de una conexión a un motor local. Viajan
// descifradas del backend al agente y son efímeras (nunca se persisten en el agente).
type DBConn struct {
	Engine   string `json:"engine"`         // postgres | mysql
	Host     string `json:"host,omitempty"` // vacío + Socket → conexión por socket
	Port     int    `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
	Socket   string `json:"socket,omitempty"`    // ruta del socket unix local (C6)
	UseLocal bool   `json:"use_local,omitempty"` // usar el acceso local del sistema (peer/socket)
}

// DBRequest: Backend → Agent. Una operación de base de datos.
type DBRequest struct {
	Envelope
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
	Conn      DBConn `json:"conn"`
	Database  string `json:"database,omitempty"`  // BD objetivo (tables/query)
	ReadOnly  bool   `json:"read_only,omitempty"` // impone solo-lectura en la conexión
	SQL       string `json:"sql,omitempty"`       // op=query: statement único a ejecutar
	MaxRows   int    `json:"max_rows,omitempty"`  // op=query: tope de filas devueltas (0 = por defecto)
	Admin     *DBAdminSpec `json:"admin,omitempty"` // op=admin: acción de gestión (D3)
	DumpFile  string `json:"dump_file,omitempty"` // op=restore/dump_delete: nombre del dump objetivo (D4)
}

// DBAdminSpec describe una acción de gestión (op=admin, D3). Los identificadores se
// validan y se entrecomillan por motor en el agente (nunca se interpolan crudos).
type DBAdminSpec struct {
	Action    string `json:"action"`              // create_database|drop_database|create_user|drop_user|change_password|grant|revoke
	Database  string `json:"database,omitempty"`  // BD objetivo (create/drop database, grant/revoke)
	Username  string `json:"username,omitempty"`  // usuario/rol objetivo (create/drop user, change_password, grant/revoke)
	Password  string `json:"password,omitempty"`  // create_user / change_password
	Privilege string `json:"privilege,omitempty"` // grant: readonly|readwrite|all
}

// DBResponse: Agent → Backend. Resultado de una operación. Data lleva el payload
// específico de la op en JSON (el backend lo reenvía al panel casi tal cual).
type DBResponse struct {
	Envelope
	RequestID string          `json:"request_id"`
	OK        bool            `json:"ok"`
	Error     string          `json:"error,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// ── Payloads de Data ──────────────────────────────────────────────────────────

// DetectedEngine describe un motor encontrado en la máquina (op detect).
type DetectedEngine struct {
	Engine  string `json:"engine"`  // postgres | mysql | redis
	Running bool   `json:"running"` // hay algo escuchando en el puerto/socket
	Port    int    `json:"port,omitempty"`
	Socket  string `json:"socket,omitempty"`
	Version string `json:"version,omitempty"` // si se pudo leer (test/status)
}

// DBEngineStatus es el estado del motor (op databases).
type DBEngineStatus struct {
	Engine      string `json:"engine"`
	Version     string `json:"version"`
	UptimeSec   int64  `json:"uptime_sec,omitempty"`
	Connections int    `json:"connections,omitempty"`
}

// DBInfo es una base de datos con su tamaño (op databases).
type DBInfo struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
}

// DBUser es un usuario/rol del motor con un resumen de privilegios (op databases).
type DBUser struct {
	Name       string `json:"name"`
	CanLogin   bool   `json:"can_login,omitempty"`
	Superuser  bool   `json:"superuser,omitempty"`
	Privileges string `json:"privileges,omitempty"` // resumen legible de grants
}

// DBDatabasesData es el payload de la op databases.
type DBDatabasesData struct {
	Status    DBEngineStatus `json:"status"`
	Databases []DBInfo       `json:"databases"`
	Users     []DBUser       `json:"users"`
}

// DBTable es una tabla con tamaño y filas estimadas (op tables).
type DBTable struct {
	Schema    string `json:"schema,omitempty"`
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
	RowsEst   int64  `json:"rows_est"`
}

// DBAdminData es el resultado de la op admin (gestión, D3): un resumen legible de lo hecho.
type DBAdminData struct {
	Message string `json:"message"`
}

// DBDumpData es el resultado de la op dump (D4): metadatos del dump creado.
type DBDumpData struct {
	File       string `json:"file"`        // nombre del archivo (sin ruta)
	SizeBytes  int64  `json:"size_bytes"`
	DurationMS int64  `json:"duration_ms"`
	Message    string `json:"message,omitempty"`
}

// DBDumpInfo describe un dump existente (op dumps, D4).
type DBDumpInfo struct {
	File         string `json:"file"`
	Database     string `json:"database"`
	Engine       string `json:"engine"`
	SizeBytes    int64  `json:"size_bytes"`
	ModifiedUnix int64  `json:"modified_unix"`
	Path         string `json:"path"` // ruta absoluta (para descargar vía el módulo de archivos)
}

// DBDumpsData es el payload de la op dumps (D4).
type DBDumpsData struct {
	Dir   string       `json:"dir"`
	Dumps []DBDumpInfo `json:"dumps"`
}

// DBRedisData es el estado de Redis (op redis): INFO + nº de claves. Solo lectura.
type DBRedisData struct {
	Version     string `json:"version"`
	UptimeSec   int64  `json:"uptime_sec"`
	Memory      string `json:"memory"`       // used_memory_human
	MemoryBytes int64  `json:"memory_bytes"` // used_memory
	Keys        int64  `json:"keys"`         // total de claves (todas las BDs)
	Connections int64  `json:"connections"`  // connected_clients
	Mode        string `json:"mode,omitempty"`
}

// DBQueryData es el resultado de la op query (consola SQL, D2). Rows lleva las celdas
// como texto (NULL = null en el JSON) para no depender del tipo de columna. Un statement
// que no devuelve filas (INSERT/UPDATE/DDL) llega con Columns vacío y RowsReturned=0.
type DBQueryData struct {
	Columns      []string   `json:"columns"`
	Rows         [][]*string `json:"rows"`
	RowsReturned int        `json:"rows_returned"`
	Truncated    bool       `json:"truncated"`     // se alcanzó el tope de filas o de bytes
	ReadOnly     bool       `json:"read_only"`     // la conexión se abrió en solo-lectura
	DurationMS   int64      `json:"duration_ms"`
}
