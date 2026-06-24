package collector

import (
	"runtime"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"
	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/internal/privileged"
	"github.com/koyere/auranode-agent/pkg/proto"
)

type Collector struct {
	log          *zap.Logger
	prevNet      map[string]net.IOCountersStat
	prevNetTime  time.Time
}

func New(log *zap.Logger) *Collector {
	return &Collector{log: log, prevNet: make(map[string]net.IOCountersStat)}
}

// SystemInfo gathers static system information (sent once on connect).
func (c *Collector) SystemInfo(version string) proto.AgentInfo {
	info := proto.AgentInfo{
		Envelope: proto.Envelope{
			Type:      proto.TypeAgentInfo,
			Timestamp: time.Now().Unix(),
		},
		Version: version,
		OS:      runtime.GOOS,
		Arch:    runtime.GOARCH,
	}

	if h, err := host.Info(); err == nil {
		info.Hostname = h.Hostname
		info.Kernel = h.KernelVersion
	}

	if counts, err := cpu.Counts(true); err == nil {
		info.CPUCores = counts
	}

	if v, err := mem.VirtualMemory(); err == nil {
		info.TotalRAMMB = int64(v.Total / 1024 / 1024)
	}

	info.PrivilegedAvailable = privileged.Available()

	return info
}

// Collect gathers all system metrics into a snapshot.
func (c *Collector) Collect() proto.Metrics {
	now := time.Now()
	m := proto.Metrics{
		Envelope: proto.Envelope{
			Type:      proto.TypeMetrics,
			Timestamp: now.Unix(),
		},
	}

	// CPU
	if percents, err := cpu.Percent(500*time.Millisecond, false); err == nil && len(percents) > 0 {
		m.CPU.UsagePercent = percents[0]
	}
	if cores, err := cpu.Counts(true); err == nil {
		m.CPU.Cores = cores
	}

	// RAM
	if v, err := mem.VirtualMemory(); err == nil {
		m.RAM.TotalMB = int64(v.Total / 1024 / 1024)
		m.RAM.UsedMB = int64(v.Used / 1024 / 1024)
		m.RAM.FreeMB = int64(v.Free / 1024 / 1024)
	}
	if s, err := mem.SwapMemory(); err == nil {
		m.RAM.SwapTotalMB = int64(s.Total / 1024 / 1024)
		m.RAM.SwapUsedMB = int64(s.Used / 1024 / 1024)
	}

	// Disk: real filesystems only. Pseudo ones (squashfs from snaps, tmpfs,
	// overlay…) are skipped — on Ubuntu they show up at 100% because they are
	// read-only or volatile, which would fake a "disk full".
	if partitions, err := disk.Partitions(false); err == nil {
		seen := make(map[string]bool)
		for _, p := range partitions {
			if seen[p.Mountpoint] || isPseudoFS(p.Fstype, p.Mountpoint) {
				continue
			}
			seen[p.Mountpoint] = true
			if usage, err := disk.Usage(p.Mountpoint); err == nil {
				m.Disk = append(m.Disk, proto.DiskMetric{
					Mount:       p.Mountpoint,
					TotalGB:     float64(usage.Total) / 1e9,
					UsedGB:      float64(usage.Used) / 1e9,
					UsedPercent: usage.UsedPercent,
				})
			}
		}
	}

	// Network (delta since last read)
	if counters, err := net.IOCounters(true); err == nil {
		elapsed := now.Sub(c.prevNetTime).Seconds()
		if elapsed <= 0 {
			elapsed = 1
		}
		for _, cnt := range counters {
			if cnt.Name == "lo" {
				continue
			}
			metric := proto.NetworkMetric{Interface: cnt.Name}
			if prev, ok := c.prevNet[cnt.Name]; ok && c.prevNetTime.IsZero() == false {
				metric.RxBytes = int64(float64(cnt.BytesRecv-prev.BytesRecv) / elapsed)
				metric.TxBytes = int64(float64(cnt.BytesSent-prev.BytesSent) / elapsed)
				if metric.RxBytes < 0 {
					metric.RxBytes = 0
				}
				if metric.TxBytes < 0 {
					metric.TxBytes = 0
				}
			}
			c.prevNet[cnt.Name] = cnt
			m.Network = append(m.Network, metric)
		}
		c.prevNetTime = now
	}

	// Load average
	if avg, err := load.Avg(); err == nil {
		m.LoadAvg = proto.LoadAvg{M1: avg.Load1, M5: avg.Load5, M15: avg.Load15}
	}

	// Top 10 processes by CPU
	if procs, err := process.Processes(); err == nil {
		type entry struct {
			pid  int32
			name string
			cpu  float64
			ram  int64
		}
		entries := make([]entry, 0, len(procs))
		for _, p := range procs {
			cpuPct, _ := p.CPUPercent()
			rss, _ := p.MemoryInfo()
			name, _ := p.Name()
			var ramMB int64
			if rss != nil {
				ramMB = int64(rss.RSS / 1024 / 1024)
			}
			entries = append(entries, entry{p.Pid, name, cpuPct, ramMB})
		}
		// Sort by CPU desc (insertion sort for 10 elements)
		for i := 0; i < len(entries) && i < 10; i++ {
			max := i
			for j := i + 1; j < len(entries); j++ {
				if entries[j].cpu > entries[max].cpu {
					max = j
				}
			}
			entries[i], entries[max] = entries[max], entries[i]
			if i < 10 {
				m.TopProcesses = append(m.TopProcesses, proto.ProcessInfo{
					PID:        int(entries[i].pid),
					Name:       entries[i].name,
					CPUPercent: entries[i].cpu,
					RAMMB:      entries[i].ram,
				})
			}
		}
		if len(m.TopProcesses) > 10 {
			m.TopProcesses = m.TopProcesses[:10]
		}
	}

	return m
}

// pseudoFstypes are virtual/non-physical filesystem types that do not represent
// real storage and must not count as disk usage.
var pseudoFstypes = map[string]bool{
	"squashfs": true, "tmpfs": true, "devtmpfs": true, "overlay": true,
	"aufs": true, "ramfs": true, "proc": true, "sysfs": true,
	"cgroup": true, "cgroup2": true, "devpts": true, "mqueue": true,
	"debugfs": true, "tracefs": true, "securityfs": true, "pstore": true,
	"autofs": true, "binfmt_misc": true, "configfs": true, "fusectl": true,
	"hugetlbfs": true, "nsfs": true, "bpf": true, "efivarfs": true,
	"fuse.snapfuse": true, "fuse.gvfsd-fuse": true, "fuse.portal": true,
}

// isPseudoFS reports whether a mount should be ignored because it is a virtual
// filesystem (squashfs from snaps, tmpfs, overlay…) or lives under system paths.
func isPseudoFS(fstype, mount string) bool {
	if pseudoFstypes[strings.ToLower(fstype)] {
		return true
	}
	return strings.HasPrefix(mount, "/snap/") ||
		strings.HasPrefix(mount, "/proc") ||
		strings.HasPrefix(mount, "/sys") ||
		strings.HasPrefix(mount, "/dev") ||
		strings.HasPrefix(mount, "/run")
}
