package dashboard

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ManagedConfig makes the dashboard launch its own local cluster of broker
// processes. Only then can it simulate broker failures safely: it kills
// processes it started itself, never anything else on the machine.
type ManagedConfig struct {
	Brokers   int    // number of brokers (default 3)
	DataDir   string // parent of broker-N data dirs
	BasePort  int    // protocol ports BasePort..BasePort+N-1 (default 19092)
	BaseHTTP  int    // HTTP ports (default 18081)
	Binary    string // streamhub binary (default: this executable)
	ExtraArgs []string
	Host      string // default 127.0.0.1
}

type brokerProc struct {
	id       int32
	addr     string
	httpAddr string
	dataDir  string
	logPath  string
	mu       sync.Mutex
	cmd      *exec.Cmd
	state    string // starting | running | killed | stopped | exited
	exited   chan struct{}
}

// manager starts, stops and kills broker processes.
type manager struct {
	g     *Gateway
	cfg   ManagedConfig
	procs map[int32]*brokerProc
	peers string
}

func newManager(g *Gateway, cfg ManagedConfig) (*manager, error) {
	if cfg.Brokers <= 0 {
		cfg.Brokers = 3
	}
	if cfg.BasePort == 0 {
		cfg.BasePort = 19092
	}
	if cfg.BaseHTTP == 0 {
		cfg.BaseHTTP = 18081
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "dashboard-data"
	}
	if cfg.Binary == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		cfg.Binary = exe
	}
	abs, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	cfg.DataDir = abs
	m := &manager{g: g, cfg: cfg, procs: map[int32]*brokerProc{}}
	var peers []string
	for i := 1; i <= cfg.Brokers; i++ {
		id := int32(i)
		p := &brokerProc{
			id:       id,
			addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.BasePort+i-1),
			httpAddr: fmt.Sprintf("%s:%d", cfg.Host, cfg.BaseHTTP+i-1),
			dataDir:  filepath.Join(cfg.DataDir, fmt.Sprintf("broker-%d", i)),
			logPath:  filepath.Join(cfg.DataDir, fmt.Sprintf("broker-%d.log", i)),
			state:    "stopped",
		}
		m.procs[id] = p
		peers = append(peers, fmt.Sprintf("%d=%s", i, p.addr))
	}
	m.peers = strings.Join(peers, ",")
	return m, nil
}

func (m *manager) bootstrap() []string {
	var out []string
	for _, id := range m.ids() {
		out = append(out, m.procs[id].addr)
	}
	return out
}

func (m *manager) ids() []int32 {
	ids := make([]int32, 0, len(m.procs))
	for id := range m.procs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (m *manager) httpAddrs() map[int32]string {
	out := map[int32]string{}
	for id, p := range m.procs {
		out[id] = p.httpAddr
	}
	return out
}

func (m *manager) addr(id int32) string {
	if p, ok := m.procs[id]; ok {
		return p.addr
	}
	return ""
}

func (m *manager) view(id int32) *ProcessView {
	p, ok := m.procs[id]
	if !ok {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	pv := &ProcessView{State: p.state, LogPath: p.logPath}
	if p.cmd != nil && p.cmd.Process != nil && (p.state == "running" || p.state == "starting") {
		pv.PID = p.cmd.Process.Pid
	}
	return pv
}

func (m *manager) startAll() error {
	if err := os.MkdirAll(m.cfg.DataDir, 0o755); err != nil {
		return err
	}
	for _, id := range m.ids() {
		if err := m.start(id); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) stopAll() {
	var wg sync.WaitGroup
	for _, id := range m.ids() {
		wg.Add(1)
		go func(id int32) { defer wg.Done(); m.stop(id) }(id)
	}
	wg.Wait()
}

// start launches broker id (if not already running).
func (m *manager) start(id int32) error {
	p, ok := m.procs[id]
	if !ok {
		return fmt.Errorf("unknown broker %d", id)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == "running" || p.state == "starting" {
		return fmt.Errorf("broker %d is already running", id)
	}
	if err := os.MkdirAll(p.dataDir, 0o755); err != nil {
		return err
	}
	logf, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	args := []string{"broker",
		"--id", strconv.Itoa(int(id)),
		"--listen", p.addr,
		"--http", p.httpAddr,
		"--data-dir", p.dataDir,
		"--peers", m.peers,
		"--default-replication-factor", strconv.Itoa(min(3, m.cfg.Brokers)),
		"--min-insync-replicas", strconv.Itoa(min(2, m.cfg.Brokers)),
		"--log-format", "json",
	}
	args = append(args, m.cfg.ExtraArgs...)
	cmd := exec.Command(m.cfg.Binary, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	// Own process group: a Ctrl-C on the dashboard does not hit brokers
	// before the dashboard shuts them down in order.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	p.cmd, p.state, p.exited = cmd, "running", make(chan struct{})
	exited := p.exited
	go func() {
		err := cmd.Wait()
		logf.Close()
		p.mu.Lock()
		if p.state == "running" || p.state == "starting" {
			p.state = "exited"
			m.g.emit(Event{Kind: "broker_process_exited", Severity: "error", Broker: id,
				Message: fmt.Sprintf("Broker %d process exited unexpectedly: %v (see %s)", id, err, p.logPath)})
		}
		p.mu.Unlock()
		close(exited)
	}()
	m.g.log.Info("started broker process", "broker", id, "pid", cmd.Process.Pid, "addr", p.addr)
	return nil
}

// kill sends SIGKILL: the broker gets no chance to hand over leadership,
// exactly like a machine crash. Peers notice through missed heartbeats.
func (m *manager) kill(id int32) error { return m.signal(id, syscall.SIGKILL, "killed") }

// stop sends SIGTERM: the broker performs a controlled shutdown (asks the
// controller to move its leaderships first) and exits cleanly.
func (m *manager) stop(id int32) error { return m.signal(id, syscall.SIGTERM, "stopped") }

func (m *manager) signal(id int32, sig syscall.Signal, newState string) error {
	p, ok := m.procs[id]
	if !ok {
		return fmt.Errorf("unknown broker %d", id)
	}
	p.mu.Lock()
	if p.state != "running" && p.state != "starting" {
		p.mu.Unlock()
		return fmt.Errorf("broker %d is not running (%s)", id, p.state)
	}
	p.state = newState
	cmd, exited := p.cmd, p.exited
	p.mu.Unlock()
	if err := cmd.Process.Signal(sig); err != nil {
		return err
	}
	select {
	case <-exited:
		return nil
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		<-exited
		return errors.New("broker did not exit within 15s; killed")
	}
}
