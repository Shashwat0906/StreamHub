// Package transport moves protocol frames over TCP.
//
// The server reads one request at a time per connection and writes the
// responses in order (like Kafka, a connection is a FIFO pipe). Clients use
// a small connection pool per address and do synchronous request/response
// per connection; concurrency comes from using several connections.
//
// Fault injection: every client connection is created by a Dialer that
// knows its own node label. Tests can "cut the cable" between two labels
// with Faults.Block; existing connections between them are closed and new
// dials fail, which is how network partitions are simulated in-process.
package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// Handler processes one request and returns the response message, or nil
// if no response must be sent (Produce with acks=0).
type Handler func(ctx context.Context, h protocol.RequestHeader, body []byte) protocol.Message

// Server accepts connections and dispatches requests to a Handler.
type Server struct {
	ln      net.Listener
	handler Handler
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// Listen starts a server on addr ("host:port"; port 0 picks a free port).
func Listen(addr string, h Handler, logger *slog.Logger) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{ln: ln, handler: h, logger: logger, ctx: ctx, cancel: cancel, conns: map[net.Conn]struct{}{}}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr is the actual listening address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveConn(c)
	}
}

func (s *Server) serveConn(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
	}()
	r := bufio.NewReaderSize(c, 64<<10)
	w := bufio.NewWriterSize(c, 64<<10)
	for {
		payload, err := protocol.ReadFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logger.Debug("connection read error", "remote", c.RemoteAddr().String(), "err", err)
			}
			return
		}
		h, body, err := protocol.ParseRequest(payload)
		if err != nil {
			s.logger.Warn("bad request header", "err", err)
			return
		}
		resp := s.handler(s.ctx, h, body)
		if resp == nil {
			continue
		}
		if _, err := w.Write(protocol.EncodeResponse(h.CorrelationID, resp)); err != nil {
			return
		}
		// Flush only when no further request is already buffered, so
		// pipelined requests get their responses batched into one write.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// Close stops accepting, closes all connections and waits for handlers.
func (s *Server) Close() error {
	s.cancel()
	err := s.ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

// ---------------------------------------------------------------- faults

// Faults is a table of severed links between node labels (usually the
// node's listen address, or "client").
type Faults struct {
	mu      sync.Mutex
	blocked map[[2]string]bool
	conns   map[*faultConn]struct{}
}

func NewFaults() *Faults {
	return &Faults{blocked: map[[2]string]bool{}, conns: map[*faultConn]struct{}{}}
}

func pair(a, b string) [2]string {
	if a > b {
		a, b = b, a
	}
	return [2]string{a, b}
}

// Block severs the link between a and b in both directions and closes any
// existing connection between them.
func (f *Faults) Block(a, b string) {
	f.mu.Lock()
	f.blocked[pair(a, b)] = true
	var victims []*faultConn
	for c := range f.conns {
		if pair(c.from, c.to) == pair(a, b) {
			victims = append(victims, c)
		}
	}
	f.mu.Unlock()
	for _, c := range victims {
		c.Conn.Close()
	}
}

// Heal restores the link between a and b.
func (f *Faults) Heal(a, b string) {
	f.mu.Lock()
	delete(f.blocked, pair(a, b))
	f.mu.Unlock()
}

// HealAll restores every link.
func (f *Faults) HealAll() {
	f.mu.Lock()
	f.blocked = map[[2]string]bool{}
	f.mu.Unlock()
}

func (f *Faults) isBlocked(a, b string) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blocked[pair(a, b)]
}

type faultConn struct {
	net.Conn
	f        *Faults
	from, to string
}

var errLinkDown = errors.New("transport: link down (fault injection)")

// ErrBadResponse wraps a response that arrived but could not be decoded.
// Unlike network errors it is not worth retrying.
var ErrBadResponse = errors.New("transport: bad response")

func (c *faultConn) Read(p []byte) (int, error) {
	if c.f.isBlocked(c.from, c.to) {
		c.Conn.Close()
		return 0, errLinkDown
	}
	return c.Conn.Read(p)
}

func (c *faultConn) Write(p []byte) (int, error) {
	if c.f.isBlocked(c.from, c.to) {
		c.Conn.Close()
		return 0, errLinkDown
	}
	return c.Conn.Write(p)
}

func (c *faultConn) Close() error {
	c.f.mu.Lock()
	delete(c.f.conns, c)
	c.f.mu.Unlock()
	return c.Conn.Close()
}

// ---------------------------------------------------------------- client

// Dialer creates connections on behalf of a node.
type Dialer struct {
	Self        string  // label of the dialing node (fault injection)
	Faults      *Faults // nil in production
	DialTimeout time.Duration
}

func (d *Dialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	if d.Faults.isBlocked(d.Self, addr) {
		return nil, errLinkDown
	}
	timeout := d.DialTimeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	nd := net.Dialer{Timeout: timeout}
	c, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	if d.Faults == nil {
		return c, nil
	}
	fc := &faultConn{Conn: c, f: d.Faults, from: d.Self, to: addr}
	d.Faults.mu.Lock()
	d.Faults.conns[fc] = struct{}{}
	d.Faults.mu.Unlock()
	return fc, nil
}

// Conn is one client connection doing synchronous request/response.
type Conn struct {
	addr string
	c    net.Conn
	r    *bufio.Reader
	corr uint32
}

// DefaultRequestTimeout applies when the context has no deadline.
const DefaultRequestTimeout = 30 * time.Second

// Call sends req and decodes the reply into resp.
func (c *Conn) Call(ctx context.Context, api protocol.APIKey, req, resp protocol.Message) error {
	c.corr++
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(DefaultRequestTimeout)
	}
	c.c.SetDeadline(deadline)
	// Abort promptly if the context is cancelled mid-call.
	stop := context.AfterFunc(ctx, func() { c.c.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	if _, err := c.c.Write(protocol.EncodeRequest(api, c.corr, req)); err != nil {
		return err
	}
	if resp == nil {
		return nil // fire and forget (acks=0)
	}
	payload, err := protocol.ReadFrame(c.r)
	if err != nil {
		return err
	}
	corr, body, err := protocol.ParseResponse(payload)
	if err != nil {
		return err
	}
	if corr != c.corr {
		return fmt.Errorf("transport: correlation id mismatch: got %d want %d", corr, c.corr)
	}
	if err := protocol.Unmarshal(body, resp); err != nil {
		return fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	return nil
}

// Close closes the connection.
func (c *Conn) Close() error { return c.c.Close() }

// Pool keeps idle connections per address.
type Pool struct {
	dialer  *Dialer
	maxIdle int

	mu     sync.Mutex
	idle   map[string][]*Conn
	closed bool
}

// NewPool creates a pool using dialer (nil = plain TCP).
func NewPool(dialer *Dialer) *Pool {
	if dialer == nil {
		dialer = &Dialer{}
	}
	return &Pool{dialer: dialer, maxIdle: 8, idle: map[string][]*Conn{}}
}

func (p *Pool) get(ctx context.Context, addr string) (*Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("transport: pool closed")
	}
	if list := p.idle[addr]; len(list) > 0 {
		c := list[len(list)-1]
		p.idle[addr] = list[:len(list)-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	nc, err := p.dialer.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &Conn{addr: addr, c: nc, r: bufio.NewReaderSize(nc, 64<<10)}, nil
}

func (p *Pool) put(c *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.idle[c.addr]) >= p.maxIdle {
		c.Close()
		return
	}
	p.idle[c.addr] = append(p.idle[c.addr], c)
}

// Call performs one request against addr using a pooled connection. A
// connection that saw any error is discarded, never reused.
func (p *Pool) Call(ctx context.Context, addr string, api protocol.APIKey, req, resp protocol.Message) error {
	c, err := p.get(ctx, addr)
	if err != nil {
		return err
	}
	if err := c.Call(ctx, api, req, resp); err != nil {
		c.Close()
		return err
	}
	// For resp == nil (acks=0) no reply is sent, so the connection is
	// still clean and can be reused.
	p.put(c)
	return nil
}

// CloseAddr drops idle connections to addr (e.g. after a broker failed).
func (p *Pool) CloseAddr(addr string) {
	p.mu.Lock()
	list := p.idle[addr]
	delete(p.idle, addr)
	p.mu.Unlock()
	for _, c := range list {
		c.Close()
	}
}

// Close closes all idle connections.
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	all := p.idle
	p.idle = map[string][]*Conn{}
	p.mu.Unlock()
	for _, list := range all {
		for _, c := range list {
			c.Close()
		}
	}
}
