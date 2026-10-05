// Package client is the StreamHub Go client: cluster metadata discovery,
// an admin API, a batching/idempotent producer and consumers (single
// partition and consumer-group based).
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/transport"
)

// Config configures a Client.
type Config struct {
	// Bootstrap is a list of broker addresses used to discover the cluster.
	Bootstrap []string
	// ClientID is reported to group coordinators.
	ClientID string
	// RequestTimeout bounds one request (default 30s).
	RequestTimeout time.Duration
	// MetadataMaxAge forces a metadata refresh this often (default 30s).
	MetadataMaxAge time.Duration
	// UnknownTopicWait is how long lookups keep refreshing metadata for a
	// topic that is not known yet (default 3s).
	UnknownTopicWait time.Duration
	// Dialer allows fault injection in tests (nil = plain TCP).
	Dialer *transport.Dialer
	Logger *slog.Logger
}

// Client holds connections and a cache of cluster metadata. It is safe for
// concurrent use and can be shared by producers, consumers and admin calls.
type Client struct {
	cfg  Config
	pool *transport.Pool
	log  *slog.Logger

	mu          sync.RWMutex
	brokers     map[int32]string
	topics      map[string]protocol.TopicInfo
	controller  int32
	lastRefresh time.Time
	refreshMu   sync.Mutex
}

// New creates a client. No connection is made until the first request.
func New(cfg Config) (*Client, error) {
	if len(cfg.Bootstrap) == 0 {
		return nil, errors.New("client: at least one bootstrap address is required")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.MetadataMaxAge == 0 {
		cfg.MetadataMaxAge = 30 * time.Second
	}
	if cfg.UnknownTopicWait == 0 {
		cfg.UnknownTopicWait = 3 * time.Second
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "streamhub-go"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Client{
		cfg:        cfg,
		pool:       transport.NewPool(cfg.Dialer),
		log:        cfg.Logger,
		brokers:    map[int32]string{},
		topics:     map[string]protocol.TopicInfo{},
		controller: -1,
	}, nil
}

// Close releases connections.
func (c *Client) Close() error {
	c.pool.Close()
	return nil
}

// call performs one request with the configured timeout.
func (c *Client) call(ctx context.Context, addr string, api protocol.APIKey, req, resp protocol.Message) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.RequestTimeout)
		defer cancel()
	}
	err := c.pool.Call(ctx, addr, api, req, resp)
	if errors.Is(err, transport.ErrBadResponse) {
		return &protocol.Error{Code: protocol.ErrUnknown, Msg: fmt.Sprintf("%s %s: %v", api, addr, err)}
	}
	if err != nil {
		c.pool.CloseAddr(addr)
		return &protocol.Error{Code: protocol.ErrNetwork, Msg: fmt.Sprintf("%s %s: %v", api, addr, err)}
	}
	return nil
}

// knownAddrs returns broker addresses to try, metadata first then
// bootstrap, in random order to spread load.
func (c *Client) knownAddrs() []string {
	c.mu.RLock()
	seen := map[string]bool{}
	var out []string
	for _, a := range c.brokers {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	c.mu.RUnlock()
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	for _, a := range c.cfg.Bootstrap {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

// RefreshMetadata fetches metadata for all topics from any reachable broker.
func (c *Client) RefreshMetadata(ctx context.Context) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	var lastErr error
	for _, addr := range c.knownAddrs() {
		var resp protocol.MetadataResponse
		if err := c.call(ctx, addr, protocol.APIMetadata, &protocol.MetadataRequest{}, &resp); err != nil {
			lastErr = err
			continue
		}
		if resp.Err != protocol.ErrNone {
			lastErr = resp.Err.AsError("metadata")
			continue
		}
		c.mu.Lock()
		c.brokers = map[int32]string{}
		for _, b := range resp.Brokers {
			c.brokers[b.ID] = b.Addr
		}
		c.topics = map[string]protocol.TopicInfo{}
		for _, t := range resp.Topics {
			if t.Err == protocol.ErrNone {
				c.topics[t.Name] = t
			}
		}
		c.controller = resp.ControllerID
		c.lastRefresh = time.Now()
		c.mu.Unlock()
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("client: no brokers reachable")
	}
	return lastErr
}

func (c *Client) maybeRefresh(ctx context.Context) error {
	c.mu.RLock()
	stale := time.Since(c.lastRefresh) > c.cfg.MetadataMaxAge
	c.mu.RUnlock()
	if stale {
		return c.RefreshMetadata(ctx)
	}
	return nil
}

// Topic returns cached metadata for a topic, refreshing once if unknown.
func (c *Client) Topic(ctx context.Context, name string) (protocol.TopicInfo, error) {
	if err := c.maybeRefresh(ctx); err != nil {
		return protocol.TopicInfo{}, err
	}
	c.mu.RLock()
	t, ok := c.topics[name]
	c.mu.RUnlock()
	if ok {
		return t, nil
	}
	// Unknown: refresh a few times. A topic created moments ago may not
	// have reached the broker we asked yet (metadata is replayed from the
	// Raft log on each broker).
	deadline := time.Now().Add(c.cfg.UnknownTopicWait)
	for attempt := 0; ; attempt++ {
		if err := c.RefreshMetadata(ctx); err != nil {
			return protocol.TopicInfo{}, err
		}
		c.mu.RLock()
		t, ok = c.topics[name]
		c.mu.RUnlock()
		if ok {
			return t, nil
		}
		if time.Now().After(deadline) || sleepCtx(ctx, backoff(attempt)) != nil {
			return protocol.TopicInfo{}, &protocol.Error{Code: protocol.ErrUnknownTopicOrPartition, Msg: name}
		}
	}
}

// Partitions returns the number of partitions of a topic.
func (c *Client) Partitions(ctx context.Context, topic string) (int, error) {
	t, err := c.Topic(ctx, topic)
	if err != nil {
		return 0, err
	}
	return len(t.Partitions), nil
}

// leader returns the leader's ID, address and epoch for a partition.
func (c *Client) leader(ctx context.Context, topic string, partition int32) (int32, string, int32, error) {
	t, err := c.Topic(ctx, topic)
	if err != nil {
		return -1, "", -1, err
	}
	if partition < 0 || int(partition) >= len(t.Partitions) {
		return -1, "", -1, &protocol.Error{Code: protocol.ErrUnknownTopicOrPartition, Msg: fmt.Sprintf("%s-%d", topic, partition)}
	}
	p := t.Partitions[partition]
	if p.Leader < 0 {
		return -1, "", -1, &protocol.Error{Code: protocol.ErrLeaderNotAvailable, Msg: fmt.Sprintf("%s-%d offline", topic, partition)}
	}
	c.mu.RLock()
	addr, ok := c.brokers[p.Leader]
	c.mu.RUnlock()
	if !ok {
		return -1, "", -1, &protocol.Error{Code: protocol.ErrLeaderNotAvailable, Msg: fmt.Sprintf("unknown broker %d", p.Leader)}
	}
	return p.Leader, addr, p.LeaderEpoch, nil
}

// brokerAddr returns a broker's address from the cache.
func (c *Client) brokerAddr(id int32) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	a, ok := c.brokers[id]
	return a, ok
}

// controllerAddr returns the controller's address, refreshing if needed.
func (c *Client) controllerAddr(ctx context.Context) (string, error) {
	if err := c.maybeRefresh(ctx); err != nil {
		return "", err
	}
	c.mu.RLock()
	id := c.controller
	addr, ok := c.brokers[id]
	c.mu.RUnlock()
	if !ok || id < 0 {
		return "", &protocol.Error{Code: protocol.ErrNotController, Msg: "controller unknown"}
	}
	return addr, nil
}

// Brokers returns the cached broker list sorted by ID.
func (c *Client) Brokers() []protocol.BrokerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]protocol.BrokerInfo, 0, len(c.brokers))
	for id, a := range c.brokers {
		out = append(out, protocol.BrokerInfo{ID: id, Addr: a})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// isRetriable reports whether err is worth retrying after a metadata refresh.
func isRetriable(err error) bool {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe.Code.Retriable()
	}
	return false
}

// codeOf extracts a protocol error code from err (ErrUnknown otherwise).
func codeOf(err error) protocol.ErrorCode {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	if err == nil {
		return protocol.ErrNone
	}
	return protocol.ErrUnknown
}

// backoff returns an exponential backoff with jitter, capped at 1s.
func backoff(attempt int) time.Duration {
	d := 50 * time.Millisecond << min(attempt, 5)
	if d > time.Second {
		d = time.Second
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
