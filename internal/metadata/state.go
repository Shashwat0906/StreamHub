// Package metadata holds StreamHub's cluster metadata as a deterministic
// state machine.
//
// Every change is a Command. Commands are first committed to a replicated
// log (Raft) and then applied, in the same order, on every broker. Because
// Apply is deterministic, every broker ends up with an identical metadata
// image without any extra synchronisation. Leader elections and ISR rules
// live inside Apply for the same reason: all brokers reach the same
// decision from the same log.
package metadata

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// NoLeader marks an offline partition.
const NoLeader int32 = -1

// BrokerMeta is a registered broker.
type BrokerMeta struct {
	ID     int32  `json:"id"`
	Addr   string `json:"addr"`
	Fenced bool   `json:"fenced"`
}

// PartitionMeta is the replicated state of one partition.
type PartitionMeta struct {
	Topic    string  `json:"topic"`
	ID       int32   `json:"id"`
	Replicas []int32 `json:"replicas"` // preferred order; Replicas[0] is the preferred leader
	Leader   int32   `json:"leader"`
	// LeaderEpoch increases every time leadership changes. Followers and
	// clients use it to reject requests from a stale leader.
	LeaderEpoch int32 `json:"leader_epoch"`
	// ISR = in-sync replicas: replicas that are caught up with the leader.
	// Only ISR members may become leader (no unclean election).
	ISR []int32 `json:"isr"`
	// PartitionEpoch increases on every change (leader or ISR) and fences
	// concurrent AlterISR requests.
	PartitionEpoch int32 `json:"partition_epoch"`
}

// Clone returns a deep copy.
func (p *PartitionMeta) Clone() PartitionMeta {
	c := *p
	c.Replicas = slices.Clone(p.Replicas)
	c.ISR = slices.Clone(p.ISR)
	return c
}

// TopicMeta is a topic and its partitions.
type TopicMeta struct {
	Name       string            `json:"name"`
	Partitions []*PartitionMeta  `json:"partitions"`
	Configs    map[string]string `json:"configs,omitempty"`
}

// ConfigInt64 reads an integer topic config with a default.
func (t *TopicMeta) ConfigInt64(key string, def int64) int64 {
	if v, ok := t.Configs[key]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// State is the full metadata image.
type State struct {
	Brokers        map[int32]*BrokerMeta `json:"brokers"`
	Topics         map[string]*TopicMeta `json:"topics"`
	NextProducerID int64                 `json:"next_producer_id"`
}

func newState() *State {
	return &State{Brokers: map[int32]*BrokerMeta{}, Topics: map[string]*TopicMeta{}, NextProducerID: 1000}
}

// ---------------------------------------------------------------- commands

// CommandType enumerates metadata commands.
type CommandType string

const (
	CmdRegisterBroker   CommandType = "register_broker"
	CmdFenceBroker      CommandType = "fence_broker"
	CmdCreateTopic      CommandType = "create_topic"
	CmdDeleteTopic      CommandType = "delete_topic"
	CmdAlterISR         CommandType = "alter_isr"
	CmdAllocateProducer CommandType = "allocate_producer_id"
	CmdNoop             CommandType = "noop"
)

// Command is the unit of change in the metadata log (JSON encoded: easy
// to inspect, and the metadata log is low volume).
type Command struct {
	Type CommandType `json:"type"`

	BrokerID int32  `json:"broker_id,omitempty"`
	Addr     string `json:"addr,omitempty"`

	Topic       string            `json:"topic,omitempty"`
	Assignments [][]int32         `json:"assignments,omitempty"` // replicas per partition
	Configs     map[string]string `json:"configs,omitempty"`

	Partition      int32   `json:"partition,omitempty"`
	LeaderEpoch    int32   `json:"leader_epoch,omitempty"`
	PartitionEpoch int32   `json:"partition_epoch,omitempty"`
	ISR            []int32 `json:"isr,omitempty"`
}

// Encode serialises a command for the log.
func (c Command) Encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // only plain data types: cannot fail
	}
	return b
}

// Result is what Apply returns to the proposer.
type Result struct {
	Err        protocol.ErrorCode
	Msg        string
	ProducerID int64
}

func fail(code protocol.ErrorCode, format string, args ...any) Result {
	return Result{Err: code, Msg: fmt.Sprintf(format, args...)}
}

// ---------------------------------------------------------------- store

// Store is a thread-safe metadata image plus the Apply logic.
type Store struct {
	mu           sync.RWMutex
	st           *State
	appliedIndex uint64

	listenMu  sync.Mutex
	listeners []chan struct{}
}

// NewStore returns an empty store.
func NewStore() *Store { return &Store{st: newState()} }

// Subscribe returns a channel that receives a signal (coalesced) after
// every applied change.
func (s *Store) Subscribe() <-chan struct{} {
	ch := make(chan struct{}, 1)
	s.listenMu.Lock()
	s.listeners = append(s.listeners, ch)
	s.listenMu.Unlock()
	return ch
}

func (s *Store) notify() {
	s.listenMu.Lock()
	defer s.listenMu.Unlock()
	for _, ch := range s.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// AppliedIndex is the log index of the last applied command.
func (s *Store) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedIndex
}

// Apply decodes and applies one committed log entry. It must be
// deterministic: no clocks, no randomness, no map iteration order leaks.
func (s *Store) Apply(index uint64, data []byte) any {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return fail(protocol.ErrInvalidRequest, "bad command: %v", err)
	}
	s.mu.Lock()
	res := s.applyLocked(c)
	if index > s.appliedIndex {
		s.appliedIndex = index
	}
	s.mu.Unlock()
	if res.Err == protocol.ErrNone {
		s.notify()
	}
	return res
}

func (s *Store) applyLocked(c Command) Result {
	st := s.st
	switch c.Type {
	case CmdNoop:
		return Result{}

	case CmdRegisterBroker:
		b, ok := st.Brokers[c.BrokerID]
		if !ok {
			b = &BrokerMeta{ID: c.BrokerID}
			st.Brokers[c.BrokerID] = b
		}
		b.Addr = c.Addr
		b.Fenced = false
		// A broker coming back may restore offline partitions: if it is in
		// the ISR of a leaderless partition it was the last in-sync
		// replica, so electing it is "clean" (no acknowledged data lost).
		for _, p := range st.sortedPartitions() {
			if p.Leader == NoLeader && slices.Contains(p.ISR, c.BrokerID) {
				p.Leader = c.BrokerID
				p.LeaderEpoch++
				p.PartitionEpoch++
			}
		}
		return Result{}

	case CmdFenceBroker:
		b, ok := st.Brokers[c.BrokerID]
		if !ok {
			return fail(protocol.ErrInvalidRequest, "unknown broker %d", c.BrokerID)
		}
		b.Fenced = true
		for _, p := range st.sortedPartitions() {
			st.fenceReplica(p, c.BrokerID)
		}
		return Result{}

	case CmdCreateTopic:
		if _, ok := st.Topics[c.Topic]; ok {
			return fail(protocol.ErrTopicAlreadyExists, "topic %q already exists", c.Topic)
		}
		if len(c.Assignments) == 0 {
			return fail(protocol.ErrInvalidPartitions, "no partitions")
		}
		t := &TopicMeta{Name: c.Topic, Configs: c.Configs}
		for i, replicas := range c.Assignments {
			p := &PartitionMeta{Topic: c.Topic, ID: int32(i), Replicas: slices.Clone(replicas), Leader: NoLeader}
			// Initial ISR: every unfenced replica (they are all empty, so
			// trivially in sync). Leader: first unfenced replica.
			for _, r := range replicas {
				if b, ok := st.Brokers[r]; ok && !b.Fenced {
					p.ISR = append(p.ISR, r)
					if p.Leader == NoLeader {
						p.Leader = r
					}
				}
			}
			if len(p.ISR) == 0 {
				return fail(protocol.ErrInvalidReplicationFactor, "no live replica for partition %d", i)
			}
			t.Partitions = append(t.Partitions, p)
		}
		st.Topics[c.Topic] = t
		return Result{}

	case CmdDeleteTopic:
		if _, ok := st.Topics[c.Topic]; !ok {
			return fail(protocol.ErrUnknownTopicOrPartition, "topic %q not found", c.Topic)
		}
		delete(st.Topics, c.Topic)
		return Result{}

	case CmdAlterISR:
		t, ok := st.Topics[c.Topic]
		if !ok || int(c.Partition) >= len(t.Partitions) {
			return fail(protocol.ErrUnknownTopicOrPartition, "%s-%d", c.Topic, c.Partition)
		}
		p := t.Partitions[c.Partition]
		// Fencing: only the current leader, at the current epoch, may
		// change the ISR. A deposed leader's request is rejected here.
		if p.Leader != c.BrokerID || p.LeaderEpoch != c.LeaderEpoch {
			return fail(protocol.ErrFencedLeaderEpoch, "leader %d epoch %d, request from %d epoch %d",
				p.Leader, p.LeaderEpoch, c.BrokerID, c.LeaderEpoch)
		}
		if !slices.Contains(c.ISR, p.Leader) {
			return fail(protocol.ErrInvalidISR, "ISR must contain the leader")
		}
		for _, r := range c.ISR {
			if !slices.Contains(p.Replicas, r) {
				return fail(protocol.ErrInvalidISR, "%d is not a replica", r)
			}
			// Do not let a fenced broker back into the ISR.
			if b, ok := st.Brokers[r]; (!ok || b.Fenced) && !slices.Contains(p.ISR, r) {
				return fail(protocol.ErrInvalidISR, "broker %d is fenced", r)
			}
		}
		p.ISR = sortedUnique(c.ISR)
		p.PartitionEpoch++
		return Result{}

	case CmdAllocateProducer:
		id := st.NextProducerID
		st.NextProducerID++
		return Result{ProducerID: id}
	}
	return fail(protocol.ErrInvalidRequest, "unknown command %q", c.Type)
}

// fenceReplica removes a dead broker from a partition's ISR and, if it was
// the leader, elects the next live ISR member in replica order.
func (st *State) fenceReplica(p *PartitionMeta, broker int32) {
	if !slices.Contains(p.Replicas, broker) {
		return
	}
	changed := false
	// Remove from ISR unless it is the very last member: the last ISR
	// member must stay recorded so that it (and only it) can be elected
	// when it comes back, which is what preserves acknowledged writes.
	if slices.Contains(p.ISR, broker) && len(p.ISR) > 1 {
		p.ISR = slices.DeleteFunc(slices.Clone(p.ISR), func(r int32) bool { return r == broker })
		changed = true
	}
	if p.Leader == broker {
		p.Leader = NoLeader
		for _, r := range p.Replicas {
			if r == broker || !slices.Contains(p.ISR, r) {
				continue
			}
			if b, ok := st.Brokers[r]; ok && !b.Fenced {
				p.Leader = r
				break
			}
		}
		p.LeaderEpoch++
		changed = true
	}
	if changed {
		p.PartitionEpoch++
	}
}

func (st *State) sortedPartitions() []*PartitionMeta {
	names := make([]string, 0, len(st.Topics))
	for n := range st.Topics {
		names = append(names, n)
	}
	sort.Strings(names)
	var out []*PartitionMeta
	for _, n := range names {
		out = append(out, st.Topics[n].Partitions...)
	}
	return out
}

func sortedUnique(v []int32) []int32 {
	out := slices.Clone(v)
	slices.Sort(out)
	return slices.Compact(out)
}

// ---------------------------------------------------------------- reads

// Broker returns a copy of a broker's metadata.
func (s *Store) Broker(id int32) (BrokerMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.st.Brokers[id]
	if !ok {
		return BrokerMeta{}, false
	}
	return *b, true
}

// Brokers returns all brokers sorted by ID.
func (s *Store) Brokers() []BrokerMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]BrokerMeta, 0, len(s.st.Brokers))
	for _, b := range s.st.Brokers {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// LiveBrokers returns unfenced broker IDs, sorted.
func (s *Store) LiveBrokers() []int32 {
	var out []int32
	for _, b := range s.Brokers() {
		if !b.Fenced {
			out = append(out, b.ID)
		}
	}
	return out
}

// Topic returns a deep copy of one topic.
func (s *Store) Topic(name string) (TopicMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.st.Topics[name]
	if !ok {
		return TopicMeta{}, false
	}
	return cloneTopic(t), true
}

// Topics returns deep copies of all topics sorted by name.
func (s *Store) Topics() []TopicMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TopicMeta, 0, len(s.st.Topics))
	for _, t := range s.st.Topics {
		out = append(out, cloneTopic(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Partition returns a copy of one partition.
func (s *Store) Partition(topic string, id int32) (PartitionMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.st.Topics[topic]
	if !ok || id < 0 || int(id) >= len(t.Partitions) {
		return PartitionMeta{}, false
	}
	return t.Partitions[id].Clone(), true
}

func cloneTopic(t *TopicMeta) TopicMeta {
	c := TopicMeta{Name: t.Name, Configs: map[string]string{}}
	for k, v := range t.Configs {
		c.Configs[k] = v
	}
	for _, p := range t.Partitions {
		pc := p.Clone()
		c.Partitions = append(c.Partitions, &pc)
	}
	return c
}

// ---------------------------------------------------------------- assignment

// AssignReplicas spreads partitions over brokers round-robin, staggering
// the followers so leadership and replicas are balanced. start rotates the
// first broker so different topics do not all lead on the same broker.
//
// Example, 3 brokers [1 2 3], 3 partitions, RF 3, start 0:
//
//	p0: [1 2 3]  p1: [2 3 1]  p2: [3 1 2]
func AssignReplicas(brokers []int32, partitions int, rf int, start int) ([][]int32, error) {
	if rf < 1 {
		return nil, fmt.Errorf("replication factor must be >= 1")
	}
	if len(brokers) < rf {
		return nil, fmt.Errorf("replication factor %d larger than live brokers %d", rf, len(brokers))
	}
	if partitions < 1 {
		return nil, fmt.Errorf("partitions must be >= 1")
	}
	n := len(brokers)
	out := make([][]int32, partitions)
	for p := 0; p < partitions; p++ {
		first := (start + p) % n
		// The shift between replicas grows every n partitions so that
		// follower placement varies too (same idea as Kafka's assignment).
		shift := 1 + (p/n)%max(1, n-1)
		replicas := []int32{brokers[first]}
		for r := 1; r < rf; r++ {
			idx := (first + shift*r) % n
			for slices.Contains(replicas, brokers[idx]) {
				idx = (idx + 1) % n
			}
			replicas = append(replicas, brokers[idx])
		}
		out[p] = replicas
	}
	return out, nil
}
