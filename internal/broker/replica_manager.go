package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/metadata"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

type tp struct {
	topic     string
	partition int32
}

func (t tp) String() string { return fmt.Sprintf("%s-%d", t.topic, t.partition) }

// ReplicaManager owns every partition replica hosted on this broker and
// keeps them in line with the metadata image: it opens logs for new
// assignments, switches replicas between leader and follower when the
// metadata says so, and deletes logs of deleted topics.
type ReplicaManager struct {
	b       *Broker
	logsDir string
	hwPath  string

	mu         sync.RWMutex
	partitions map[tp]*Partition
	hwCheckpt  map[string]int64

	fetchers *fetcherManager
}

func newReplicaManager(b *Broker) (*ReplicaManager, error) {
	rm := &ReplicaManager{
		b:          b,
		logsDir:    filepath.Join(b.cfg.DataDir, "logs"),
		hwPath:     filepath.Join(b.cfg.DataDir, "hw-checkpoint.json"),
		partitions: map[tp]*Partition{},
		hwCheckpt:  map[string]int64{},
	}
	if err := os.MkdirAll(rm.logsDir, 0o755); err != nil {
		return nil, err
	}
	if data, err := os.ReadFile(rm.hwPath); err == nil {
		json.Unmarshal(data, &rm.hwCheckpt)
	}
	rm.fetchers = newFetcherManager(rm)
	return rm, nil
}

func (rm *ReplicaManager) start() {
	b := rm.b
	changes := b.meta.Subscribe()
	rm.reconcile()
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		// Reconcile on every metadata change, and periodically as a safety
		// net (e.g. a log failed to open and should be retried).
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-b.ctx.Done():
				return
			case <-changes:
			case <-t.C:
			}
			rm.reconcile()
		}
	}()
	b.goLoop(b.cfg.FlushInterval, rm.flush)
	b.goLoop(b.cfg.RetentionCheck, rm.applyRetention)
	b.goLoop(b.cfg.ReplicaLagTime/4, rm.checkISR)
}

func (rm *ReplicaManager) get(topic string, id int32) *Partition {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.partitions[tp{topic, id}]
}

func (rm *ReplicaManager) all() []*Partition {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	out := make([]*Partition, 0, len(rm.partitions))
	for _, p := range rm.partitions {
		out = append(out, p)
	}
	return out
}

func (rm *ReplicaManager) logConfig(t metadata.TopicMeta) storage.Config {
	cfg := rm.b.cfg.Log
	cfg.RetentionMs = t.ConfigInt64("retention.ms", cfg.RetentionMs)
	cfg.RetentionBytes = t.ConfigInt64("retention.bytes", cfg.RetentionBytes)
	cfg.SegmentBytes = t.ConfigInt64("segment.bytes", cfg.SegmentBytes)
	return cfg
}

// reconcile brings local replicas in line with metadata.
func (rm *ReplicaManager) reconcile() {
	// Do not act on a half-replayed metadata log (e.g. right after a
	// restart): we could briefly follow long-gone leaders.
	if !rm.b.metadataReady() {
		return
	}
	self := rm.b.cfg.ID
	desired := map[tp]metadata.PartitionMeta{}
	topics := map[string]metadata.TopicMeta{}
	for _, t := range rm.b.meta.Topics() {
		topics[t.Name] = t
		for _, p := range t.Partitions {
			if slices.Contains(p.Replicas, self) {
				desired[tp{t.Name, p.ID}] = *p
			}
		}
	}

	// Remove replicas that are no longer assigned (topic deleted).
	rm.mu.Lock()
	var removed []*Partition
	for key, p := range rm.partitions {
		if _, ok := desired[key]; !ok {
			removed = append(removed, p)
			delete(rm.partitions, key)
		}
	}
	rm.mu.Unlock()
	for _, p := range removed {
		rm.fetchers.remove(p)
		p.mu.Lock()
		p.role = roleNone
		p.mu.Unlock()
		p.wakeWaiters()
		if err := p.log.Delete(); err != nil {
			rm.b.logger.Warn("delete log failed", "partition", p.String(), "err", err)
		} else {
			rm.b.logger.Info("deleted replica", "partition", p.String())
		}
	}

	// Create or update assigned replicas.
	for key, pm := range desired {
		p := rm.get(key.topic, key.partition)
		if p == nil {
			var err error
			p, err = rm.open(key, topics[key.topic])
			if err != nil {
				rm.b.logger.Error("open replica failed", "partition", key.String(), "err", err)
				continue
			}
		}
		t := topics[key.topic]
		cfg := rm.logConfig(t)
		p.log.SetRetention(cfg.RetentionBytes, cfg.RetentionMs)

		newRole, changed := p.applyMetadata(pm)
		if !changed {
			continue
		}
		rm.b.logger.Info("replica role change", "partition", p.String(), "role", newRole.String(),
			"leader", pm.Leader, "leader_epoch", pm.LeaderEpoch, "isr", pm.ISR)
		switch newRole {
		case roleLeader:
			rm.fetchers.remove(p)
		case roleFollower:
			rm.fetchers.add(p, pm.Leader, pm.LeaderEpoch)
		default:
			rm.fetchers.remove(p)
		}
		p.wakeWaiters() // unblock acks=all waiters / long polls on role change
	}
}

func (rm *ReplicaManager) open(key tp, t metadata.TopicMeta) (*Partition, error) {
	dir := filepath.Join(rm.logsDir, key.String())
	l, err := storage.Open(dir, rm.logConfig(t), rm.b.logger)
	if err != nil {
		return nil, err
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if p, ok := rm.partitions[key]; ok { // raced with another reconcile
		l.Close()
		return p, nil
	}
	p := newPartition(key.topic, key.partition, rm.b.cfg.ID, l, rm.hwCheckpt[key.String()])
	rm.partitions[key] = p
	return p, nil
}

// flush fsyncs logs and checkpoints high-watermarks (so a restarted
// follower knows a safe lower bound of what was committed).
func (rm *ReplicaManager) flush() {
	cp := map[string]int64{}
	for _, p := range rm.all() {
		if err := p.log.Flush(); err != nil {
			rm.b.logger.Warn("flush failed", "partition", p.String(), "err", err)
		}
		cp[p.String()] = p.HighWatermark()
	}
	data, _ := json.Marshal(cp)
	tmp := rm.hwPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err == nil {
		os.Rename(tmp, rm.hwPath)
	}
}

func (rm *ReplicaManager) applyRetention() {
	now := time.Now()
	for _, p := range rm.all() {
		n, err := p.log.ApplyRetention(now, p.HighWatermark())
		if err != nil {
			rm.b.logger.Warn("retention failed", "partition", p.String(), "err", err)
		}
		if n > 0 {
			rm.b.logger.Info("retention", "partition", p.String(), "segments_deleted", n)
		}
	}
}

func (rm *ReplicaManager) close() {
	rm.fetchers.closeAll()
	rm.flush()
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for _, p := range rm.partitions {
		p.close()
	}
	rm.partitions = map[tp]*Partition{}
}
