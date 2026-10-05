package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const epochFileName = "leader-epoch-checkpoint"

// EpochEntry records that leader epoch Epoch started writing at StartOffset.
type EpochEntry struct {
	Epoch       int32
	StartOffset int64
}

// EpochCache is the leader-epoch history of one partition replica:
// "epoch 3 began at offset 120, epoch 5 began at offset 410, ...".
//
// Why it exists: when leadership changes, a follower may hold records the
// new leader never received (they were never committed). Truncating a
// follower to its own high-watermark is NOT safe in all cases (Kafka
// KIP-101). Instead the follower asks the new leader "where did my last
// epoch end on your log?" and truncates exactly there. That answer comes
// from this cache.
type EpochCache struct {
	mu      sync.Mutex
	path    string
	entries []EpochEntry
}

func openEpochCache(dir string) (*EpochCache, error) {
	c := &EpochCache{path: filepath.Join(dir, epochFileName)}
	f, err := os.Open(c.path)
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e EpochEntry
		if _, err := fmt.Sscanf(line, "%d %d", &e.Epoch, &e.StartOffset); err != nil {
			// A torn checkpoint is not fatal: the cache is rebuilt from
			// records as they are appended. Keep what parsed.
			break
		}
		c.entries = append(c.entries, e)
	}
	return c, nil
}

// assign records that epoch starts at offset if epoch is newer than the
// latest known epoch. Must be called before appending the record.
func (c *EpochCache) assign(epoch int32, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.entries); n > 0 && c.entries[n-1].Epoch >= epoch {
		return nil
	}
	c.entries = append(c.entries, EpochEntry{epoch, offset})
	return c.persistLocked()
}

// LatestEpoch returns the newest epoch in the cache, or -1.
func (c *EpochCache) LatestEpoch() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) == 0 {
		return -1
	}
	return c.entries[len(c.entries)-1].Epoch
}

// EndOffsetFor answers OffsetForLeaderEpoch: for the largest epoch <=
// requested, return that epoch and the offset where it ended (the start of
// the following epoch, or logEnd if it is the latest epoch). If the cache
// knows no epoch <= requested it returns (-1, -1).
func (c *EpochCache) EndOffsetFor(requested int32, logEnd int64) (int32, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.entries) - 1; i >= 0; i-- {
		if c.entries[i].Epoch <= requested {
			if i == len(c.entries)-1 {
				return c.entries[i].Epoch, logEnd
			}
			return c.entries[i].Epoch, c.entries[i+1].StartOffset
		}
	}
	return -1, -1
}

// truncateFrom removes epochs that start at or after offset (log truncated
// at the tail).
func (c *EpochCache) truncateFrom(offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.entries)
	for n > 0 && c.entries[n-1].StartOffset >= offset {
		n--
	}
	if n == len(c.entries) {
		return nil
	}
	c.entries = c.entries[:n]
	return c.persistLocked()
}

// Entries returns a copy (for tests and diagnostics).
func (c *EpochCache) Entries() []EpochEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]EpochEntry(nil), c.entries...)
}

// persistLocked writes the checkpoint atomically (temp file + rename).
func (c *EpochCache) persistLocked() error {
	var b strings.Builder
	for _, e := range c.entries {
		fmt.Fprintf(&b, "%d %d\n", e.Epoch, e.StartOffset)
	}
	return writeFileAtomic(c.path, []byte(b.String()))
}

// writeFileAtomic writes data to path via a temp file, fsync and rename so
// readers see either the old or the new content, never a mix.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
