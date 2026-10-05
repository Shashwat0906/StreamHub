package client

import (
	"hash/fnv"
	"sync/atomic"
)

// Partitioner chooses a partition for a message with no explicit partition.
type Partitioner interface {
	Partition(topic string, key []byte, numPartitions int) int32
}

// DefaultPartitioner sends keyed messages to hash(key) % n, so every
// message with the same key lands in the same partition (and therefore
// stays ordered), and spreads keyless messages round-robin.
type DefaultPartitioner struct {
	counter atomic.Uint64
}

// Partition implements Partitioner.
func (p *DefaultPartitioner) Partition(topic string, key []byte, n int) int32 {
	if n <= 0 {
		return 0
	}
	if key != nil {
		return HashPartition(key, n)
	}
	return int32((p.counter.Add(1) - 1) % uint64(n))
}

// HashPartition is FNV-1a over the key, masked to a positive int, mod n.
// It is stable across processes and versions, which is what matters:
// producers written in different places must agree on key placement.
func HashPartition(key []byte, n int) int32 {
	h := fnv.New32a()
	h.Write(key)
	return int32((h.Sum32() & 0x7fffffff) % uint32(n))
}
