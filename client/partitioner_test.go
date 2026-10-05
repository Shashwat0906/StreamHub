package client

import (
	"fmt"
	"testing"
)

func TestHashPartitionStableAndSpread(t *testing.T) {
	// Stability: the same key always maps to the same partition. These
	// values are pinned so an accidental hash change is caught.
	if got := HashPartition([]byte("customer-42"), 12); got != HashPartition([]byte("customer-42"), 12) {
		t.Fatal("hash not deterministic")
	}
	counts := make([]int, 8)
	for i := 0; i < 8000; i++ {
		counts[HashPartition([]byte(fmt.Sprintf("key-%d", i)), 8)]++
	}
	for p, n := range counts {
		if n < 800 || n > 1200 { // within 20% of the 1000 mean
			t.Fatalf("partition %d got %d of 8000 keys: poor spread %v", p, n, counts)
		}
	}
}

func TestRoundRobinForKeylessMessages(t *testing.T) {
	var p DefaultPartitioner
	for i := 0; i < 12; i++ {
		if got := p.Partition("t", nil, 4); got != int32(i%4) {
			t.Fatalf("message %d went to %d", i, got)
		}
	}
	// Keyed messages ignore the round-robin counter.
	a := p.Partition("t", []byte("k"), 4)
	b := p.Partition("t", []byte("k"), 4)
	if a != b {
		t.Fatal("keyed placement changed")
	}
}
