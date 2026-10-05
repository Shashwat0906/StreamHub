package broker

import (
	"testing"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

func req(pid int64, seq int32, n int) *protocol.ProduceRequest {
	return &protocol.ProduceRequest{ProducerID: pid, BaseSequence: seq, Records: make([]storage.Record, n)}
}

func TestSequenceChecks(t *testing.T) {
	ps := map[int64]*producerState{}
	// Unknown producer: accepted.
	if off, code, _ := checkSequence(ps, req(1, 0, 3)); off != -1 || code != protocol.ErrNone {
		t.Fatalf("new producer: %d %s", off, code)
	}
	recordSequence(ps, 1, 0, 3, 100) // seqs 0..2 at offsets 100..102
	cases := []struct {
		seq   int32
		n     int
		off   int64
		code  protocol.ErrorCode
		label string
	}{
		{3, 2, -1, protocol.ErrNone, "next in order"},
		{0, 3, 100, protocol.ErrNone, "exact retry -> original offset"},
		{1, 2, 101, protocol.ErrNone, "retry of a suffix"},
		{5, 1, -1, protocol.ErrOutOfOrderSequence, "gap"},
		{2, 3, -1, protocol.ErrOutOfOrderSequence, "overlaps the end"},
	}
	for _, c := range cases {
		off, code, _ := checkSequence(ps, req(1, c.seq, c.n))
		if off != c.off || code != c.code {
			t.Fatalf("%s: got (%d, %s) want (%d, %s)", c.label, off, code, c.off, c.code)
		}
	}
}

func TestSequenceWindowEviction(t *testing.T) {
	ps := map[int64]*producerState{}
	recordSequence(ps, 9, 0, int32(recentSeqWindow+10), 0)
	// Very old sequence: known to be written, but its offset was evicted.
	if _, code, _ := checkSequence(ps, req(9, 0, 1)); code != protocol.ErrDuplicateSequence {
		t.Fatalf("old duplicate: %s", code)
	}
}

func TestProducerStateRebuiltFromLog(t *testing.T) {
	l, err := storage.Open(t.TempDir(), storage.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Append([]storage.Record{{ProducerID: 4, Sequence: 0}, {ProducerID: 4, Sequence: 1}, {ProducerID: -1, Sequence: -1}})
	ps := rebuildProducerState(l)
	if st := ps[4]; st == nil || st.lastSeq != 1 || st.offsets[1] != 1 {
		t.Fatalf("rebuilt state %+v", st)
	}
	if off, code, _ := checkSequence(ps, req(4, 0, 2)); off != 0 || code != protocol.ErrNone {
		t.Fatalf("dedupe after rebuild: %d %s", off, code)
	}
}
