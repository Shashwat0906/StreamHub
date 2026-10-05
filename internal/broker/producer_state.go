package broker

import (
	"fmt"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

// recentSeqWindow is how many recent sequence numbers (and their offsets)
// we remember per producer to answer retries of already-written batches.
const recentSeqWindow = 1024

// producerState tracks the idempotent producer protocol for one producer
// on one partition.
//
// The producer numbers its records 0,1,2,... per partition. A batch is
// accepted only if it starts exactly at lastSeq+1. A batch whose records
// are all <= lastSeq is a retry of something already written: we reply
// with the original offset instead of writing it again. That turns
// "at-least-once with retries" into "no duplicates" for a single producer
// session on a single partition.
type producerState struct {
	lastSeq int32
	// offsets maps recent sequence numbers to the offset they were written
	// at, so a duplicate batch can be acknowledged with its real offset.
	offsets map[int32]int64
	order   []int32 // FIFO of keys in offsets, for eviction
}

func (s *producerState) remember(seq int32, offset int64) {
	if _, ok := s.offsets[seq]; ok {
		return
	}
	s.offsets[seq] = offset
	s.order = append(s.order, seq)
	if len(s.order) > recentSeqWindow {
		delete(s.offsets, s.order[0])
		s.order = s.order[1:]
	}
}

// checkSequence returns (originalBaseOffset >= 0, NONE) for a duplicate,
// (-1, NONE) for a new in-order batch, or an error code.
func checkSequence(producers map[int64]*producerState, req *protocol.ProduceRequest) (int64, protocol.ErrorCode, string) {
	st, ok := producers[req.ProducerID]
	if !ok {
		// Unknown producer (new, or its records were deleted by retention):
		// accept whatever sequence it starts with.
		return -1, protocol.ErrNone, ""
	}
	n := int32(len(req.Records))
	last := req.BaseSequence + n - 1
	switch {
	case req.BaseSequence == st.lastSeq+1:
		return -1, protocol.ErrNone, ""
	case last <= st.lastSeq:
		if off, ok := st.offsets[req.BaseSequence]; ok {
			return off, protocol.ErrNone, ""
		}
		return -1, protocol.ErrDuplicateSequence, fmt.Sprintf("seq %d already written", req.BaseSequence)
	default:
		return -1, protocol.ErrOutOfOrderSequence,
			fmt.Sprintf("expected seq %d, got %d", st.lastSeq+1, req.BaseSequence)
	}
}

// recordSequence updates state after a successful leader append.
func recordSequence(producers map[int64]*producerState, pid int64, baseSeq, count int32, baseOffset int64) {
	for i := int32(0); i < count; i++ {
		observeRecord(producers, pid, baseSeq+i, baseOffset+int64(i))
	}
}

func observeRecord(producers map[int64]*producerState, pid int64, seq int32, offset int64) {
	if pid < 0 || seq < 0 {
		return
	}
	st, ok := producers[pid]
	if !ok {
		st = &producerState{lastSeq: -1, offsets: map[int32]int64{}}
		producers[pid] = st
	}
	if seq > st.lastSeq {
		st.lastSeq = seq
	}
	st.remember(seq, offset)
}

// rebuildProducerState scans the log to restore idempotence state after a
// restart. Records carry producerId and sequence, so followers that become
// leader have the same state as the old leader.
//
// Limitation: this is a full scan, O(log size) at partition open. Kafka
// avoids it with periodic producer-state snapshots; we accept the cost.
func rebuildProducerState(log *storage.Log) map[int64]*producerState {
	producers := map[int64]*producerState{}
	off := log.StartOffset()
	for {
		recs, err := log.Read(off, -1, 4<<20)
		if err != nil || len(recs) == 0 {
			return producers
		}
		for i := range recs {
			observeRecord(producers, recs[i].ProducerID, recs[i].Sequence, recs[i].Offset)
		}
		off = recs[len(recs)-1].Offset + 1
	}
}

// truncateProducerState drops knowledge of records at or above offset
// (after a log truncation) by rebuilding from the log.
func (p *Partition) rebuildProducersLocked() {
	p.producers = rebuildProducerState(p.log)
}
