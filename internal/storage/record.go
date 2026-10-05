// Package storage implements StreamHub's per-partition commit log:
// an append-only sequence of records split across segment files, with a
// sparse offset index per segment, a CRC on every record, crash recovery,
// retention and a leader-epoch cache used for safe replica truncation.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Record is one message stored in a partition.
//
// Offset, LeaderEpoch, ProducerID and Sequence are assigned/validated by
// the broker; Key, Value and Timestamp come from the producer.
type Record struct {
	Offset      int64
	Timestamp   int64 // unix milliseconds
	LeaderEpoch int32 // epoch of the leader that appended this record
	ProducerID  int64 // -1 when the producer is not idempotent
	Sequence    int32 // per-(producer, partition) sequence, -1 when unused
	Key         []byte
	Value       []byte
}

const (
	// lengthFieldSize is the size of the leading length prefix.
	lengthFieldSize = 4
	// recordOverhead is every fixed-size field of an encoded record,
	// including the length prefix:
	// length(4) crc(4) offset(8) ts(8) epoch(4) pid(8) seq(4) keyLen(4) valLen(4)
	recordOverhead = 4 + 4 + 8 + 8 + 4 + 8 + 4 + 4 + 4
	// MaxRecordSize bounds a single encoded record; anything larger in a
	// length field is treated as corruption during recovery.
	MaxRecordSize = 16 << 20
)

var (
	// ErrCorrupt is returned when a record fails validation.
	ErrCorrupt = errors.New("storage: corrupt record")
	// ErrTruncated is returned when the buffer ends mid-record.
	ErrTruncated = errors.New("storage: truncated record")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// EncodedSize returns the number of bytes Encode will produce.
func (r *Record) EncodedSize() int {
	return recordOverhead + len(r.Key) + len(r.Value)
}

// AppendEncoded appends the on-disk encoding of r to dst and returns it.
// Layout (big-endian): length crc offset timestamp epoch producerId
// sequence keyLen key valueLen value. The CRC covers everything after
// the crc field, so a torn or bit-flipped write is always detected.
func (r *Record) AppendEncoded(dst []byte) []byte {
	size := r.EncodedSize()
	start := len(dst)
	dst = append(dst, make([]byte, size)...)
	b := dst[start:]

	binary.BigEndian.PutUint32(b[0:], uint32(size-lengthFieldSize))
	// b[4:8] is the CRC, filled in last.
	binary.BigEndian.PutUint64(b[8:], uint64(r.Offset))
	binary.BigEndian.PutUint64(b[16:], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(b[24:], uint32(r.LeaderEpoch))
	binary.BigEndian.PutUint64(b[28:], uint64(r.ProducerID))
	binary.BigEndian.PutUint32(b[36:], uint32(r.Sequence))
	pos := 40
	if r.Key == nil {
		binary.BigEndian.PutUint32(b[pos:], ^uint32(0)) // -1 = null key
	} else {
		binary.BigEndian.PutUint32(b[pos:], uint32(len(r.Key)))
	}
	pos += 4
	pos += copy(b[pos:], r.Key)
	binary.BigEndian.PutUint32(b[pos:], uint32(len(r.Value)))
	pos += 4
	copy(b[pos:], r.Value)

	binary.BigEndian.PutUint32(b[4:], crc32.Checksum(b[8:], castagnoli))
	return dst
}

// DecodeRecord decodes one record from the start of b. It returns the
// record and the number of bytes consumed. Key and Value alias b.
func DecodeRecord(b []byte) (Record, int, error) {
	var r Record
	if len(b) < lengthFieldSize {
		return r, 0, ErrTruncated
	}
	n := int(binary.BigEndian.Uint32(b))
	if n < recordOverhead-lengthFieldSize || n > MaxRecordSize {
		return r, 0, fmt.Errorf("%w: bad length %d", ErrCorrupt, n)
	}
	total := n + lengthFieldSize
	if len(b) < total {
		return r, 0, ErrTruncated
	}
	b = b[:total]
	if crc32.Checksum(b[8:], castagnoli) != binary.BigEndian.Uint32(b[4:]) {
		return r, 0, fmt.Errorf("%w: crc mismatch", ErrCorrupt)
	}
	r.Offset = int64(binary.BigEndian.Uint64(b[8:]))
	r.Timestamp = int64(binary.BigEndian.Uint64(b[16:]))
	r.LeaderEpoch = int32(binary.BigEndian.Uint32(b[24:]))
	r.ProducerID = int64(binary.BigEndian.Uint64(b[28:]))
	r.Sequence = int32(binary.BigEndian.Uint32(b[36:]))
	pos := 40
	keyLen := int32(binary.BigEndian.Uint32(b[pos:]))
	pos += 4
	if keyLen >= 0 {
		if pos+int(keyLen)+4 > total {
			return r, 0, fmt.Errorf("%w: key overflows record", ErrCorrupt)
		}
		r.Key = b[pos : pos+int(keyLen)]
		pos += int(keyLen)
	} else if keyLen != -1 {
		return r, 0, fmt.Errorf("%w: bad key length", ErrCorrupt)
	}
	valLen := int(binary.BigEndian.Uint32(b[pos:]))
	pos += 4
	if pos+valLen != total {
		return r, 0, fmt.Errorf("%w: value length mismatch", ErrCorrupt)
	}
	r.Value = b[pos:total]
	return r, total, nil
}
