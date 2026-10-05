package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config controls one partition log.
type Config struct {
	// SegmentBytes is the size at which the active segment is rolled.
	SegmentBytes int64
	// IndexIntervalBytes is how much log data separates two index entries.
	IndexIntervalBytes int64
	// RetentionBytes deletes old segments once the log exceeds this size
	// (-1 = unlimited).
	RetentionBytes int64
	// RetentionMs deletes segments whose newest record is older than this
	// (-1 = unlimited).
	RetentionMs int64
	// FsyncEveryAppend calls fsync after every append. Off by default:
	// like Kafka, StreamHub relies on replication for durability and
	// fsyncs on segment roll and on a periodic flush.
	FsyncEveryAppend bool
}

// DefaultConfig returns production-ish defaults.
func DefaultConfig() Config {
	return Config{
		SegmentBytes:       64 << 20,
		IndexIntervalBytes: 4 << 10,
		RetentionBytes:     -1,
		RetentionMs:        7 * 24 * 3600 * 1000,
	}
}

var (
	// ErrOffsetOutOfRange is returned by Read for offsets outside
	// [StartOffset, EndOffset].
	ErrOffsetOutOfRange = errors.New("storage: offset out of range")
	// ErrNonSequential is returned when a replicated append does not start
	// at the log end offset.
	ErrNonSequential = errors.New("storage: non-sequential append")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("storage: log closed")
)

// Log is the commit log of one partition replica.
//
// All writes are serialized by mu. Reads take the read lock and use
// positional reads (pread), so many consumers can read concurrently.
type Log struct {
	mu       sync.RWMutex
	dir      string
	cfg      Config
	segments []*segment // sorted by baseOffset; last is active
	epochs   *EpochCache
	closed   bool
	logger   *slog.Logger

	// RecoveredTruncatedBytes is how many corrupt tail bytes were removed
	// when the log was opened (0 after a clean shutdown).
	RecoveredTruncatedBytes int64
}

// Open opens (or creates) the log stored in dir and runs crash recovery.
func Open(dir string, cfg Config, logger *slog.Logger) (*Log, error) {
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = DefaultConfig().SegmentBytes
	}
	if cfg.IndexIntervalBytes <= 0 {
		cfg.IndexIntervalBytes = DefaultConfig().IndexIntervalBytes
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{dir: dir, cfg: cfg, logger: logger.With("log", filepath.Base(dir))}

	epochs, err := openEpochCache(dir)
	if err != nil {
		return nil, err
	}
	l.epochs = epochs

	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, err
	}
	for i, base := range bases {
		last := i == len(bases)-1
		seg, truncated, err := openSegment(dir, base, cfg.IndexIntervalBytes, last)
		if err != nil {
			l.closeSegments()
			return nil, fmt.Errorf("open segment %d: %w", base, err)
		}
		if truncated > 0 {
			l.RecoveredTruncatedBytes += truncated
			l.logger.Warn("truncated corrupt log tail during recovery",
				"segment", base, "bytes", truncated, "next_offset", seg.nextOffset)
		}
		l.segments = append(l.segments, seg)
	}
	if len(l.segments) == 0 {
		seg, err := createSegment(dir, 0, cfg.IndexIntervalBytes)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, seg)
	}
	// Epochs that start beyond the recovered end refer to lost data.
	if err := l.epochs.truncateFrom(l.endOffsetLocked()); err != nil {
		return nil, err
	}
	return l, nil
}

func listSegmentBases(dir string) ([]int64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, logSuffix) {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, logSuffix), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func (l *Log) active() *segment { return l.segments[len(l.segments)-1] }

func (l *Log) endOffsetLocked() int64 { return l.active().nextOffset }

// EndOffset is the log end offset (LEO): the offset the next record gets.
func (l *Log) EndOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.endOffsetLocked()
}

// StartOffset is the first offset still stored (moves forward with retention).
func (l *Log) StartOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset
}

// Size returns the total bytes of all segment files.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var n int64
	for _, s := range l.segments {
		n += s.size
	}
	return n
}

// SegmentCount returns the number of segments (for tests/metrics).
func (l *Log) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// Epochs exposes the leader-epoch cache.
func (l *Log) Epochs() *EpochCache { return l.epochs }

// Dir returns the directory of the log.
func (l *Log) Dir() string { return l.dir }

// Append assigns consecutive offsets starting at the log end offset to
// recs (mutating them) and appends them. It returns the first offset.
func (l *Log) Append(recs []Record) (int64, error) {
	if len(recs) == 0 {
		return -1, errors.New("storage: empty append")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return -1, ErrClosed
	}
	base := l.endOffsetLocked()
	for i := range recs {
		recs[i].Offset = base + int64(i)
	}
	return base, l.appendLocked(recs)
}

// AppendReplicated appends records that already carry offsets assigned by
// the partition leader. The first offset must equal the log end offset.
func (l *Log) AppendReplicated(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if recs[0].Offset != l.endOffsetLocked() {
		return fmt.Errorf("%w: got %d, log end %d", ErrNonSequential, recs[0].Offset, l.endOffsetLocked())
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].Offset <= recs[i-1].Offset {
			return fmt.Errorf("%w: offsets not increasing", ErrNonSequential)
		}
	}
	return l.appendLocked(recs)
}

func (l *Log) appendLocked(recs []Record) error {
	// Record epoch boundaries before writing the data, so the epoch cache
	// never misses the start of an epoch that made it to disk.
	for i := range recs {
		if err := l.epochs.assign(recs[i].LeaderEpoch, recs[i].Offset); err != nil {
			return err
		}
	}
	size := 0
	for i := range recs {
		size += recs[i].EncodedSize()
	}
	act := l.active()
	lastRel := recs[len(recs)-1].Offset - act.baseOffset
	if act.size > 0 && (act.size+int64(size) > l.cfg.SegmentBytes || lastRel > 1<<31) {
		if err := l.rollLocked(recs[0].Offset); err != nil {
			return err
		}
		act = l.active()
	}
	if err := act.append(recs); err != nil {
		return err
	}
	if l.cfg.FsyncEveryAppend {
		return act.sync()
	}
	return nil
}

// rollLocked seals the active segment (fsync) and starts a new one.
func (l *Log) rollLocked(base int64) error {
	if err := l.active().sync(); err != nil {
		return err
	}
	seg, err := createSegment(l.dir, base, l.cfg.IndexIntervalBytes)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.logger.Debug("rolled segment", "base_offset", base)
	return nil
}

// Read returns records in [offset, maxOffset) up to roughly maxBytes.
// maxOffset is how a leader hides records above the high-watermark from
// consumers; pass -1 for "no limit". Reading exactly at the end offset
// returns no records and no error (the consumer is caught up).
func (l *Log) Read(offset, maxOffset int64, maxBytes int) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	end := l.endOffsetLocked()
	if offset < l.segments[0].baseOffset || offset > end {
		return nil, fmt.Errorf("%w: %d not in [%d, %d]", ErrOffsetOutOfRange, offset, l.segments[0].baseOffset, end)
	}
	if maxOffset < 0 || maxOffset > end {
		maxOffset = end
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	// Find the segment that holds offset: the last with baseOffset <= offset.
	i := sort.Search(len(l.segments), func(i int) bool { return l.segments[i].baseOffset > offset }) - 1
	if i < 0 {
		i = 0
	}
	var out []Record
	remaining := maxBytes
	for ; i < len(l.segments) && offset < maxOffset; i++ {
		recs, n, err := l.segments[i].read(offset, maxOffset, remaining)
		if err != nil {
			return out, err
		}
		if len(recs) > 0 {
			out = append(out, recs...)
			offset = recs[len(recs)-1].Offset + 1
			remaining -= n
			if remaining <= 0 {
				break
			}
		}
	}
	return out, nil
}

// TruncateTo removes every record with Offset >= offset. Used by followers
// to discard a divergent tail after a leader change.
func (l *Log) TruncateTo(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if offset >= l.endOffsetLocked() {
		return nil
	}
	if offset < l.segments[0].baseOffset {
		offset = l.segments[0].baseOffset
	}
	// Drop whole segments that start at or after offset (keep at least one).
	for len(l.segments) > 1 && l.active().baseOffset >= offset {
		if err := l.active().remove(); err != nil {
			return err
		}
		l.segments = l.segments[:len(l.segments)-1]
	}
	if err := l.active().truncateTo(offset); err != nil {
		return err
	}
	l.logger.Info("truncated log", "to_offset", offset)
	return l.epochs.truncateFrom(offset)
}

// ResetTo deletes all data and restarts the log empty at offset. A
// follower uses this when the leader's log start has moved past the
// follower's end (the data it would need was deleted by retention).
func (l *Log) ResetTo(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	for _, s := range l.segments {
		if err := s.remove(); err != nil {
			return err
		}
	}
	l.segments = nil
	seg, err := createSegment(l.dir, offset, l.cfg.IndexIntervalBytes)
	if err != nil {
		return err
	}
	l.segments = []*segment{seg}
	return l.epochs.truncateFrom(0)
}

// ApplyRetention deletes sealed segments that violate retention.bytes or
// retention.ms. The active segment is never deleted. maxDeletable is an
// upper bound offset (exclusive): segments that contain offsets >=
// maxDeletable are kept (the replica manager passes the high-watermark so
// we never delete data a follower or consumer has not had the chance to
// see). It returns the number of segments deleted.
func (l *Log) ApplyRetention(now time.Time, maxDeletable int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	var total int64
	for _, s := range l.segments {
		total += s.size
	}
	deleted := 0
	nowMs := now.UnixMilli()
	for len(l.segments) > 1 {
		s := l.segments[0]
		if maxDeletable >= 0 && s.nextOffset > maxDeletable {
			break
		}
		bySize := l.cfg.RetentionBytes >= 0 && total-s.size >= l.cfg.RetentionBytes
		byTime := l.cfg.RetentionMs >= 0 && s.maxTimestamp > 0 && nowMs-s.maxTimestamp > l.cfg.RetentionMs
		if !bySize && !byTime {
			break
		}
		if err := s.remove(); err != nil {
			return deleted, err
		}
		total -= s.size
		l.segments = l.segments[1:]
		deleted++
		l.logger.Info("retention deleted segment", "base_offset", s.baseOffset,
			"by_size", bySize, "by_time", byTime, "new_start_offset", l.segments[0].baseOffset)
	}
	return deleted, nil
}

// SetRetention updates retention settings at runtime.
func (l *Log) SetRetention(bytes, ms int64) {
	l.mu.Lock()
	l.cfg.RetentionBytes = bytes
	l.cfg.RetentionMs = ms
	l.mu.Unlock()
}

// Flush fsyncs the active segment.
func (l *Log) Flush() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil
	}
	return l.active().sync()
}

// Close flushes and closes all files.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	err := l.active().sync()
	l.closeSegments()
	return err
}

func (l *Log) closeSegments() {
	for _, s := range l.segments {
		s.close()
	}
}

// Delete closes the log and removes its directory.
func (l *Log) Delete() error {
	l.Close()
	return os.RemoveAll(l.dir)
}
