package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

// indexEntrySize is the size of one sparse-index entry on disk:
// relativeOffset(uint32) + position(uint32).
const indexEntrySize = 8

// indexEntry maps an offset (relative to the segment base) to the byte
// position of that record inside the segment's .log file.
type indexEntry struct {
	relOffset uint32
	position  uint32
}

// index is a sparse offset index for one segment.
//
// It is "sparse" because it does not contain every offset: one entry is
// added roughly every IndexIntervalBytes of log data. To find an offset we
// binary-search for the closest entry at or before it and scan the log
// forward from there. This keeps the index tiny (a few KB per GB of log)
// while bounding the scan to roughly one interval.
//
// Entries are kept in memory (they are small) and appended to a file so
// they do not have to be rebuilt on every restart.
type index struct {
	file    *os.File
	entries []indexEntry
}

func openIndex(path string) (*index, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &index{file: f}, nil
}

// load reads entries from disk. It returns an error if the file is not
// well formed so the caller can decide to rebuild it from the log.
func (ix *index) load(baseOffset int64, logSize int64) error {
	data, err := os.ReadFile(ix.file.Name())
	if err != nil {
		return err
	}
	if len(data)%indexEntrySize != 0 {
		return fmt.Errorf("index size %d not a multiple of %d", len(data), indexEntrySize)
	}
	ix.entries = ix.entries[:0]
	var prev indexEntry
	for i := 0; i < len(data); i += indexEntrySize {
		e := indexEntry{
			relOffset: binary.BigEndian.Uint32(data[i:]),
			position:  binary.BigEndian.Uint32(data[i+4:]),
		}
		// Entries must be strictly increasing and point inside the log.
		if i > 0 && (e.relOffset <= prev.relOffset || e.position <= prev.position) {
			return fmt.Errorf("index not monotonic at entry %d", i/indexEntrySize)
		}
		if int64(e.position) >= logSize {
			return fmt.Errorf("index entry %d points past end of log", i/indexEntrySize)
		}
		ix.entries = append(ix.entries, e)
		prev = e
	}
	return nil
}

// append adds an entry to memory and disk.
func (ix *index) append(relOffset uint32, position uint32) error {
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:], relOffset)
	binary.BigEndian.PutUint32(buf[4:], position)
	if _, err := ix.file.Write(buf[:]); err != nil {
		return err
	}
	ix.entries = append(ix.entries, indexEntry{relOffset, position})
	return nil
}

// lookup returns the byte position of the greatest indexed offset that is
// <= relOffset, or 0 (start of segment) if there is none.
func (ix *index) lookup(relOffset uint32) uint32 {
	i := sort.Search(len(ix.entries), func(i int) bool {
		return ix.entries[i].relOffset > relOffset
	})
	if i == 0 {
		return 0
	}
	return ix.entries[i-1].position
}

// truncateAfterPosition drops entries pointing at or beyond pos. Used when
// the log is truncated.
func (ix *index) truncateAfterPosition(pos uint32) error {
	i := sort.Search(len(ix.entries), func(i int) bool {
		return ix.entries[i].position >= pos
	})
	ix.entries = ix.entries[:i]
	if err := ix.file.Truncate(int64(i * indexEntrySize)); err != nil {
		return err
	}
	_, err := ix.file.Seek(int64(i*indexEntrySize), 0)
	return err
}

// reset clears the index (memory and disk) before a rebuild.
func (ix *index) reset() error {
	ix.entries = ix.entries[:0]
	if err := ix.file.Truncate(0); err != nil {
		return err
	}
	_, err := ix.file.Seek(0, 0)
	return err
}

// seekEnd positions the file for appends after a successful load.
func (ix *index) seekEnd() error {
	_, err := ix.file.Seek(0, 2)
	return err
}

func (ix *index) sync() error  { return ix.file.Sync() }
func (ix *index) close() error { return ix.file.Close() }
