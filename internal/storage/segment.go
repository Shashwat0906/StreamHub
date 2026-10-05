package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	logSuffix   = ".log"
	indexSuffix = ".index"
)

// segment is one contiguous range of offsets [baseOffset, nextOffset)
// stored in "<baseOffset>.log" plus its sparse index "<baseOffset>.index".
//
// Only the newest segment of a log (the "active" segment) is ever written.
// Older segments are immutable until retention deletes them whole.
type segment struct {
	baseOffset   int64
	nextOffset   int64 // offset the next appended record will get
	size         int64 // bytes in the .log file
	maxTimestamp int64 // newest record timestamp, used by time retention

	log   *os.File
	index *index

	indexInterval   int64
	bytesSinceIndex int64
}

func segmentFileName(dir string, base int64, suffix string) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, suffix))
}

// createSegment creates a new, empty segment starting at base.
func createSegment(dir string, base int64, indexInterval int64) (*segment, error) {
	lf, err := os.OpenFile(segmentFileName(dir, base, logSuffix), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	ix, err := openIndex(segmentFileName(dir, base, indexSuffix))
	if err != nil {
		lf.Close()
		return nil, err
	}
	if err := ix.reset(); err != nil {
		lf.Close()
		ix.close()
		return nil, err
	}
	return &segment{
		baseOffset:    base,
		nextOffset:    base,
		log:           lf,
		index:         ix,
		indexInterval: indexInterval,
	}, nil
}

// openSegment opens an existing segment.
//
// If recover is true (used for the newest segment, the only one that can
// have been mid-write during a crash) the whole file is scanned, every
// record's CRC is checked, any corrupt or partially written tail is cut off
// and the index is rebuilt. For older, sealed segments only the index is
// validated (and rebuilt if invalid), and the tail after the last index
// entry is scanned to recover nextOffset and maxTimestamp.
func openSegment(dir string, base int64, indexInterval int64, recover bool) (*segment, int64, error) {
	lf, err := os.OpenFile(segmentFileName(dir, base, logSuffix), os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, err
	}
	ix, err := openIndex(segmentFileName(dir, base, indexSuffix))
	if err != nil {
		lf.Close()
		return nil, 0, err
	}
	st, err := lf.Stat()
	if err != nil {
		lf.Close()
		ix.close()
		return nil, 0, err
	}
	s := &segment{
		baseOffset:    base,
		nextOffset:    base,
		size:          st.Size(),
		log:           lf,
		index:         ix,
		indexInterval: indexInterval,
	}

	var truncated int64
	if !recover {
		if err := ix.load(base, s.size); err == nil {
			// Scan only the tail after the last index entry.
			var from uint32
			if n := len(ix.entries); n > 0 {
				from = ix.entries[n-1].position
			}
			if _, err := s.scan(int64(from), false); err == nil {
				if err := ix.seekEnd(); err != nil {
					s.close()
					return nil, 0, err
				}
				return s, 0, nil
			}
		}
		// Fall through: index or tail invalid, do a full recovery scan.
	}
	truncated, err = s.recoverFull()
	if err != nil {
		s.close()
		return nil, 0, err
	}
	return s, truncated, nil
}

// recoverFull rebuilds the index from scratch while validating every
// record, truncating the file at the first invalid record. It returns the
// number of bytes truncated.
func (s *segment) recoverFull() (int64, error) {
	if err := s.index.reset(); err != nil {
		return 0, err
	}
	s.nextOffset = s.baseOffset
	s.maxTimestamp = 0
	s.bytesSinceIndex = 0
	good, err := s.scan(0, true)
	if err != nil {
		return 0, err
	}
	truncated := s.size - good
	if truncated > 0 {
		if err := s.log.Truncate(good); err != nil {
			return 0, err
		}
		s.size = good
	}
	return truncated, nil
}

// scan reads records from byte position from to the end of the file,
// updating nextOffset and maxTimestamp. If rebuildIndex is true it also
// appends index entries. It returns the byte position just after the last
// valid record. Invalid data stops the scan without an error; I/O errors
// are returned.
func (s *segment) scan(from int64, rebuildIndex bool) (int64, error) {
	r := bufio.NewReaderSize(io.NewSectionReader(s.log, from, s.size-from), 64<<10)
	pos := from
	lastOffset := s.nextOffset - 1
	for {
		rec, n, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, ErrTruncated) || errors.Is(err, ErrCorrupt) {
				return pos, nil
			}
			return pos, err
		}
		// Offsets must strictly increase and belong to this segment.
		if rec.Offset < s.baseOffset || rec.Offset <= lastOffset || rec.Offset-s.baseOffset > math.MaxUint32 {
			return pos, nil
		}
		if rebuildIndex {
			if s.bytesSinceIndex >= s.indexInterval {
				if err := s.index.append(uint32(rec.Offset-s.baseOffset), uint32(pos)); err != nil {
					return pos, err
				}
				s.bytesSinceIndex = 0
			}
			s.bytesSinceIndex += int64(n)
		}
		lastOffset = rec.Offset
		s.nextOffset = rec.Offset + 1
		if rec.Timestamp > s.maxTimestamp {
			s.maxTimestamp = rec.Timestamp
		}
		pos += int64(n)
	}
}

// readRecord reads exactly one encoded record from r.
func readRecord(r *bufio.Reader) (Record, int, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, 0, ErrTruncated
		}
		return Record{}, 0, err // io.EOF on a clean end
	}
	n := int(binary.BigEndian.Uint32(lenBuf[:]))
	if n < recordOverhead-lengthFieldSize || n > MaxRecordSize {
		return Record{}, 0, ErrCorrupt
	}
	buf := make([]byte, n+lengthFieldSize)
	copy(buf, lenBuf[:])
	if _, err := io.ReadFull(r, buf[lengthFieldSize:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return Record{}, 0, ErrTruncated
		}
		return Record{}, 0, err
	}
	rec, used, err := DecodeRecord(buf)
	return rec, used, err
}

// append writes already-offset-assigned records to the segment. Records
// must have offsets >= nextOffset and strictly increasing.
func (s *segment) append(recs []Record) error {
	buf := make([]byte, 0, 256*len(recs))
	pos := s.size
	for i := range recs {
		r := &recs[i]
		if s.bytesSinceIndex >= s.indexInterval {
			if err := s.index.append(uint32(r.Offset-s.baseOffset), uint32(pos)); err != nil {
				return err
			}
			s.bytesSinceIndex = 0
		}
		before := len(buf)
		buf = r.AppendEncoded(buf)
		n := int64(len(buf) - before)
		s.bytesSinceIndex += n
		pos += n
	}
	if _, err := s.log.WriteAt(buf, s.size); err != nil {
		return err
	}
	s.size = pos
	last := recs[len(recs)-1]
	s.nextOffset = last.Offset + 1
	for i := range recs {
		if recs[i].Timestamp > s.maxTimestamp {
			s.maxTimestamp = recs[i].Timestamp
		}
	}
	return nil
}

// read returns records with offset in [offset, maxOffset) starting from
// offset, stopping once maxBytes of encoded data has been collected. At
// least one record is returned if any is available (so a single record
// larger than maxBytes can still be consumed).
func (s *segment) read(offset, maxOffset int64, maxBytes int) ([]Record, int, error) {
	if offset < s.baseOffset {
		offset = s.baseOffset
	}
	if offset >= s.nextOffset || offset >= maxOffset {
		return nil, 0, nil
	}
	start := int64(s.index.lookup(uint32(offset - s.baseOffset)))
	r := bufio.NewReaderSize(io.NewSectionReader(s.log, start, s.size-start), 64<<10)
	var out []Record
	total := 0
	for {
		rec, n, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, total, nil
			}
			return out, total, fmt.Errorf("read segment %d: %w", s.baseOffset, err)
		}
		if rec.Offset < offset {
			continue
		}
		if rec.Offset >= maxOffset {
			return out, total, nil
		}
		if len(out) > 0 && total+n > maxBytes {
			return out, total, nil
		}
		out = append(out, rec)
		total += n
		if total >= maxBytes {
			return out, total, nil
		}
	}
}

// positionOf returns the byte position of the first record with
// Offset >= offset (or the segment size if none).
func (s *segment) positionOf(offset int64) (int64, error) {
	if offset <= s.baseOffset {
		return 0, nil
	}
	start := int64(s.index.lookup(uint32(offset - s.baseOffset)))
	r := bufio.NewReaderSize(io.NewSectionReader(s.log, start, s.size-start), 64<<10)
	pos := start
	for {
		rec, n, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return s.size, nil
			}
			return 0, err
		}
		if rec.Offset >= offset {
			return pos, nil
		}
		pos += int64(n)
	}
}

// truncateTo removes all records with Offset >= offset. Truncation only
// happens on leader changes, so we keep it simple: cut the file and rebuild
// the segment's index and stats with a full scan.
func (s *segment) truncateTo(offset int64) error {
	pos, err := s.positionOf(offset)
	if err != nil {
		return err
	}
	if err := s.log.Truncate(pos); err != nil {
		return err
	}
	s.size = pos
	_, err = s.recoverFull()
	return err
}

func (s *segment) sync() error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	return s.index.sync()
}

func (s *segment) close() error {
	err1 := s.log.Close()
	err2 := s.index.close()
	if err1 != nil {
		return err1
	}
	return err2
}

// remove closes and deletes both files.
func (s *segment) remove() error {
	s.close()
	err1 := os.Remove(s.log.Name())
	err2 := os.Remove(s.index.file.Name())
	if err1 != nil && !os.IsNotExist(err1) {
		return err1
	}
	if err2 != nil && !os.IsNotExist(err2) {
		return err2
	}
	return nil
}
