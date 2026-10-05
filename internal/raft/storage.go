package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Entry is one Raft log entry. Data == nil is a leader no-op.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// hardState must be durable before answering any RPC (Raft figure 2).
type hardState struct {
	Term     uint64 `json:"term"`
	VotedFor int32  `json:"voted_for"`
}

// diskLog is the persistent Raft log: an append-only file of
// [len u32][crc u32][term u64][index u64][data] records, plus an in-memory
// copy of every entry (the metadata log is small; there is no compaction).
type diskLog struct {
	dir       string
	f         *os.File
	entries   []Entry // entries[i].Index == i+1
	positions []int64 // byte offset of each entry in the file
	size      int64
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func openDiskLog(dir string) (*diskLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "log.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	l := &diskLog{dir: dir, f: f}
	if err := l.load(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// load reads entries until the first invalid record and truncates the
// file there (a crash can leave a torn tail).
func (l *diskLog) load() error {
	r := bufio.NewReader(io.NewSectionReader(l.f, 0, 1<<62))
	var pos int64
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(hdr[0:])
		crc := binary.BigEndian.Uint32(hdr[4:])
		if n < 16 || n > 64<<20 {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			break
		}
		if crc32.Checksum(body, crcTable) != crc {
			break
		}
		e := Entry{
			Term:  binary.BigEndian.Uint64(body[0:]),
			Index: binary.BigEndian.Uint64(body[8:]),
		}
		if len(body) > 16 {
			e.Data = body[16:]
		}
		if e.Index != uint64(len(l.entries))+1 {
			break
		}
		l.entries = append(l.entries, e)
		l.positions = append(l.positions, pos)
		pos += int64(8 + n)
	}
	l.size = pos
	if err := l.f.Truncate(pos); err != nil {
		return err
	}
	return nil
}

func (l *diskLog) lastIndex() uint64 { return uint64(len(l.entries)) }

func (l *diskLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index (0 for index 0).
func (l *diskLog) term(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index > l.lastIndex() {
		return 0, false
	}
	return l.entries[index-1].Term, true
}

func (l *diskLog) entry(index uint64) Entry { return l.entries[index-1] }

// slice returns up to max entries starting at index.
func (l *diskLog) slice(from uint64, max int) []Entry {
	if from < 1 || from > l.lastIndex() {
		return nil
	}
	end := min(uint64(len(l.entries)), from-1+uint64(max))
	return append([]Entry(nil), l.entries[from-1:end]...)
}

// append writes entries and fsyncs.
func (l *diskLog) append(es ...Entry) error {
	var buf []byte
	for _, e := range es {
		if e.Index != l.lastIndex()+1 {
			return fmt.Errorf("raft: append index %d, expected %d", e.Index, l.lastIndex()+1)
		}
		body := make([]byte, 16+len(e.Data))
		binary.BigEndian.PutUint64(body[0:], e.Term)
		binary.BigEndian.PutUint64(body[8:], e.Index)
		copy(body[16:], e.Data)
		var hdr [8]byte
		binary.BigEndian.PutUint32(hdr[0:], uint32(len(body)))
		binary.BigEndian.PutUint32(hdr[4:], crc32.Checksum(body, crcTable))
		l.positions = append(l.positions, l.size+int64(len(buf)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, body...)
		l.entries = append(l.entries, e)
	}
	if _, err := l.f.WriteAt(buf, l.size); err != nil {
		return err
	}
	l.size += int64(len(buf))
	return l.f.Sync()
}

// truncateFrom deletes entries with Index >= index (conflict resolution).
func (l *diskLog) truncateFrom(index uint64) error {
	if index < 1 || index > l.lastIndex() {
		return nil
	}
	pos := l.positions[index-1]
	if err := l.f.Truncate(pos); err != nil {
		return err
	}
	l.entries = l.entries[:index-1]
	l.positions = l.positions[:index-1]
	l.size = pos
	return l.f.Sync()
}

func (l *diskLog) close() error { return l.f.Close() }

func loadHardState(dir string) (hardState, error) {
	hs := hardState{VotedFor: -1}
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return hs, nil
	}
	if err != nil {
		return hs, err
	}
	if err := json.Unmarshal(data, &hs); err != nil {
		return hs, fmt.Errorf("raft: corrupt state.json: %w", err)
	}
	return hs, nil
}

// saveHardState writes term/vote atomically and durably.
func saveHardState(dir string, hs hardState) error {
	data, _ := json.Marshal(hs)
	tmp := filepath.Join(dir, "state.json.tmp")
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
	f.Close()
	return os.Rename(tmp, filepath.Join(dir, "state.json"))
}
