package metadata

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"
)

// LocalLog is a single-node Proposer: commands are appended to a local
// JSON-lines file (fsync'd) and applied immediately. It is used for
// standalone single-broker mode and in unit tests. Clusters use Raft.
type LocalLog struct {
	mu    sync.Mutex
	f     *os.File
	store *Store
	index uint64
	self  int32
}

// OpenLocalLog replays path into store and returns a proposer.
func OpenLocalLog(path string, store *Store, self int32) (*LocalLog, error) {
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		var idx uint64
		for sc.Scan() {
			line := sc.Bytes()
			var c Command
			if json.Unmarshal(line, &c) != nil {
				break // torn last line after a crash: ignore it
			}
			idx++
			store.Apply(idx, line)
		}
		f.Close()
		l, err := openForAppend(path, store, self)
		if l != nil {
			l.index = idx
		}
		return l, err
	}
	return openForAppend(path, store, self)
}

func openForAppend(path string, store *Store, self int32) (*LocalLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &LocalLog{f: f, store: store, self: self}, nil
}

func (l *LocalLog) Propose(ctx context.Context, cmd Command) (Result, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	data := cmd.Encode()
	if _, err := l.f.Write(append(data, '\n')); err != nil {
		return Result{}, err
	}
	if err := l.f.Sync(); err != nil {
		return Result{}, err
	}
	l.index++
	return l.store.Apply(l.index, data).(Result), nil
}

func (l *LocalLog) IsLeader() bool  { return true }
func (l *LocalLog) LeaderID() int32 { return l.self }
func (l *LocalLog) Close() error    { return l.f.Close() }
