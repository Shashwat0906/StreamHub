package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func mkRecs(n int, prefix string, epoch int32) []Record {
	out := make([]Record, n)
	for i := range out {
		out[i] = Record{
			Timestamp:   time.Now().UnixMilli(),
			LeaderEpoch: epoch,
			ProducerID:  -1,
			Sequence:    -1,
			Key:         []byte(fmt.Sprintf("k%d", i)),
			Value:       []byte(fmt.Sprintf("%s-%d", prefix, i)),
		}
	}
	return out
}

func smallCfg() Config {
	return Config{SegmentBytes: 1024, IndexIntervalBytes: 128, RetentionBytes: -1, RetentionMs: -1}
}

func TestRecordRoundTrip(t *testing.T) {
	cases := []Record{
		{Offset: 7, Timestamp: 123, LeaderEpoch: 2, ProducerID: 99, Sequence: 5, Key: []byte("key"), Value: []byte("value")},
		{Offset: 0, Timestamp: 1, LeaderEpoch: 0, ProducerID: -1, Sequence: -1, Key: nil, Value: []byte{}},
		{Offset: 1 << 40, Key: []byte{}, Value: bytes.Repeat([]byte("x"), 10000)},
	}
	for _, in := range cases {
		buf := in.AppendEncoded(nil)
		if len(buf) != in.EncodedSize() {
			t.Fatalf("size %d != %d", len(buf), in.EncodedSize())
		}
		out, n, err := DecodeRecord(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(buf) || out.Offset != in.Offset || out.Timestamp != in.Timestamp ||
			out.LeaderEpoch != in.LeaderEpoch || out.ProducerID != in.ProducerID ||
			out.Sequence != in.Sequence || !bytes.Equal(out.Value, in.Value) ||
			(in.Key == nil) != (out.Key == nil) || !bytes.Equal(out.Key, in.Key) {
			t.Fatalf("round trip mismatch: %+v vs %+v", in, out)
		}
	}
}

func TestRecordCRCDetectsCorruption(t *testing.T) {
	r := Record{Offset: 1, Key: []byte("a"), Value: []byte("hello")}
	buf := r.AppendEncoded(nil)
	for i := 8; i < len(buf); i++ {
		c := append([]byte(nil), buf...)
		c[i] ^= 0x40
		if _, _, err := DecodeRecord(c); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flip at byte %d not detected: %v", i, err)
		}
	}
	if _, _, err := DecodeRecord(buf[:len(buf)-1]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("truncation not detected: %v", err)
	}
}

func TestAppendReadAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, smallCfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for b := 0; b < 20; b++ {
		base, err := l.Append(mkRecs(5, fmt.Sprintf("b%d", b), 0))
		if err != nil {
			t.Fatal(err)
		}
		if base != int64(b*5) {
			t.Fatalf("base %d want %d", base, b*5)
		}
	}
	if l.EndOffset() != 100 {
		t.Fatalf("end %d", l.EndOffset())
	}
	if l.SegmentCount() < 3 {
		t.Fatalf("expected several segments, got %d", l.SegmentCount())
	}
	// Read every offset individually: exercises index lookup + scan.
	for off := int64(0); off < 100; off++ {
		recs, err := l.Read(off, -1, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 1 || recs[0].Offset != off {
			t.Fatalf("read %d got %+v", off, recs)
		}
		want := fmt.Sprintf("b%d-%d", off/5, off%5)
		if string(recs[0].Value) != want {
			t.Fatalf("offset %d value %q want %q", off, recs[0].Value, want)
		}
	}
	// A large read spans segments and returns everything in order.
	recs, err := l.Read(0, -1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 100 {
		t.Fatalf("got %d records", len(recs))
	}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("out of order at %d: %d", i, r.Offset)
		}
	}
	// maxOffset (high-watermark) hides the tail.
	recs, _ = l.Read(90, 95, 1<<20)
	if len(recs) != 5 || recs[4].Offset != 94 {
		t.Fatalf("maxOffset not respected: %d records", len(recs))
	}
	// Reading at the end is not an error; beyond it is.
	if recs, err := l.Read(100, -1, 100); err != nil || len(recs) != 0 {
		t.Fatalf("read at end: %v %d", err, len(recs))
	}
	if _, err := l.Read(101, -1, 100); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("expected out of range, got %v", err)
	}
}

func TestReopenKeepsData(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, smallCfg(), nil)
	for i := 0; i < 10; i++ {
		l.Append(mkRecs(4, "x", 0))
	}
	l.Close()
	l2, err := Open(dir, smallCfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.EndOffset() != 40 || l2.RecoveredTruncatedBytes != 0 {
		t.Fatalf("end %d truncated %d", l2.EndOffset(), l2.RecoveredTruncatedBytes)
	}
	l2.Append(mkRecs(1, "after", 0))
	recs, _ := l2.Read(40, -1, 100)
	if len(recs) != 1 || string(recs[0].Value) != "after-0" {
		t.Fatalf("append after reopen: %+v", recs)
	}
}

func activeLogFile(t *testing.T, dir string) string {
	bases, err := listSegmentBases(dir)
	if err != nil || len(bases) == 0 {
		t.Fatal("no segments", err)
	}
	return segmentFileName(dir, bases[len(bases)-1], logSuffix)
}

// Simulates a crash in the middle of writing a record: the file ends with
// half a record. Recovery must drop exactly that partial record.
func TestRecoverTruncatesTornWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := smallCfg()
	cfg.SegmentBytes = 1 << 20
	l, _ := Open(dir, cfg, nil)
	l.Append(mkRecs(10, "ok", 0))
	l.Close()

	path := activeLogFile(t, dir)
	torn := Record{Offset: 10, Key: []byte("k"), Value: []byte("this record was being written when we crashed")}
	enc := torn.AppendEncoded(nil)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write(enc[:len(enc)/2])
	f.Close()

	l2, err := Open(dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.EndOffset() != 10 {
		t.Fatalf("end offset %d want 10", l2.EndOffset())
	}
	if l2.RecoveredTruncatedBytes != int64(len(enc)/2) {
		t.Fatalf("truncated %d want %d", l2.RecoveredTruncatedBytes, len(enc)/2)
	}
	recs, _ := l2.Read(0, -1, 1<<20)
	if len(recs) != 10 {
		t.Fatalf("lost good records: %d", len(recs))
	}
	// The log is writable again at the right offset.
	base, _ := l2.Append(mkRecs(1, "new", 0))
	if base != 10 {
		t.Fatalf("base %d", base)
	}
}

// Simulates a bit flip (or garbage) inside the last segment: everything
// from the bad record on is dropped, everything before survives.
func TestRecoverBadCRC(t *testing.T) {
	dir := t.TempDir()
	cfg := smallCfg()
	cfg.SegmentBytes = 1 << 20
	l, _ := Open(dir, cfg, nil)
	l.Append(mkRecs(10, "ok", 0))
	l.Close()

	path := activeLogFile(t, dir)
	data, _ := os.ReadFile(path)
	one := mkRecs(1, "ok", 0)[0]
	// Corrupt a byte inside record #6 (records have equal size here).
	recSize := one.EncodedSize()
	data[6*recSize+30] ^= 0xff
	os.WriteFile(path, data, 0o644)

	l2, err := Open(dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.EndOffset() != 6 {
		t.Fatalf("end offset %d want 6", l2.EndOffset())
	}
	recs, _ := l2.Read(0, -1, 1<<20)
	if len(recs) != 6 {
		t.Fatalf("records %d", len(recs))
	}
}

func TestRecoverRebuildsCorruptIndex(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, smallCfg(), nil)
	for i := 0; i < 20; i++ {
		l.Append(mkRecs(5, "v", 0))
	}
	l.Close()
	// Damage the first segment's index (odd size).
	bases, _ := listSegmentBases(dir)
	os.WriteFile(segmentFileName(dir, bases[0], indexSuffix), []byte{1, 2, 3}, 0o644)
	l2, err := Open(dir, smallCfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	for off := int64(0); off < 100; off += 7 {
		recs, err := l2.Read(off, -1, 1)
		if err != nil || len(recs) != 1 || recs[0].Offset != off {
			t.Fatalf("offset %d: %v %+v", off, err, recs)
		}
	}
}

func TestTruncateTo(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, smallCfg(), nil)
	defer l.Close()
	l.Append(mkRecs(30, "e1", 1))
	l.Append(mkRecs(30, "e2", 2))
	if err := l.TruncateTo(25); err != nil {
		t.Fatal(err)
	}
	if l.EndOffset() != 25 {
		t.Fatalf("end %d", l.EndOffset())
	}
	if e := l.Epochs().Entries(); len(e) != 1 || e[0].Epoch != 1 {
		t.Fatalf("epochs after truncate: %+v", e)
	}
	base, _ := l.Append(mkRecs(1, "e3", 3))
	if base != 25 {
		t.Fatalf("base %d", base)
	}
	recs, _ := l.Read(0, -1, 1<<20)
	if len(recs) != 26 || string(recs[25].Value) != "e3-0" {
		t.Fatalf("bad content after truncate: %d", len(recs))
	}
	// Truncation survives a restart.
	l.Close()
	l2, _ := Open(dir, smallCfg(), nil)
	defer l2.Close()
	if l2.EndOffset() != 26 {
		t.Fatalf("end after reopen %d", l2.EndOffset())
	}
}

func TestAppendReplicatedRequiresSequentialOffsets(t *testing.T) {
	l, _ := Open(t.TempDir(), smallCfg(), nil)
	defer l.Close()
	recs := mkRecs(3, "r", 0)
	for i := range recs {
		recs[i].Offset = int64(i)
	}
	if err := l.AppendReplicated(recs); err != nil {
		t.Fatal(err)
	}
	gap := mkRecs(1, "gap", 0)
	gap[0].Offset = 10
	if err := l.AppendReplicated(gap); !errors.Is(err, ErrNonSequential) {
		t.Fatalf("expected ErrNonSequential, got %v", err)
	}
}

func TestRetentionBySize(t *testing.T) {
	cfg := smallCfg()
	cfg.RetentionBytes = 2048
	l, _ := Open(t.TempDir(), cfg, nil)
	defer l.Close()
	for i := 0; i < 50; i++ {
		l.Append(mkRecs(5, "v", 0))
	}
	before := l.SegmentCount()
	n, err := l.ApplyRetention(time.Now(), -1)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || l.SegmentCount() != before-n {
		t.Fatalf("deleted %d of %d", n, before)
	}
	if l.Size() < 2048-1024 || l.StartOffset() == 0 {
		t.Fatalf("size %d start %d", l.Size(), l.StartOffset())
	}
	if _, err := l.Read(0, -1, 10); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("expected out of range after retention: %v", err)
	}
	if recs, err := l.Read(l.StartOffset(), -1, 10); err != nil || len(recs) == 0 {
		t.Fatalf("read at new start: %v", err)
	}
}

func TestRetentionByTimeKeepsActiveSegment(t *testing.T) {
	cfg := smallCfg()
	cfg.RetentionMs = 1000
	l, _ := Open(t.TempDir(), cfg, nil)
	defer l.Close()
	old := mkRecs(5, "old", 0)
	for i := range old {
		old[i].Timestamp = time.Now().Add(-time.Hour).UnixMilli()
	}
	for i := 0; i < 10; i++ {
		l.Append(append([]Record(nil), old...))
	}
	n, _ := l.ApplyRetention(time.Now(), -1)
	if l.SegmentCount() != 1 || n == 0 {
		t.Fatalf("expected only active segment left, have %d (deleted %d)", l.SegmentCount(), n)
	}
	// maxDeletable protects data above the high-watermark.
	l2, _ := Open(t.TempDir(), cfg, nil)
	defer l2.Close()
	for i := 0; i < 10; i++ {
		l2.Append(append([]Record(nil), old...))
	}
	if n, _ := l2.ApplyRetention(time.Now(), 0); n != 0 {
		t.Fatalf("deleted %d segments above maxDeletable", n)
	}
}

func TestEpochCacheEndOffsetFor(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, smallCfg(), nil)
	l.Append(mkRecs(10, "a", 1)) // epoch 1: 0..9
	l.Append(mkRecs(5, "b", 3))  // epoch 3: 10..14
	l.Append(mkRecs(5, "c", 4))  // epoch 4: 15..19
	ec := l.Epochs()
	check := func(req int32, wantEpoch int32, wantEnd int64) {
		t.Helper()
		e, end := ec.EndOffsetFor(req, l.EndOffset())
		if e != wantEpoch || end != wantEnd {
			t.Fatalf("EndOffsetFor(%d) = (%d,%d) want (%d,%d)", req, e, end, wantEpoch, wantEnd)
		}
	}
	check(1, 1, 10)
	check(2, 1, 10) // epoch 2 never wrote: answer for epoch 1
	check(3, 3, 15)
	check(4, 4, 20)
	check(9, 4, 20)
	check(0, -1, -1)
	l.Close()
	// Checkpoint persisted.
	l2, _ := Open(dir, smallCfg(), nil)
	defer l2.Close()
	if got := l2.Epochs().Entries(); len(got) != 3 || got[2].StartOffset != 15 {
		t.Fatalf("epochs not persisted: %+v", got)
	}
}

func TestIndexLookup(t *testing.T) {
	ix, err := openIndex(filepath.Join(t.TempDir(), "x.index"))
	if err != nil {
		t.Fatal(err)
	}
	defer ix.close()
	ix.append(10, 1000)
	ix.append(20, 2000)
	ix.append(30, 3000)
	cases := map[uint32]uint32{0: 0, 9: 0, 10: 1000, 15: 1000, 20: 2000, 29: 2000, 30: 3000, 99: 3000}
	for in, want := range cases {
		if got := ix.lookup(in); got != want {
			t.Fatalf("lookup(%d)=%d want %d", in, got, want)
		}
	}
	if err := ix.load(0, 4000); err != nil {
		t.Fatal(err)
	}
	if len(ix.entries) != 3 {
		t.Fatalf("load got %d entries", len(ix.entries))
	}
	if err := ix.load(0, 2500); err == nil {
		t.Fatal("entry pointing past log end should fail validation")
	}
}

func BenchmarkAppend(b *testing.B) {
	l, _ := Open(b.TempDir(), DefaultConfig(), nil)
	defer l.Close()
	val := bytes.Repeat([]byte("x"), 100)
	batch := make([]Record, 100)
	for i := range batch {
		batch[i] = Record{Value: val, ProducerID: -1, Sequence: -1}
	}
	b.SetBytes(int64(100 * batch[0].EncodedSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Append(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	l, _ := Open(b.TempDir(), DefaultConfig(), nil)
	defer l.Close()
	val := bytes.Repeat([]byte("x"), 100)
	for i := 0; i < 1000; i++ {
		batch := make([]Record, 100)
		for j := range batch {
			batch[j] = Record{Value: val}
		}
		l.Append(batch)
	}
	b.ResetTimer()
	var bytesRead int64
	for i := 0; i < b.N; i++ {
		off := int64(i*100) % 100000
		recs, err := l.Read(off, -1, 64<<10)
		if err != nil {
			b.Fatal(err)
		}
		for _, r := range recs {
			bytesRead += int64(r.EncodedSize())
		}
	}
	b.SetBytes(bytesRead / int64(b.N))
}
