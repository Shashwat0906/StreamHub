package storage

import "testing"

// FuzzDecodeRecord: arbitrary bytes must never panic the record decoder,
// and anything it accepts must re-encode to the same bytes.
func FuzzDecodeRecord(f *testing.F) {
	r := Record{Offset: 1, Timestamp: 2, LeaderEpoch: 3, ProducerID: 4, Sequence: 5, Key: []byte("k"), Value: []byte("v")}
	f.Add(r.AppendEncoded(nil))
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		rec, n, err := DecodeRecord(b)
		if err != nil {
			return
		}
		if got := rec.AppendEncoded(nil); string(got) != string(b[:n]) {
			t.Fatalf("accepted record does not round-trip")
		}
	})
}
