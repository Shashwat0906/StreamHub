package protocol

import "testing"

// FuzzDecodeRequests: a malicious or corrupt peer must not be able to
// panic the broker through any request decoder.
func FuzzDecodeRequests(f *testing.F) {
	f.Add(Marshal(&FetchRequest{ReplicaID: 1, Partitions: []FetchPartition{{"t", 0, 0, 1, -1}}}))
	f.Add(Marshal(&ProduceRequest{Topic: "t"}))
	f.Add(Marshal(&RaftAppendRequest{Entries: []RaftEntry{{1, 1, []byte("x")}}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, m := range []Message{
			&ProduceRequest{}, &FetchRequest{}, &MetadataRequest{}, &CreateTopicRequest{},
			&JoinGroupRequest{}, &OffsetCommitRequest{}, &OffsetFetchRequest{},
			&RaftAppendRequest{}, &RaftVoteRequest{}, &AlterISRRequest{}, &FetchResponse{}, &MetadataResponse{},
		} {
			_ = Unmarshal(b, m)
		}
	})
}
