package protocol

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/Shashwat0906/StreamHub/internal/storage"
)

// Every message must survive encode → decode → encode byte-for-byte.
func TestMessagesRoundTrip(t *testing.T) {
	recs := []storage.Record{
		{Offset: 5, Timestamp: 99, LeaderEpoch: 2, ProducerID: 7, Sequence: 3, Key: []byte("k"), Value: []byte("v")},
		{Offset: 6, Key: nil, Value: []byte{}},
	}
	msgs := []Message{
		&ProduceRequest{Acks: AcksAll, TimeoutMs: 1000, Topic: "t", Partition: 2, ProducerID: 9, BaseSequence: 4, Records: recs},
		&ProduceResponse{Err: ErrNotLeader, ErrMsg: "x", BaseOffset: 10, LogStartOffset: 1},
		&FetchRequest{ReplicaID: 2, MaxWaitMs: 500, MinBytes: 1, Partitions: []FetchPartition{{"t", 1, 100, 1024, 3}}},
		&FetchResponse{Partitions: []FetchPartitionResponse{{Topic: "t", Partition: 1, HighWatermark: 7, LogEndOffset: 9, LeaderEpoch: 3, Records: recs}}},
		&ListOffsetsRequest{Topic: "t", Partition: 0, Timestamp: OffsetEarliest},
		&ListOffsetsResponse{Offset: 42},
		&MetadataRequest{Topics: []string{"a", "b"}},
		&MetadataResponse{ControllerID: 2, Brokers: []BrokerInfo{{1, "h:1", false, "h:8081"}, {2, "h:2", true, ""}},
			Topics: []TopicInfo{{Name: "a", Partitions: []PartitionInfo{{0, 1, 4, []int32{1, 2, 3}, []int32{1, 2}}}, Configs: map[string]string{"retention.ms": "1"}}}},
		&CreateTopicRequest{Name: "a", Partitions: 3, ReplicationFactor: 3, Configs: map[string]string{"k": "v"}},
		&SimpleResponse{Err: ErrTopicAlreadyExists, ErrMsg: "exists"},
		&DeleteTopicRequest{Name: "a"},
		&InitProducerIDResponse{ProducerID: 77},
		&FindCoordinatorRequest{Group: "g"},
		&FindCoordinatorResponse{NodeID: 3, Addr: "x:1"},
		&JoinGroupRequest{Group: "g", MemberID: "m", ClientID: "c", Topics: []string{"t"}, Strategy: "range", SessionTimeoutMs: 1, RebalanceTimeoutMs: 2},
		&JoinGroupResponse{Generation: 3, MemberID: "m", Strategy: "range", Members: 2, Assignment: []TopicPartitions{{"t", []int32{0, 2}}}},
		&HeartbeatRequest{Group: "g", MemberID: "m", Generation: 4},
		&LeaveGroupRequest{Group: "g", MemberID: "m"},
		&OffsetCommitRequest{Group: "g", MemberID: "m", Generation: 1, Offsets: []CommitOffset{{"t", 0, 5, "meta"}}},
		&OffsetFetchRequest{Group: "g", Partitions: []TopicPartitions{{"t", []int32{1}}}},
		&OffsetFetchResponse{Offsets: []CommitOffset{{"t", 1, -1, ""}}},
		&ListGroupsResponse{Groups: []GroupListing{{"g", "Stable"}}},
		&DescribeGroupResponse{State: "Stable", Strategy: "roundrobin", Generation: 2, Members: []GroupMemberInfo{{"m", "c", []TopicPartitions{{"t", []int32{0}}}}}},
		&OffsetForLeaderEpochRequest{Topic: "t", Partition: 0, LeaderEpoch: 5},
		&OffsetForLeaderEpochResponse{LeaderEpoch: 4, EndOffset: 100},
		&BrokerHeartbeatRequest{BrokerID: 1, Addr: "a", HTTPAddr: "a:8080", ShuttingDown: true},
		&BrokerHeartbeatResponse{ControllerID: 1, Fenced: true},
		&AlterISRRequest{BrokerID: 1, Topic: "t", Partition: 0, LeaderEpoch: 2, NewISR: []int32{1, 2}},
		&RaftVoteRequest{Term: 5, CandidateID: 2, LastLogIndex: 10, LastLogTerm: 4},
		&RaftVoteResponse{Term: 5, Granted: true},
		&RaftAppendRequest{Term: 5, LeaderID: 1, PrevLogIndex: 3, PrevLogTerm: 2, Entries: []RaftEntry{{5, 4, []byte("cmd")}}, LeaderCommit: 3},
		&RaftAppendResponse{Term: 5, Success: true, MatchIndex: 4},
	}
	for _, m := range msgs {
		b := Marshal(m)
		out := reflect.New(reflect.TypeOf(m).Elem()).Interface().(Message)
		if err := Unmarshal(b, out); err != nil {
			t.Fatalf("%T: %v", m, err)
		}
		if !bytes.Equal(Marshal(out), b) {
			t.Fatalf("%T: re-encoding differs", m)
		}
	}
}

func TestFrameRoundTrip(t *testing.T) {
	req := &HeartbeatRequest{Group: "g", MemberID: "m", Generation: 1}
	frame := EncodeRequest(APIHeartbeat, 42, req)
	payload, err := ReadFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatal(err)
	}
	h, body, err := ParseRequest(payload)
	if err != nil || h.API != APIHeartbeat || h.CorrelationID != 42 || h.Version != Version {
		t.Fatalf("header %+v err %v", h, err)
	}
	var got HeartbeatRequest
	if err := Unmarshal(body, &got); err != nil || got != *req {
		t.Fatalf("body %+v err %v", got, err)
	}
}

func TestDecoderRejectsGarbage(t *testing.T) {
	// A huge array length must not allocate; it must fail cleanly.
	e := NewEncoder(nil)
	e.Int32(2)   // replica id
	e.Int32(0)   // max wait
	e.Int32(0)   // min bytes
	e.Int32(1e9) // partitions count: absurd
	var f FetchRequest
	if err := Unmarshal(e.Bytes(), &f); err == nil {
		t.Fatal("expected error for absurd array length")
	}
	var p ProduceRequest
	if err := Unmarshal([]byte{0, 1}, &p); err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestRetriableCodes(t *testing.T) {
	if !ErrNotLeader.Retriable() || ErrTopicAlreadyExists.Retriable() {
		t.Fatal("retriable classification wrong")
	}
}
