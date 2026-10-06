package protocol

import "github.com/Shashwat0906/StreamHub/internal/storage"

// APIKey identifies a request type in the frame header.
type APIKey uint16

const (
	APIProduce APIKey = iota + 1
	APIFetch
	APIListOffsets
	APIMetadata
	APICreateTopic
	APIDeleteTopic
	APIInitProducerID
	APIFindCoordinator
	APIJoinGroup
	APIHeartbeat
	APILeaveGroup
	APIOffsetCommit
	APIOffsetFetch
	APIListGroups
	APIDescribeGroup
	APIOffsetForLeaderEpoch
	APIBrokerHeartbeat
	APIAlterISR
	APIRaftVote
	APIRaftAppend
)

var apiNames = map[APIKey]string{
	APIProduce: "Produce", APIFetch: "Fetch", APIListOffsets: "ListOffsets",
	APIMetadata: "Metadata", APICreateTopic: "CreateTopic", APIDeleteTopic: "DeleteTopic",
	APIInitProducerID: "InitProducerId", APIFindCoordinator: "FindCoordinator",
	APIJoinGroup: "JoinGroup", APIHeartbeat: "Heartbeat", APILeaveGroup: "LeaveGroup",
	APIOffsetCommit: "OffsetCommit", APIOffsetFetch: "OffsetFetch", APIListGroups: "ListGroups",
	APIDescribeGroup: "DescribeGroup", APIOffsetForLeaderEpoch: "OffsetForLeaderEpoch",
	APIBrokerHeartbeat: "BrokerHeartbeat", APIAlterISR: "AlterISR",
	APIRaftVote: "RaftVote", APIRaftAppend: "RaftAppend",
}

func (k APIKey) String() string {
	if s, ok := apiNames[k]; ok {
		return s
	}
	return "Unknown"
}

// Acks values for Produce.
const (
	AcksNone   int16 = 0
	AcksLeader int16 = 1
	AcksAll    int16 = -1
)

// Special ListOffsets timestamps.
const (
	OffsetLatest   int64 = -1
	OffsetEarliest int64 = -2
)

// ---------------------------------------------------------------- records

func encodeRecords(e *Encoder, recs []storage.Record) {
	e.ArrayLen(len(recs))
	for i := range recs {
		r := &recs[i]
		e.Int64(r.Offset)
		e.Int64(r.Timestamp)
		e.Int32(r.LeaderEpoch)
		e.Int64(r.ProducerID)
		e.Int32(r.Sequence)
		e.BytesField(r.Key)
		e.BytesField(r.Value)
	}
}

func decodeRecords(d *Decoder) []storage.Record {
	n := d.ArrayLen(40)
	if n == 0 {
		return nil
	}
	out := make([]storage.Record, n)
	for i := range out {
		r := &out[i]
		r.Offset = d.Int64()
		r.Timestamp = d.Int64()
		r.LeaderEpoch = d.Int32()
		r.ProducerID = d.Int64()
		r.Sequence = d.Int32()
		r.Key = d.BytesField()
		r.Value = d.BytesField()
	}
	return out
}

// ---------------------------------------------------------------- produce

// ProduceRequest appends one batch to one partition.
type ProduceRequest struct {
	Acks         int16
	TimeoutMs    int32
	Topic        string
	Partition    int32
	ProducerID   int64 // -1 = not idempotent
	BaseSequence int32 // sequence of Records[0]; -1 when ProducerID == -1
	Records      []storage.Record
}

func (m *ProduceRequest) Encode(e *Encoder) {
	e.Int16(m.Acks)
	e.Int32(m.TimeoutMs)
	e.String(m.Topic)
	e.Int32(m.Partition)
	e.Int64(m.ProducerID)
	e.Int32(m.BaseSequence)
	encodeRecords(e, m.Records)
}

func (m *ProduceRequest) Decode(d *Decoder) {
	m.Acks = d.Int16()
	m.TimeoutMs = d.Int32()
	m.Topic = d.String()
	m.Partition = d.Int32()
	m.ProducerID = d.Int64()
	m.BaseSequence = d.Int32()
	m.Records = decodeRecords(d)
}

type ProduceResponse struct {
	Err            ErrorCode
	ErrMsg         string
	BaseOffset     int64
	LogStartOffset int64
}

func (m *ProduceResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.String(m.ErrMsg)
	e.Int64(m.BaseOffset)
	e.Int64(m.LogStartOffset)
}

func (m *ProduceResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.ErrMsg = d.String()
	m.BaseOffset = d.Int64()
	m.LogStartOffset = d.Int64()
}

// ---------------------------------------------------------------- fetch

// ReplicaIDConsumer marks a fetch from a normal consumer.
const ReplicaIDConsumer int32 = -1

type FetchPartition struct {
	Topic              string
	Partition          int32
	FetchOffset        int64
	MaxBytes           int32
	CurrentLeaderEpoch int32 // -1 = don't check
}

// FetchRequest reads from several partitions at once. If no partition has
// at least MinBytes available the broker holds the request (long poll) for
// up to MaxWaitMs.
type FetchRequest struct {
	ReplicaID  int32 // broker ID for follower fetches, -1 for consumers
	MaxWaitMs  int32
	MinBytes   int32
	Partitions []FetchPartition
}

func (m *FetchRequest) Encode(e *Encoder) {
	e.Int32(m.ReplicaID)
	e.Int32(m.MaxWaitMs)
	e.Int32(m.MinBytes)
	e.ArrayLen(len(m.Partitions))
	for _, p := range m.Partitions {
		e.String(p.Topic)
		e.Int32(p.Partition)
		e.Int64(p.FetchOffset)
		e.Int32(p.MaxBytes)
		e.Int32(p.CurrentLeaderEpoch)
	}
}

func (m *FetchRequest) Decode(d *Decoder) {
	m.ReplicaID = d.Int32()
	m.MaxWaitMs = d.Int32()
	m.MinBytes = d.Int32()
	n := d.ArrayLen(22)
	m.Partitions = make([]FetchPartition, n)
	for i := range m.Partitions {
		p := &m.Partitions[i]
		p.Topic = d.String()
		p.Partition = d.Int32()
		p.FetchOffset = d.Int64()
		p.MaxBytes = d.Int32()
		p.CurrentLeaderEpoch = d.Int32()
	}
}

type FetchPartitionResponse struct {
	Topic          string
	Partition      int32
	Err            ErrorCode
	HighWatermark  int64
	LogStartOffset int64
	LogEndOffset   int64
	LeaderEpoch    int32
	Records        []storage.Record
}

type FetchResponse struct {
	Err        ErrorCode
	Partitions []FetchPartitionResponse
}

func (m *FetchResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.ArrayLen(len(m.Partitions))
	for i := range m.Partitions {
		p := &m.Partitions[i]
		e.String(p.Topic)
		e.Int32(p.Partition)
		e.Int16(int16(p.Err))
		e.Int64(p.HighWatermark)
		e.Int64(p.LogStartOffset)
		e.Int64(p.LogEndOffset)
		e.Int32(p.LeaderEpoch)
		encodeRecords(e, p.Records)
	}
}

func (m *FetchResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	n := d.ArrayLen(40)
	m.Partitions = make([]FetchPartitionResponse, n)
	for i := range m.Partitions {
		p := &m.Partitions[i]
		p.Topic = d.String()
		p.Partition = d.Int32()
		p.Err = ErrorCode(d.Int16())
		p.HighWatermark = d.Int64()
		p.LogStartOffset = d.Int64()
		p.LogEndOffset = d.Int64()
		p.LeaderEpoch = d.Int32()
		p.Records = decodeRecords(d)
	}
}

// ---------------------------------------------------------------- list offsets

type ListOffsetsRequest struct {
	Topic     string
	Partition int32
	Timestamp int64 // OffsetLatest or OffsetEarliest
}

func (m *ListOffsetsRequest) Encode(e *Encoder) {
	e.String(m.Topic)
	e.Int32(m.Partition)
	e.Int64(m.Timestamp)
}

func (m *ListOffsetsRequest) Decode(d *Decoder) {
	m.Topic = d.String()
	m.Partition = d.Int32()
	m.Timestamp = d.Int64()
}

type ListOffsetsResponse struct {
	Err    ErrorCode
	Offset int64
}

func (m *ListOffsetsResponse) Encode(e *Encoder) { e.Int16(int16(m.Err)); e.Int64(m.Offset) }
func (m *ListOffsetsResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.Offset = d.Int64()
}

// ---------------------------------------------------------------- metadata

type MetadataRequest struct {
	Topics []string // empty = all topics
}

func (m *MetadataRequest) Encode(e *Encoder) { e.Strings(m.Topics) }
func (m *MetadataRequest) Decode(d *Decoder) { m.Topics = d.Strings() }

type BrokerInfo struct {
	ID       int32
	Addr     string
	Fenced   bool
	HTTPAddr string // advertised /metrics + /v1/state address ("" if disabled)
}

type PartitionInfo struct {
	ID          int32
	Leader      int32 // -1 = offline
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
}

type TopicInfo struct {
	Name       string
	Err        ErrorCode
	Partitions []PartitionInfo
	Configs    map[string]string
}

type MetadataResponse struct {
	Err          ErrorCode
	ControllerID int32
	Brokers      []BrokerInfo
	Topics       []TopicInfo
}

func (m *MetadataResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.Int32(m.ControllerID)
	e.ArrayLen(len(m.Brokers))
	for _, b := range m.Brokers {
		e.Int32(b.ID)
		e.String(b.Addr)
		e.Bool(b.Fenced)
		e.String(b.HTTPAddr)
	}
	e.ArrayLen(len(m.Topics))
	for _, t := range m.Topics {
		e.String(t.Name)
		e.Int16(int16(t.Err))
		e.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			e.Int32(p.ID)
			e.Int32(p.Leader)
			e.Int32(p.LeaderEpoch)
			e.Int32s(p.Replicas)
			e.Int32s(p.ISR)
		}
		e.StringMap(t.Configs)
	}
}

func (m *MetadataResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.ControllerID = d.Int32()
	nb := d.ArrayLen(9)
	m.Brokers = make([]BrokerInfo, nb)
	for i := range m.Brokers {
		m.Brokers[i].ID = d.Int32()
		m.Brokers[i].Addr = d.String()
		m.Brokers[i].Fenced = d.Bool()
		m.Brokers[i].HTTPAddr = d.String()
	}
	nt := d.ArrayLen(8)
	m.Topics = make([]TopicInfo, nt)
	for i := range m.Topics {
		t := &m.Topics[i]
		t.Name = d.String()
		t.Err = ErrorCode(d.Int16())
		np := d.ArrayLen(20)
		t.Partitions = make([]PartitionInfo, np)
		for j := range t.Partitions {
			p := &t.Partitions[j]
			p.ID = d.Int32()
			p.Leader = d.Int32()
			p.LeaderEpoch = d.Int32()
			p.Replicas = d.Int32s()
			p.ISR = d.Int32s()
		}
		t.Configs = d.StringMap()
	}
}

// ---------------------------------------------------------------- admin

type CreateTopicRequest struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

func (m *CreateTopicRequest) Encode(e *Encoder) {
	e.String(m.Name)
	e.Int32(m.Partitions)
	e.Int16(m.ReplicationFactor)
	e.StringMap(m.Configs)
}

func (m *CreateTopicRequest) Decode(d *Decoder) {
	m.Name = d.String()
	m.Partitions = d.Int32()
	m.ReplicationFactor = d.Int16()
	m.Configs = d.StringMap()
}

// SimpleResponse is used by APIs that only report success or failure.
type SimpleResponse struct {
	Err    ErrorCode
	ErrMsg string
}

func (m *SimpleResponse) Encode(e *Encoder) { e.Int16(int16(m.Err)); e.String(m.ErrMsg) }
func (m *SimpleResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.ErrMsg = d.String()
}

type DeleteTopicRequest struct{ Name string }

func (m *DeleteTopicRequest) Encode(e *Encoder) { e.String(m.Name) }
func (m *DeleteTopicRequest) Decode(d *Decoder) { m.Name = d.String() }

type InitProducerIDRequest struct{}

func (m *InitProducerIDRequest) Encode(e *Encoder) {}
func (m *InitProducerIDRequest) Decode(d *Decoder) {}

type InitProducerIDResponse struct {
	Err        ErrorCode
	ProducerID int64
}

func (m *InitProducerIDResponse) Encode(e *Encoder) { e.Int16(int16(m.Err)); e.Int64(m.ProducerID) }
func (m *InitProducerIDResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.ProducerID = d.Int64()
}

// ---------------------------------------------------------------- groups

type FindCoordinatorRequest struct{ Group string }

func (m *FindCoordinatorRequest) Encode(e *Encoder) { e.String(m.Group) }
func (m *FindCoordinatorRequest) Decode(d *Decoder) { m.Group = d.String() }

type FindCoordinatorResponse struct {
	Err    ErrorCode
	NodeID int32
	Addr   string
}

func (m *FindCoordinatorResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.Int32(m.NodeID)
	e.String(m.Addr)
}

func (m *FindCoordinatorResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.NodeID = d.Int32()
	m.Addr = d.String()
}

// TopicPartitions is a topic and a list of its partitions.
type TopicPartitions struct {
	Topic      string
	Partitions []int32
}

func encodeTPs(e *Encoder, tps []TopicPartitions) {
	e.ArrayLen(len(tps))
	for _, tp := range tps {
		e.String(tp.Topic)
		e.Int32s(tp.Partitions)
	}
}

func decodeTPs(d *Decoder) []TopicPartitions {
	n := d.ArrayLen(6)
	if n == 0 {
		return nil
	}
	out := make([]TopicPartitions, n)
	for i := range out {
		out[i].Topic = d.String()
		out[i].Partitions = d.Int32s()
	}
	return out
}

// JoinGroupRequest joins (or rejoins) a consumer group. MemberID is empty
// on the first join. The call blocks until the rebalance completes.
type JoinGroupRequest struct {
	Group              string
	MemberID           string
	ClientID           string
	Topics             []string
	Strategy           string // "range" or "roundrobin"
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
}

func (m *JoinGroupRequest) Encode(e *Encoder) {
	e.String(m.Group)
	e.String(m.MemberID)
	e.String(m.ClientID)
	e.Strings(m.Topics)
	e.String(m.Strategy)
	e.Int32(m.SessionTimeoutMs)
	e.Int32(m.RebalanceTimeoutMs)
}

func (m *JoinGroupRequest) Decode(d *Decoder) {
	m.Group = d.String()
	m.MemberID = d.String()
	m.ClientID = d.String()
	m.Topics = d.Strings()
	m.Strategy = d.String()
	m.SessionTimeoutMs = d.Int32()
	m.RebalanceTimeoutMs = d.Int32()
}

type JoinGroupResponse struct {
	Err        ErrorCode
	Generation int32
	MemberID   string
	Strategy   string
	Members    int32
	Assignment []TopicPartitions
}

func (m *JoinGroupResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.Int32(m.Generation)
	e.String(m.MemberID)
	e.String(m.Strategy)
	e.Int32(m.Members)
	encodeTPs(e, m.Assignment)
}

func (m *JoinGroupResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.Generation = d.Int32()
	m.MemberID = d.String()
	m.Strategy = d.String()
	m.Members = d.Int32()
	m.Assignment = decodeTPs(d)
}

type HeartbeatRequest struct {
	Group      string
	MemberID   string
	Generation int32
}

func (m *HeartbeatRequest) Encode(e *Encoder) {
	e.String(m.Group)
	e.String(m.MemberID)
	e.Int32(m.Generation)
}

func (m *HeartbeatRequest) Decode(d *Decoder) {
	m.Group = d.String()
	m.MemberID = d.String()
	m.Generation = d.Int32()
}

type LeaveGroupRequest struct {
	Group    string
	MemberID string
}

func (m *LeaveGroupRequest) Encode(e *Encoder) { e.String(m.Group); e.String(m.MemberID) }
func (m *LeaveGroupRequest) Decode(d *Decoder) {
	m.Group = d.String()
	m.MemberID = d.String()
}

type CommitOffset struct {
	Topic     string
	Partition int32
	Offset    int64 // next offset to consume
	Metadata  string
}

type OffsetCommitRequest struct {
	Group      string
	MemberID   string // empty + Generation -1 = "simple" commit outside a group
	Generation int32
	Offsets    []CommitOffset
}

func (m *OffsetCommitRequest) Encode(e *Encoder) {
	e.String(m.Group)
	e.String(m.MemberID)
	e.Int32(m.Generation)
	e.ArrayLen(len(m.Offsets))
	for _, o := range m.Offsets {
		e.String(o.Topic)
		e.Int32(o.Partition)
		e.Int64(o.Offset)
		e.String(o.Metadata)
	}
}

func (m *OffsetCommitRequest) Decode(d *Decoder) {
	m.Group = d.String()
	m.MemberID = d.String()
	m.Generation = d.Int32()
	n := d.ArrayLen(16)
	m.Offsets = make([]CommitOffset, n)
	for i := range m.Offsets {
		o := &m.Offsets[i]
		o.Topic = d.String()
		o.Partition = d.Int32()
		o.Offset = d.Int64()
		o.Metadata = d.String()
	}
}

type OffsetFetchRequest struct {
	Group      string
	Partitions []TopicPartitions // empty = all committed partitions
}

func (m *OffsetFetchRequest) Encode(e *Encoder) { e.String(m.Group); encodeTPs(e, m.Partitions) }
func (m *OffsetFetchRequest) Decode(d *Decoder) {
	m.Group = d.String()
	m.Partitions = decodeTPs(d)
}

type OffsetFetchResponse struct {
	Err     ErrorCode
	Offsets []CommitOffset // Offset -1 = nothing committed
}

func (m *OffsetFetchResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.ArrayLen(len(m.Offsets))
	for _, o := range m.Offsets {
		e.String(o.Topic)
		e.Int32(o.Partition)
		e.Int64(o.Offset)
		e.String(o.Metadata)
	}
}

func (m *OffsetFetchResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	n := d.ArrayLen(16)
	m.Offsets = make([]CommitOffset, n)
	for i := range m.Offsets {
		o := &m.Offsets[i]
		o.Topic = d.String()
		o.Partition = d.Int32()
		o.Offset = d.Int64()
		o.Metadata = d.String()
	}
}

type ListGroupsRequest struct{}

func (m *ListGroupsRequest) Encode(e *Encoder) {}
func (m *ListGroupsRequest) Decode(d *Decoder) {}

type GroupListing struct {
	Group string
	State string
}

type ListGroupsResponse struct {
	Err    ErrorCode
	Groups []GroupListing
}

func (m *ListGroupsResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.ArrayLen(len(m.Groups))
	for _, g := range m.Groups {
		e.String(g.Group)
		e.String(g.State)
	}
}

func (m *ListGroupsResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	n := d.ArrayLen(4)
	m.Groups = make([]GroupListing, n)
	for i := range m.Groups {
		m.Groups[i].Group = d.String()
		m.Groups[i].State = d.String()
	}
}

type DescribeGroupRequest struct{ Group string }

func (m *DescribeGroupRequest) Encode(e *Encoder) { e.String(m.Group) }
func (m *DescribeGroupRequest) Decode(d *Decoder) { m.Group = d.String() }

type GroupMemberInfo struct {
	MemberID   string
	ClientID   string
	Assignment []TopicPartitions
}

type DescribeGroupResponse struct {
	Err        ErrorCode
	State      string
	Strategy   string
	Generation int32
	Members    []GroupMemberInfo
}

func (m *DescribeGroupResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.String(m.State)
	e.String(m.Strategy)
	e.Int32(m.Generation)
	e.ArrayLen(len(m.Members))
	for _, mm := range m.Members {
		e.String(mm.MemberID)
		e.String(mm.ClientID)
		encodeTPs(e, mm.Assignment)
	}
}

func (m *DescribeGroupResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.State = d.String()
	m.Strategy = d.String()
	m.Generation = d.Int32()
	n := d.ArrayLen(8)
	m.Members = make([]GroupMemberInfo, n)
	for i := range m.Members {
		m.Members[i].MemberID = d.String()
		m.Members[i].ClientID = d.String()
		m.Members[i].Assignment = decodeTPs(d)
	}
}

// ---------------------------------------------------------------- replication

type OffsetForLeaderEpochRequest struct {
	Topic       string
	Partition   int32
	LeaderEpoch int32 // the follower's latest epoch
}

func (m *OffsetForLeaderEpochRequest) Encode(e *Encoder) {
	e.String(m.Topic)
	e.Int32(m.Partition)
	e.Int32(m.LeaderEpoch)
}

func (m *OffsetForLeaderEpochRequest) Decode(d *Decoder) {
	m.Topic = d.String()
	m.Partition = d.Int32()
	m.LeaderEpoch = d.Int32()
}

type OffsetForLeaderEpochResponse struct {
	Err         ErrorCode
	LeaderEpoch int32
	EndOffset   int64
}

func (m *OffsetForLeaderEpochResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.Int32(m.LeaderEpoch)
	e.Int64(m.EndOffset)
}

func (m *OffsetForLeaderEpochResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.LeaderEpoch = d.Int32()
	m.EndOffset = d.Int64()
}

// ---------------------------------------------------------------- controller

type BrokerHeartbeatRequest struct {
	BrokerID int32
	Addr     string
	HTTPAddr string
	// ShuttingDown asks the controller to fence this broker now, moving
	// its partition leadership away before it stops (controlled shutdown).
	ShuttingDown bool
}

func (m *BrokerHeartbeatRequest) Encode(e *Encoder) {
	e.Int32(m.BrokerID)
	e.String(m.Addr)
	e.String(m.HTTPAddr)
	e.Bool(m.ShuttingDown)
}

func (m *BrokerHeartbeatRequest) Decode(d *Decoder) {
	m.BrokerID = d.Int32()
	m.Addr = d.String()
	m.HTTPAddr = d.String()
	m.ShuttingDown = d.Bool()
}

type BrokerHeartbeatResponse struct {
	Err          ErrorCode
	ControllerID int32
	Fenced       bool
}

func (m *BrokerHeartbeatResponse) Encode(e *Encoder) {
	e.Int16(int16(m.Err))
	e.Int32(m.ControllerID)
	e.Bool(m.Fenced)
}

func (m *BrokerHeartbeatResponse) Decode(d *Decoder) {
	m.Err = ErrorCode(d.Int16())
	m.ControllerID = d.Int32()
	m.Fenced = d.Bool()
}

// AlterISRRequest is sent by a partition leader to the controller to shrink
// or expand the ISR. LeaderEpoch fences requests from deposed leaders.
type AlterISRRequest struct {
	BrokerID    int32
	Topic       string
	Partition   int32
	LeaderEpoch int32
	NewISR      []int32
}

func (m *AlterISRRequest) Encode(e *Encoder) {
	e.Int32(m.BrokerID)
	e.String(m.Topic)
	e.Int32(m.Partition)
	e.Int32(m.LeaderEpoch)
	e.Int32s(m.NewISR)
}

func (m *AlterISRRequest) Decode(d *Decoder) {
	m.BrokerID = d.Int32()
	m.Topic = d.String()
	m.Partition = d.Int32()
	m.LeaderEpoch = d.Int32()
	m.NewISR = d.Int32s()
}

// ---------------------------------------------------------------- raft

type RaftVoteRequest struct {
	Term         uint64
	CandidateID  int32
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (m *RaftVoteRequest) Encode(e *Encoder) {
	e.Uint64(m.Term)
	e.Int32(m.CandidateID)
	e.Uint64(m.LastLogIndex)
	e.Uint64(m.LastLogTerm)
}

func (m *RaftVoteRequest) Decode(d *Decoder) {
	m.Term = d.Uint64()
	m.CandidateID = d.Int32()
	m.LastLogIndex = d.Uint64()
	m.LastLogTerm = d.Uint64()
}

type RaftVoteResponse struct {
	Term    uint64
	Granted bool
}

func (m *RaftVoteResponse) Encode(e *Encoder) { e.Uint64(m.Term); e.Bool(m.Granted) }
func (m *RaftVoteResponse) Decode(d *Decoder) {
	m.Term = d.Uint64()
	m.Granted = d.Bool()
}

type RaftEntry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

type RaftAppendRequest struct {
	Term         uint64
	LeaderID     int32
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []RaftEntry
	LeaderCommit uint64
}

func (m *RaftAppendRequest) Encode(e *Encoder) {
	e.Uint64(m.Term)
	e.Int32(m.LeaderID)
	e.Uint64(m.PrevLogIndex)
	e.Uint64(m.PrevLogTerm)
	e.ArrayLen(len(m.Entries))
	for _, en := range m.Entries {
		e.Uint64(en.Term)
		e.Uint64(en.Index)
		e.BytesField(en.Data)
	}
	e.Uint64(m.LeaderCommit)
}

func (m *RaftAppendRequest) Decode(d *Decoder) {
	m.Term = d.Uint64()
	m.LeaderID = d.Int32()
	m.PrevLogIndex = d.Uint64()
	m.PrevLogTerm = d.Uint64()
	n := d.ArrayLen(20)
	m.Entries = make([]RaftEntry, n)
	for i := range m.Entries {
		m.Entries[i].Term = d.Uint64()
		m.Entries[i].Index = d.Uint64()
		m.Entries[i].Data = d.BytesField()
	}
	m.LeaderCommit = d.Uint64()
}

// RaftAppendResponse. On failure ConflictIndex tells the leader where to
// back up to (an optimisation over decrementing nextIndex one at a time).
type RaftAppendResponse struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
}

func (m *RaftAppendResponse) Encode(e *Encoder) {
	e.Uint64(m.Term)
	e.Bool(m.Success)
	e.Uint64(m.MatchIndex)
	e.Uint64(m.ConflictIndex)
}

func (m *RaftAppendResponse) Decode(d *Decoder) {
	m.Term = d.Uint64()
	m.Success = d.Bool()
	m.MatchIndex = d.Uint64()
	m.ConflictIndex = d.Uint64()
}
