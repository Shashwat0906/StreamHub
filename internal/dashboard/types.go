// Package dashboard is the StreamHub web dashboard backend ("gateway").
//
// Browsers cannot speak the broker's binary TCP protocol, so this service
// sits in between: it talks to the cluster with the regular Go client and
// each broker's HTTP /v1/state endpoint, aggregates a cluster snapshot once
// per second, diffs consecutive snapshots into a timeline of events
// (broker down, leader elected, ISR changed, ...) and pushes both to the
// browser over Server-Sent Events. It also exposes REST endpoints for the
// interactive pages (produce, live stream, DLQ retry, failure simulation)
// and serves the compiled React app.
package dashboard

// Every JSON type here is mirrored in web/src/api/types.ts.

// Snapshot is the full cluster view pushed to the browser every second.
type Snapshot struct {
	Time         int64          `json:"time"` // unix ms
	Mode         string         `json:"mode"` // "managed" or "attached"
	Reachable    bool           `json:"reachable"`
	Error        string         `json:"error,omitempty"`
	ControllerID int32          `json:"controllerId"`
	Totals       Totals         `json:"totals"`
	Brokers      []BrokerView   `json:"brokers"`
	Topics       []TopicView    `json:"topics"`
	Groups       []GroupView    `json:"groups"`
	History      []HistoryPoint `json:"history"`
	Demo         DemoView       `json:"demo"`
	DLQIndex     []string       `json:"dlqIndex"` // "topic/partition/offset" of dead-lettered records (recent)
	Capabilities Capabilities   `json:"capabilities"`
	Recent       []Event        `json:"recentEvents,omitempty"` // only in /api/snapshot
}

// Capabilities tells the UI which controls the backend supports.
type Capabilities struct {
	BrokerControl   bool   `json:"brokerControl"`   // kill/stop/start brokers (managed mode only)
	ConsumerControl bool   `json:"consumerControl"` // demo consumers (always)
	Traffic         bool   `json:"traffic"`         // demo traffic generator (always)
	Note            string `json:"note,omitempty"`
}

// Totals feed the overview cards.
type Totals struct {
	Brokers            int     `json:"brokers"`
	HealthyBrokers     int     `json:"healthyBrokers"`
	OfflineBrokers     int     `json:"offlineBrokers"`
	Topics             int     `json:"topics"`
	Partitions         int     `json:"partitions"`
	OfflinePartitions  int     `json:"offlinePartitions"`
	UnderReplicated    int     `json:"underReplicated"`
	ActiveGroups       int     `json:"activeGroups"`
	MessagesProduced   int64   `json:"messagesProduced"` // sum of log-end offsets of user topics
	MessagesConsumed   int64   `json:"messagesConsumed"` // sum of committed offsets of all groups
	ProducedPerSec     float64 `json:"producedPerSec"`
	ConsumedPerSec     float64 `json:"consumedPerSec"`
	ConsumerLag        int64   `json:"consumerLag"`
	ProduceErrors      int64   `json:"produceErrors"`      // broker produce_errors counters (since broker start)
	ProcessingFailures int64   `json:"processingFailures"` // demo consumers' failed records
	DLQMessages        int64   `json:"dlqMessages"`        // records in *.dlq topics
}

// BrokerView is one broker as seen by the dashboard.
type BrokerView struct {
	ID                int32            `json:"id"`
	Addr              string           `json:"addr"`
	HTTPAddr          string           `json:"httpAddr"`
	Status            string           `json:"status"` // healthy | fenced | offline
	StatusDetail      string           `json:"statusDetail,omitempty"`
	Reachable         bool             `json:"reachable"` // its /v1/state answered
	Fenced            bool             `json:"fenced"`    // fenced in committed cluster metadata
	IsController      bool             `json:"isController"`
	LeaderPartitions  []string         `json:"leaderPartitions"`
	ReplicaPartitions []string         `json:"replicaPartitions"`
	InMsgPerSec       float64          `json:"inMsgPerSec"`
	OutMsgPerSec      float64          `json:"outMsgPerSec"`
	StorageBytes      int64            `json:"storageBytes"`
	UptimeSeconds     float64          `json:"uptimeSeconds"`
	RaftRole          string           `json:"raftRole,omitempty"`
	RaftTerm          uint64           `json:"raftTerm,omitempty"`
	Counters          map[string]int64 `json:"counters,omitempty"`
	Process           *ProcessView     `json:"process,omitempty"` // managed mode only
}

// ProcessView describes a broker process launched by the dashboard.
type ProcessView struct {
	PID     int    `json:"pid"`
	State   string `json:"state"` // running | killed | stopped | exited | starting
	LogPath string `json:"logPath"`
}

// TopicView is a topic with its partitions.
type TopicView struct {
	Name       string            `json:"name"`
	Internal   bool              `json:"internal"`
	DLQ        bool              `json:"dlq"`
	Configs    map[string]string `json:"configs,omitempty"`
	Partitions []PartitionView   `json:"partitions"`
	Messages   int64             `json:"messages"` // sum of log-end offsets
}

// PartitionView is the state of one partition, combining metadata (leader,
// replicas, ISR) with the leader's live log state.
type PartitionView struct {
	ID              int32            `json:"id"`
	Leader          int32            `json:"leader"`
	LeaderEpoch     int32            `json:"leaderEpoch"`
	Replicas        []int32          `json:"replicas"`
	ISR             []int32          `json:"isr"`
	LogStart        int64            `json:"logStart"`
	LogEnd          int64            `json:"logEnd"`        // latest offset + 1
	HighWatermark   int64            `json:"highWatermark"` // -1 if the leader is unreachable
	SizeBytes       int64            `json:"sizeBytes"`
	Lag             int64            `json:"lag"` // summed over groups that committed offsets here
	GroupLag        map[string]int64 `json:"groupLag,omitempty"`
	Offline         bool             `json:"offline"`
	UnderReplicated bool             `json:"underReplicated"`
}

// GroupView is a consumer group.
type GroupView struct {
	ID         string            `json:"id"`
	State      string            `json:"state"`
	Strategy   string            `json:"strategy"`
	Generation int32             `json:"generation"`
	Members    []MemberView      `json:"members"`
	Offsets    []GroupOffsetView `json:"offsets"`
	TotalLag   int64             `json:"totalLag"`
	Error      string            `json:"error,omitempty"`
	// Stale: the coordinator could not be reached, these are the last
	// known values (kept for up to 30 s during coordinator failover).
	Stale      bool  `json:"stale,omitempty"`
	StaleSince int64 `json:"staleSince,omitempty"`
}

// MemberView is one group member and its assignment.
type MemberView struct {
	ID         string   `json:"id"`
	ClientID   string   `json:"clientId"`
	Partitions []string `json:"partitions"` // "topic-N"
}

// GroupOffsetView is a committed offset with lag.
type GroupOffsetView struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"`
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
	Owner     string `json:"owner,omitempty"` // member currently assigned
}

// HistoryPoint is one sample for the time-series charts.
type HistoryPoint struct {
	T              int64              `json:"t"`
	ProducedPerSec float64            `json:"producedPerSec"`
	ConsumedPerSec float64            `json:"consumedPerSec"`
	Lag            int64              `json:"lag"`
	BrokerIn       map[string]float64 `json:"brokerIn"` // broker id -> msgs/s in
	HealthyBrokers int                `json:"healthyBrokers"`
}

// Event is one entry in the cluster timeline.
type Event struct {
	ID        int64  `json:"id"`
	Time      int64  `json:"time"` // unix ms
	Kind      string `json:"kind"`
	Phase     string `json:"phase,omitempty"` // unavailable | detected | leader_failure | election | isr | reassignment | recovery
	Severity  string `json:"severity"`        // info | warn | error | success
	Message   string `json:"message"`
	Broker    int32  `json:"broker,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Partition *int32 `json:"partition,omitempty"`
	Group     string `json:"group,omitempty"`
}

// DemoView describes the demo traffic generator and demo consumers.
type DemoView struct {
	Traffic   TrafficView    `json:"traffic"`
	Consumers []ConsumerView `json:"consumers"`
}

// TrafficView is the traffic generator state.
type TrafficView struct {
	Running     bool    `json:"running"`
	Topic       string  `json:"topic"`
	Rate        int     `json:"rate"`        // messages per second
	FailureRate float64 `json:"failureRate"` // fraction marked to fail processing
	Sent        int64   `json:"sent"`
	Errors      int64   `json:"errors"`
	LastError   string  `json:"lastError,omitempty"`
}

// ConsumerView is one demo consumer (a real group member).
type ConsumerView struct {
	ID         string   `json:"id"`
	Group      string   `json:"group"`
	Topic      string   `json:"topic"`
	Status     string   `json:"status"` // starting | running | crashed | stopped
	MemberID   string   `json:"memberId"`
	Generation int32    `json:"generation"`
	Partitions []string `json:"partitions"`
	DelayMs    int      `json:"delayMs"`
	Processed  int64    `json:"processed"`
	Failed     int64    `json:"failed"`
	LastError  string   `json:"lastError,omitempty"`
}

// StreamMessage is one record in the live message stream.
type StreamMessage struct {
	ID        string `json:"id"` // topic/partition/offset
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Size      int    `json:"size"`
	Truncated bool   `json:"truncated"`
	// Ack describes the producer acknowledgement. Every record a consumer
	// can see is below the high-watermark (committed to the ISR); for
	// records produced through the dashboard we also know the acks mode.
	Ack string `json:"ack"`
}

// ProduceRequest is the body of POST /api/produce.
type ProduceRequest struct {
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Acks      string `json:"acks"` // "0" | "1" | "all"
	Partition *int32 `json:"partition,omitempty"`
}

// ProduceResult is the response of POST /api/produce.
type ProduceResult struct {
	MessageID string  `json:"messageId"`
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Offset    int64   `json:"offset"`
	Acks      string  `json:"acks"`
	Status    string  `json:"status"` // acknowledged | sent-no-ack | failed
	Error     string  `json:"error,omitempty"`
	LatencyMs float64 `json:"latencyMs"`
	Timestamp int64   `json:"timestamp"`
}

// DLQEntry is one dead-lettered message.
type DLQEntry struct {
	ID              string `json:"id"` // dlqTopic/partition/offset
	DLQTopic        string `json:"dlqTopic"`
	DLQPartition    int32  `json:"dlqPartition"`
	DLQOffset       int64  `json:"dlqOffset"`
	OriginalTopic   string `json:"originalTopic"`
	Partition       int32  `json:"partition"`
	Offset          int64  `json:"offset"`
	Key             string `json:"key"`
	Payload         string `json:"payload"`
	Error           string `json:"error"`
	RetryCount      int    `json:"retryCount"`
	ConsumerGroup   string `json:"consumerGroup"`
	FailedAt        int64  `json:"failedAt"`
	Retried         bool   `json:"retried"`
	Eligible        bool   `json:"eligible"`
	IneligibleCause string `json:"ineligibleCause,omitempty"`
}
