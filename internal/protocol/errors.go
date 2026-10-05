package protocol

import "fmt"

// ErrorCode is carried as int16 at the start of every response body.
// Values loosely follow Kafka's numbering so they are easy to look up.
type ErrorCode int16

const (
	ErrNone                     ErrorCode = 0
	ErrUnknown                  ErrorCode = -1
	ErrOffsetOutOfRange         ErrorCode = 1
	ErrCorruptMessage           ErrorCode = 2
	ErrUnknownTopicOrPartition  ErrorCode = 3
	ErrLeaderNotAvailable       ErrorCode = 5
	ErrNotLeader                ErrorCode = 6
	ErrRequestTimedOut          ErrorCode = 7
	ErrMessageTooLarge          ErrorCode = 10
	ErrNetwork                  ErrorCode = 13
	ErrCoordinatorNotAvailable  ErrorCode = 15
	ErrNotCoordinator           ErrorCode = 16
	ErrInvalidTopic             ErrorCode = 17
	ErrNotEnoughReplicas        ErrorCode = 19
	ErrNotEnoughReplicasAfter   ErrorCode = 20
	ErrIllegalGeneration        ErrorCode = 22
	ErrUnknownMemberID          ErrorCode = 25
	ErrRebalanceInProgress      ErrorCode = 27
	ErrTopicAlreadyExists       ErrorCode = 36
	ErrInvalidPartitions        ErrorCode = 37
	ErrInvalidReplicationFactor ErrorCode = 38
	ErrNotController            ErrorCode = 41
	ErrInvalidRequest           ErrorCode = 42
	ErrOutOfOrderSequence       ErrorCode = 45
	ErrDuplicateSequence        ErrorCode = 46
	ErrUnknownProducerID        ErrorCode = 59
	ErrGroupIDNotFound          ErrorCode = 69
	ErrFencedLeaderEpoch        ErrorCode = 74
	ErrUnknownLeaderEpoch       ErrorCode = 75
	ErrBrokerFenced             ErrorCode = 80
	ErrInvalidISR               ErrorCode = 81
)

var errorNames = map[ErrorCode]string{
	ErrNone:                     "NONE",
	ErrUnknown:                  "UNKNOWN",
	ErrOffsetOutOfRange:         "OFFSET_OUT_OF_RANGE",
	ErrCorruptMessage:           "CORRUPT_MESSAGE",
	ErrUnknownTopicOrPartition:  "UNKNOWN_TOPIC_OR_PARTITION",
	ErrLeaderNotAvailable:       "LEADER_NOT_AVAILABLE",
	ErrNotLeader:                "NOT_LEADER",
	ErrRequestTimedOut:          "REQUEST_TIMED_OUT",
	ErrMessageTooLarge:          "MESSAGE_TOO_LARGE",
	ErrNetwork:                  "NETWORK_EXCEPTION",
	ErrCoordinatorNotAvailable:  "COORDINATOR_NOT_AVAILABLE",
	ErrNotCoordinator:           "NOT_COORDINATOR",
	ErrInvalidTopic:             "INVALID_TOPIC",
	ErrNotEnoughReplicas:        "NOT_ENOUGH_REPLICAS",
	ErrNotEnoughReplicasAfter:   "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
	ErrIllegalGeneration:        "ILLEGAL_GENERATION",
	ErrUnknownMemberID:          "UNKNOWN_MEMBER_ID",
	ErrRebalanceInProgress:      "REBALANCE_IN_PROGRESS",
	ErrTopicAlreadyExists:       "TOPIC_ALREADY_EXISTS",
	ErrInvalidPartitions:        "INVALID_PARTITIONS",
	ErrInvalidReplicationFactor: "INVALID_REPLICATION_FACTOR",
	ErrNotController:            "NOT_CONTROLLER",
	ErrInvalidRequest:           "INVALID_REQUEST",
	ErrOutOfOrderSequence:       "OUT_OF_ORDER_SEQUENCE",
	ErrDuplicateSequence:        "DUPLICATE_SEQUENCE",
	ErrUnknownProducerID:        "UNKNOWN_PRODUCER_ID",
	ErrGroupIDNotFound:          "GROUP_ID_NOT_FOUND",
	ErrFencedLeaderEpoch:        "FENCED_LEADER_EPOCH",
	ErrUnknownLeaderEpoch:       "UNKNOWN_LEADER_EPOCH",
	ErrBrokerFenced:             "BROKER_FENCED",
	ErrInvalidISR:               "INVALID_ISR",
}

func (c ErrorCode) String() string {
	if s, ok := errorNames[c]; ok {
		return s
	}
	return fmt.Sprintf("ERROR_%d", int16(c))
}

// Retriable reports whether a client should refresh metadata and retry.
func (c ErrorCode) Retriable() bool {
	switch c {
	case ErrLeaderNotAvailable, ErrNotLeader, ErrRequestTimedOut, ErrNetwork,
		ErrCoordinatorNotAvailable, ErrNotCoordinator, ErrNotEnoughReplicas,
		ErrNotEnoughReplicasAfter, ErrNotController, ErrFencedLeaderEpoch,
		ErrUnknownLeaderEpoch, ErrUnknownTopicOrPartition, ErrBrokerFenced:
		return true
	}
	return false
}

// Error lets an ErrorCode be used as a Go error.
type Error struct {
	Code ErrorCode
	Msg  string
}

func (e *Error) Error() string {
	if e.Msg != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Msg)
	}
	return e.Code.String()
}

// AsError converts a non-zero code into an error (nil for ErrNone).
func (c ErrorCode) AsError(msg string) error {
	if c == ErrNone {
		return nil
	}
	return &Error{Code: c, Msg: msg}
}
