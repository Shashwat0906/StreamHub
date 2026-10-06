package dashboard

import (
	"strings"
	"testing"
)

func baseSnapshot() *Snapshot {
	return &Snapshot{
		Reachable:    true,
		ControllerID: 1,
		Brokers: []BrokerView{
			{ID: 1, Reachable: true, Status: "healthy"}, {ID: 2, Reachable: true, Status: "healthy"}, {ID: 3, Reachable: true, Status: "healthy"},
		},
		Topics: []TopicView{{Name: "orders", Partitions: []PartitionView{
			{ID: 0, Leader: 2, LeaderEpoch: 0, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3, 1}},
		}}},
	}
}

func kinds(evs []Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.Kind)
	}
	return out
}

func has(evs []Event, kind, phase string) bool {
	for _, e := range evs {
		if e.Kind == kind && (phase == "" || e.Phase == phase) {
			return true
		}
	}
	return false
}

func TestDiffNoChangeNoEvents(t *testing.T) {
	if evs := diffSnapshots(baseSnapshot(), baseSnapshot()); len(evs) != 0 {
		t.Fatalf("unexpected events %v", kinds(evs))
	}
	if evs := diffSnapshots(nil, baseSnapshot()); evs != nil {
		t.Fatal("first snapshot must not produce events")
	}
}

// The six phases the failure-simulation page shows, in order.
func TestDiffBrokerFailureSequence(t *testing.T) {
	s0 := baseSnapshot()

	// 1. Broker 2 stops answering.
	s1 := baseSnapshot()
	s1.Brokers[1].Reachable, s1.Brokers[1].Status = false, "offline"
	evs := diffSnapshots(s0, s1)
	if !has(evs, "broker_unavailable", PhaseUnavailable) {
		t.Fatalf("step 1: %v", kinds(evs))
	}

	// 2-4. Controller fences it; the partition elects broker 3 with a
	// higher epoch and the ISR shrinks.
	s2 := baseSnapshot()
	s2.Brokers[1].Reachable, s2.Brokers[1].Status = false, "offline"
	s2.Brokers[1].Fenced = true
	s2.Topics[0].Partitions[0].Leader = 3
	s2.Topics[0].Partitions[0].LeaderEpoch = 1
	s2.Topics[0].Partitions[0].ISR = []int32{3, 1}
	evs = diffSnapshots(s1, s2)
	for _, want := range [][2]string{{"broker_fenced", PhaseDetected}, {"leader_elected", PhaseElection}, {"isr_shrink", PhaseISR}} {
		if !has(evs, want[0], want[1]) {
			t.Fatalf("step 2-4 missing %v: %v", want, kinds(evs))
		}
	}
	for _, e := range evs {
		if e.Kind == "leader_elected" && !strings.Contains(e.Message, "broker 3") {
			t.Fatalf("election message: %q", e.Message)
		}
	}

	// 6. Broker 2 returns and rejoins the ISR.
	s3 := baseSnapshot()
	s3.Topics[0].Partitions[0].Leader = 3
	s3.Topics[0].Partitions[0].LeaderEpoch = 1
	evs = diffSnapshots(s2, s3)
	for _, want := range []string{"broker_reachable", "broker_unfenced", "isr_expand"} {
		if !has(evs, want, PhaseRecovery) {
			t.Fatalf("recovery missing %s: %v", want, kinds(evs))
		}
	}

	// 5. Preferred leader moves back to broker 2.
	s4 := baseSnapshot()
	s4.Topics[0].Partitions[0].LeaderEpoch = 2
	evs = diffSnapshots(s3, s4)
	if !has(evs, "leader_rebalanced", PhaseReassignment) {
		t.Fatalf("preferred leader restore: %v", kinds(evs))
	}
}

func TestDiffPartitionOfflineAndController(t *testing.T) {
	s0 := baseSnapshot()
	s1 := baseSnapshot()
	s1.ControllerID = 3
	s1.Topics[0].Partitions[0].Leader = -1
	evs := diffSnapshots(s0, s1)
	if !has(evs, "partition_offline", PhaseLeaderFailure) || !has(evs, "controller_changed", PhaseElection) {
		t.Fatalf("got %v", kinds(evs))
	}
}

func TestDiffGroupRebalance(t *testing.T) {
	s0 := baseSnapshot()
	s0.Groups = []GroupView{{ID: "g", State: "Stable", Generation: 1, Members: []MemberView{{ID: "a"}, {ID: "b"}}}}
	s1 := baseSnapshot()
	s1.Groups = []GroupView{{ID: "g", State: "Stable", Generation: 2, Members: []MemberView{{ID: "b"}}}}
	evs := diffSnapshots(s0, s1)
	if !has(evs, "group_rebalanced", "") {
		t.Fatalf("got %v", kinds(evs))
	}
}
