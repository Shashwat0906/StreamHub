package metadata

import (
	"slices"
	"testing"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

func apply(t *testing.T, s *Store, c Command) Result {
	t.Helper()
	return s.Apply(s.AppliedIndex()+1, c.Encode()).(Result)
}

func TestAssignReplicasBalanced(t *testing.T) {
	brokers := []int32{1, 2, 3}
	a, err := AssignReplicas(brokers, 6, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	leaders := map[int32]int{}
	for p, reps := range a {
		if len(reps) != 3 {
			t.Fatalf("p%d: %v", p, reps)
		}
		uniq := slices.Clone(reps)
		slices.Sort(uniq)
		if len(slices.Compact(uniq)) != 3 {
			t.Fatalf("p%d has duplicate replicas %v", p, reps)
		}
		leaders[reps[0]]++
	}
	for _, b := range brokers {
		if leaders[b] != 2 {
			t.Fatalf("leaders not balanced: %v", leaders)
		}
	}
	if _, err := AssignReplicas(brokers, 1, 4, 0); err == nil {
		t.Fatal("rf > brokers must fail")
	}
}

func setup(t *testing.T) *Store {
	s := NewStore()
	for _, id := range []int32{1, 2, 3} {
		apply(t, s, Command{Type: CmdRegisterBroker, BrokerID: id, Addr: "x"})
	}
	if r := apply(t, s, Command{Type: CmdCreateTopic, Topic: "t", Assignments: [][]int32{{1, 2, 3}}}); r.Err != protocol.ErrNone {
		t.Fatal(r.Msg)
	}
	return s
}

func TestFenceLeaderElectsFromISR(t *testing.T) {
	s := setup(t)
	p, _ := s.Partition("t", 0)
	if p.Leader != 1 || !slices.Equal(p.ISR, []int32{1, 2, 3}) || p.LeaderEpoch != 0 {
		t.Fatalf("initial %+v", p)
	}
	apply(t, s, Command{Type: CmdFenceBroker, BrokerID: 1})
	p, _ = s.Partition("t", 0)
	if p.Leader != 2 || p.LeaderEpoch != 1 || slices.Contains(p.ISR, 1) {
		t.Fatalf("after fencing leader: %+v", p)
	}
}

func TestNoUncleanElection(t *testing.T) {
	s := setup(t)
	// ISR shrinks to just the leader (followers fell behind).
	r := apply(t, s, Command{Type: CmdAlterISR, BrokerID: 1, Topic: "t", Partition: 0, LeaderEpoch: 0, ISR: []int32{1}})
	if r.Err != protocol.ErrNone {
		t.Fatal(r.Msg)
	}
	apply(t, s, Command{Type: CmdFenceBroker, BrokerID: 1})
	p, _ := s.Partition("t", 0)
	// 2 and 3 are alive but out of sync: electing them could lose
	// acknowledged writes, so the partition goes offline instead.
	if p.Leader != NoLeader || !slices.Equal(p.ISR, []int32{1}) {
		t.Fatalf("unclean election happened: %+v", p)
	}
	// When the last ISR member returns, it becomes leader again.
	apply(t, s, Command{Type: CmdRegisterBroker, BrokerID: 1, Addr: "x"})
	p, _ = s.Partition("t", 0)
	if p.Leader != 1 {
		t.Fatalf("last ISR member not re-elected: %+v", p)
	}
}

func TestAlterISRFencesStaleLeader(t *testing.T) {
	s := setup(t)
	apply(t, s, Command{Type: CmdFenceBroker, BrokerID: 1}) // leader -> 2, epoch 1
	// Old leader 1 (epoch 0) tries to shrink the ISR: rejected.
	r := apply(t, s, Command{Type: CmdAlterISR, BrokerID: 1, Topic: "t", Partition: 0, LeaderEpoch: 0, ISR: []int32{1}})
	if r.Err != protocol.ErrFencedLeaderEpoch {
		t.Fatalf("stale AlterISR accepted: %+v", r)
	}
	// Fenced broker cannot be added back to the ISR.
	r = apply(t, s, Command{Type: CmdAlterISR, BrokerID: 2, Topic: "t", Partition: 0, LeaderEpoch: 1, ISR: []int32{1, 2, 3}})
	if r.Err != protocol.ErrInvalidISR {
		t.Fatalf("fenced broker re-added: %+v", r)
	}
	r = apply(t, s, Command{Type: CmdAlterISR, BrokerID: 2, Topic: "t", Partition: 0, LeaderEpoch: 1, ISR: []int32{3}})
	if r.Err != protocol.ErrInvalidISR {
		t.Fatalf("ISR without leader accepted: %+v", r)
	}
}

func TestDeterministicReplay(t *testing.T) {
	cmds := []Command{
		{Type: CmdRegisterBroker, BrokerID: 1, Addr: "a"},
		{Type: CmdRegisterBroker, BrokerID: 2, Addr: "b"},
		{Type: CmdCreateTopic, Topic: "x", Assignments: [][]int32{{1, 2}, {2, 1}}},
		{Type: CmdCreateTopic, Topic: "y", Assignments: [][]int32{{2, 1}}},
		{Type: CmdFenceBroker, BrokerID: 2},
		{Type: CmdAllocateProducer},
		{Type: CmdRegisterBroker, BrokerID: 2, Addr: "b"},
	}
	a, b := NewStore(), NewStore()
	for i, c := range cmds {
		a.Apply(uint64(i+1), c.Encode())
		b.Apply(uint64(i+1), c.Encode())
	}
	ta, tb := a.Topics(), b.Topics()
	for i := range ta {
		for j := range ta[i].Partitions {
			pa, pb := ta[i].Partitions[j], tb[i].Partitions[j]
			if pa.Leader != pb.Leader || pa.LeaderEpoch != pb.LeaderEpoch || !slices.Equal(pa.ISR, pb.ISR) {
				t.Fatalf("replicas diverged: %+v vs %+v", pa, pb)
			}
		}
	}
}
