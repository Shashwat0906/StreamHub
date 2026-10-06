package dashboard

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Failure phases, in the order a broker failure plays out. The UI's
// failure page lights these up as matching events arrive.
const (
	PhaseUnavailable   = "unavailable"    // broker stops answering
	PhaseDetected      = "detected"       // controller fences it (missed heartbeats)
	PhaseLeaderFailure = "leader_failure" // partitions it led lose their leader
	PhaseElection      = "election"       // a new leader is elected from the ISR
	PhaseISR           = "isr"            // ISR shrinks/expands
	PhaseReassignment  = "reassignment"   // consumer groups rebalance; leadership moves back to preferred replicas
	PhaseRecovery      = "recovery"       // broker back, rejoins ISR
)

func p32(v int32) *int32 { return &v }

// diffSnapshots turns the change between two snapshots into timeline
// events. It only uses information both snapshots contain, so it is
// deterministic and unit-tested.
func diffSnapshots(prev, cur *Snapshot) []Event {
	if prev == nil || cur == nil || !cur.Reachable {
		return nil
	}
	var out []Event
	add := func(e Event) { out = append(out, e) }

	// Cluster reachability.
	if !prev.Reachable && cur.Reachable {
		add(Event{Kind: "cluster_reachable", Severity: "success", Message: "Dashboard reconnected to the cluster"})
	}

	// Controller.
	if prev.ControllerID != cur.ControllerID && cur.ControllerID >= 0 {
		msg := fmt.Sprintf("Broker %d is now the active controller (Raft leader)", cur.ControllerID)
		if prev.ControllerID >= 0 {
			msg = fmt.Sprintf("Controller moved from broker %d to broker %d (new Raft leader elected)", prev.ControllerID, cur.ControllerID)
		}
		add(Event{Kind: "controller_changed", Phase: PhaseElection, Severity: "warn", Message: msg, Broker: cur.ControllerID})
	}

	// Brokers.
	prevB := map[int32]BrokerView{}
	for _, b := range prev.Brokers {
		prevB[b.ID] = b
	}
	for _, b := range cur.Brokers {
		pb, ok := prevB[b.ID]
		if !ok {
			add(Event{Kind: "broker_joined", Severity: "info", Message: fmt.Sprintf("Broker %d registered (%s)", b.ID, b.Addr), Broker: b.ID})
			continue
		}
		if pb.Reachable && !b.Reachable {
			add(Event{Kind: "broker_unavailable", Phase: PhaseUnavailable, Severity: "error", Broker: b.ID,
				Message: fmt.Sprintf("Broker %d stopped responding (%s)", b.ID, firstNonEmpty(b.StatusDetail, "unreachable"))})
		}
		if !pb.Fenced && b.Fenced {
			why := "its session expired (missed heartbeats)"
			if b.Reachable {
				why = "controlled shutdown or lost contact with the controller"
			}
			add(Event{Kind: "broker_fenced", Phase: PhaseDetected, Severity: "error", Broker: b.ID,
				Message: fmt.Sprintf("Controller fenced broker %d: %s", b.ID, why)})
		}
		if pb.Fenced && !b.Fenced {
			add(Event{Kind: "broker_unfenced", Phase: PhaseRecovery, Severity: "success", Broker: b.ID,
				Message: fmt.Sprintf("Broker %d re-registered with the controller", b.ID)})
		}
		if !pb.Reachable && b.Reachable {
			add(Event{Kind: "broker_reachable", Phase: PhaseRecovery, Severity: "success", Broker: b.ID,
				Message: fmt.Sprintf("Broker %d is responding again", b.ID)})
		}
	}

	// Topics and partitions.
	prevT := map[string]TopicView{}
	for _, t := range prev.Topics {
		prevT[t.Name] = t
	}
	curNames := map[string]bool{}
	for _, t := range cur.Topics {
		curNames[t.Name] = true
		pt, ok := prevT[t.Name]
		if !ok {
			add(Event{Kind: "topic_created", Severity: "info", Topic: t.Name,
				Message: fmt.Sprintf("Topic %s created with %d partitions", t.Name, len(t.Partitions))})
			continue
		}
		for i, p := range t.Partitions {
			if i >= len(pt.Partitions) {
				break
			}
			pp := pt.Partitions[i]
			name := fmt.Sprintf("%s-%d", t.Name, p.ID)
			if pp.Leader != p.Leader {
				switch {
				case p.Leader < 0:
					add(Event{Kind: "partition_offline", Phase: PhaseLeaderFailure, Severity: "error", Topic: t.Name, Partition: p32(p.ID),
						Message: fmt.Sprintf("%s has no leader: broker %d failed and no other in-sync replica is alive", name, pp.Leader)})
				case pp.Leader < 0:
					add(Event{Kind: "leader_elected", Phase: PhaseElection, Severity: "success", Topic: t.Name, Partition: p32(p.ID), Broker: p.Leader,
						Message: fmt.Sprintf("%s back online: broker %d elected leader (epoch %d)", name, p.Leader, p.LeaderEpoch)})
				default:
					reason := "leadership moved"
					if bs := brokerStatus(cur, pp.Leader); bs != "healthy" {
						add(Event{Kind: "leader_lost", Phase: PhaseLeaderFailure, Severity: "error", Topic: t.Name, Partition: p32(p.ID), Broker: pp.Leader,
							Message: fmt.Sprintf("%s lost its leader (broker %d is %s)", name, pp.Leader, bs)})
						reason = "failover"
					} else if p.Leader == firstOr(p.Replicas, -1) {
						reason = "preferred leader restored"
					}
					if reason == "preferred leader restored" {
						// Leadership moves back to the recovered broker: the
						// partition is reassigned, not failing over.
						add(Event{Kind: "leader_rebalanced", Phase: PhaseReassignment, Severity: "info", Topic: t.Name, Partition: p32(p.ID), Broker: p.Leader,
							Message: fmt.Sprintf("%s: leadership moved back to preferred broker %d, was %d (epoch %d)", name, p.Leader, pp.Leader, p.LeaderEpoch)})
					} else {
						add(Event{Kind: "leader_elected", Phase: PhaseElection, Severity: "warn", Topic: t.Name, Partition: p32(p.ID), Broker: p.Leader,
							Message: fmt.Sprintf("%s: broker %d elected leader, was %d (%s, epoch %d)", name, p.Leader, pp.Leader, reason, p.LeaderEpoch)})
					}
				}
			}
			removed, added := setDiff(pp.ISR, p.ISR)
			if len(removed) > 0 {
				add(Event{Kind: "isr_shrink", Phase: PhaseISR, Severity: "warn", Topic: t.Name, Partition: p32(p.ID),
					Message: fmt.Sprintf("%s ISR shrank %v → %v (removed %v)", name, pp.ISR, p.ISR, removed)})
			}
			if len(added) > 0 {
				add(Event{Kind: "isr_expand", Phase: PhaseRecovery, Severity: "success", Topic: t.Name, Partition: p32(p.ID),
					Message: fmt.Sprintf("%s ISR expanded %v → %v (caught up: %v)", name, pp.ISR, p.ISR, added)})
			}
		}
	}
	for name := range prevT {
		if !curNames[name] {
			add(Event{Kind: "topic_deleted", Severity: "info", Topic: name, Message: fmt.Sprintf("Topic %s deleted", name)})
		}
	}

	// Consumer groups.
	prevG := map[string]GroupView{}
	for _, g := range prev.Groups {
		prevG[g.ID] = g
	}
	for _, g := range cur.Groups {
		pg, ok := prevG[g.ID]
		if !ok {
			continue
		}
		if pg.Generation != g.Generation && g.State == "Stable" {
			var parts []string
			for _, m := range g.Members {
				parts = append(parts, fmt.Sprintf("%s→[%s]", shortMember(m.ID), strings.Join(m.Partitions, ",")))
			}
			sort.Strings(parts)
			add(Event{Kind: "group_rebalanced", Phase: PhaseReassignment, Severity: "info", Group: g.ID,
				Message: fmt.Sprintf("Group %s rebalanced to generation %d with %d member(s): %s",
					g.ID, g.Generation, len(g.Members), strings.Join(parts, " "))})
		}
		if pg.State != g.State && g.State == "PreparingRebalance" {
			add(Event{Kind: "group_rebalancing", Phase: PhaseReassignment, Severity: "warn", Group: g.ID,
				Message: fmt.Sprintf("Group %s is rebalancing (membership changed)", g.ID)})
		}
	}
	return out
}

func brokerStatus(s *Snapshot, id int32) string {
	for _, b := range s.Brokers {
		if b.ID == id {
			return b.Status
		}
	}
	return "unknown"
}

func setDiff(before, after []int32) (removed, added []int32) {
	for _, x := range before {
		if !slices.Contains(after, x) {
			removed = append(removed, x)
		}
	}
	for _, x := range after {
		if !slices.Contains(before, x) {
			added = append(added, x)
		}
	}
	return
}

func firstOr(v []int32, def int32) int32 {
	if len(v) == 0 {
		return def
	}
	return v[0]
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// shortMember trims the random suffix of member IDs for readable events.
func shortMember(id string) string {
	if i := strings.LastIndex(id, "-"); i > 0 && len(id)-i > 8 {
		return id[:i] + "-" + id[i+1:i+5]
	}
	return id
}
