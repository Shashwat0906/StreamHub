package broker

import (
	"slices"
	"sort"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

// Assignment strategies. Both are deterministic: the same members and
// partitions always give the same result, which keeps rebalances stable
// and makes them easy to test.

// AssignMember is the input to an assignment strategy.
type AssignMember struct {
	ID     string
	Topics []string
}

// AssignRange assigns, per topic, contiguous ranges of partitions to the
// members subscribed to that topic (sorted by member ID). With 7
// partitions and 3 members: [0 1 2] [3 4] [5 6]. Simple and keeps
// "partition i of topic A and partition i of topic B" on the same member,
// but can be unbalanced across many topics.
func AssignRange(members []AssignMember, partitions map[string]int32) map[string][]protocol.TopicPartitions {
	out := emptyAssignment(members)
	for _, topic := range sortedTopics(partitions) {
		subs := subscribers(members, topic)
		if len(subs) == 0 {
			continue
		}
		n := int(partitions[topic])
		per, extra := n/len(subs), n%len(subs)
		next := 0
		for i, m := range subs {
			count := per
			if i < extra {
				count++
			}
			var ps []int32
			for j := 0; j < count; j++ {
				ps = append(ps, int32(next))
				next++
			}
			if len(ps) > 0 {
				out[m] = append(out[m], protocol.TopicPartitions{Topic: topic, Partitions: ps})
			}
		}
	}
	return out
}

// AssignRoundRobin lays out every (topic, partition) in order and deals
// them to members one by one, skipping members not subscribed to a topic.
// Balanced across topics: member counts differ by at most one when all
// members subscribe to the same topics.
func AssignRoundRobin(members []AssignMember, partitions map[string]int32) map[string][]protocol.TopicPartitions {
	out := emptyAssignment(members)
	ids := memberIDs(members)
	if len(ids) == 0 {
		return out
	}
	subscribed := map[string]map[string]bool{}
	for _, m := range members {
		subscribed[m.ID] = map[string]bool{}
		for _, t := range m.Topics {
			subscribed[m.ID][t] = true
		}
	}
	perMember := map[string]map[string][]int32{}
	cursor := 0
	for _, topic := range sortedTopics(partitions) {
		for p := int32(0); p < partitions[topic]; p++ {
			// Find the next member (circularly) subscribed to this topic.
			for tries := 0; tries < len(ids); tries++ {
				m := ids[cursor%len(ids)]
				cursor++
				if subscribed[m][topic] {
					if perMember[m] == nil {
						perMember[m] = map[string][]int32{}
					}
					perMember[m][topic] = append(perMember[m][topic], p)
					break
				}
			}
		}
	}
	for m, byTopic := range perMember {
		for _, topic := range sortedKeys(byTopic) {
			out[m] = append(out[m], protocol.TopicPartitions{Topic: topic, Partitions: byTopic[topic]})
		}
	}
	return out
}

func emptyAssignment(members []AssignMember) map[string][]protocol.TopicPartitions {
	out := map[string][]protocol.TopicPartitions{}
	for _, m := range members {
		out[m.ID] = nil
	}
	return out
}

func memberIDs(members []AssignMember) []string {
	ids := make([]string, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.ID)
	}
	sort.Strings(ids)
	return ids
}

func subscribers(members []AssignMember, topic string) []string {
	var out []string
	for _, m := range members {
		if slices.Contains(m.Topics, topic) {
			out = append(out, m.ID)
		}
	}
	sort.Strings(out)
	return out
}

func sortedTopics(m map[string]int32) []string {
	out := make([]string, 0, len(m))
	for t := range m {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
