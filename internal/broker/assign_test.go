package broker

import (
	"fmt"
	"testing"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

func flatten(a map[string][]protocol.TopicPartitions) map[string][]string {
	out := map[string][]string{}
	for m, tps := range a {
		for _, tp := range tps {
			for _, p := range tp.Partitions {
				out[m] = append(out[m], fmt.Sprintf("%s-%d", tp.Topic, p))
			}
		}
	}
	return out
}

// every partition is assigned to exactly one subscribed member
func checkComplete(t *testing.T, a map[string][]protocol.TopicPartitions, members []AssignMember, parts map[string]int32) {
	t.Helper()
	seen := map[string]string{}
	sub := map[string]map[string]bool{}
	for _, m := range members {
		sub[m.ID] = map[string]bool{}
		for _, tp := range m.Topics {
			sub[m.ID][tp] = true
		}
	}
	for m, tps := range a {
		for _, tp := range tps {
			if !sub[m][tp.Topic] {
				t.Fatalf("%s got unsubscribed topic %s", m, tp.Topic)
			}
			for _, p := range tp.Partitions {
				key := fmt.Sprintf("%s-%d", tp.Topic, p)
				if prev, dup := seen[key]; dup {
					t.Fatalf("%s assigned to both %s and %s", key, prev, m)
				}
				seen[key] = m
			}
		}
	}
	for topic, n := range parts {
		anySub := false
		for _, m := range members {
			if sub[m.ID][topic] {
				anySub = true
			}
		}
		for p := int32(0); p < n; p++ {
			if _, ok := seen[fmt.Sprintf("%s-%d", topic, p)]; !ok && anySub {
				t.Fatalf("%s-%d unassigned", topic, p)
			}
		}
	}
}

func TestAssignRange(t *testing.T) {
	members := []AssignMember{{"c", []string{"a"}}, {"a", []string{"a"}}, {"b", []string{"a"}}}
	parts := map[string]int32{"a": 7}
	got := flatten(AssignRange(members, parts))
	want := map[string]string{"a": "[a-0 a-1 a-2]", "b": "[a-3 a-4]", "c": "[a-5 a-6]"}
	for m, w := range want {
		if fmt.Sprint(got[m]) != w {
			t.Fatalf("member %s got %v want %s", m, got[m], w)
		}
	}
	checkComplete(t, AssignRange(members, parts), members, parts)
}

func TestAssignRangeMoreMembersThanPartitions(t *testing.T) {
	members := []AssignMember{{"a", []string{"t"}}, {"b", []string{"t"}}, {"c", []string{"t"}}}
	a := AssignRange(members, map[string]int32{"t": 2})
	if len(a["c"]) != 0 || len(flatten(a)["a"]) != 1 || len(flatten(a)["b"]) != 1 {
		t.Fatalf("got %v", flatten(a))
	}
}

func TestAssignRoundRobinBalanced(t *testing.T) {
	members := []AssignMember{{"m1", []string{"x", "y"}}, {"m2", []string{"x", "y"}}, {"m3", []string{"x", "y"}}}
	parts := map[string]int32{"x": 4, "y": 4}
	a := AssignRoundRobin(members, parts)
	checkComplete(t, a, members, parts)
	f := flatten(a)
	for _, m := range []string{"m1", "m2", "m3"} {
		if n := len(f[m]); n < 2 || n > 3 {
			t.Fatalf("unbalanced: %v", f)
		}
	}
	// Range on the same input is less balanced across topics (4+4 over 3
	// members gives m1 two extra), which is why round-robin exists.
	r := flatten(AssignRange(members, parts))
	if len(r["m1"]) != 4 {
		t.Fatalf("range expected m1=4, got %v", r)
	}
}

func TestAssignRoundRobinMixedSubscriptions(t *testing.T) {
	members := []AssignMember{{"a", []string{"x"}}, {"b", []string{"x", "y"}}}
	parts := map[string]int32{"x": 3, "y": 2}
	a := AssignRoundRobin(members, parts)
	checkComplete(t, a, members, parts)
	for _, tp := range a["a"] {
		if tp.Topic == "y" {
			t.Fatal("a is not subscribed to y")
		}
	}
}

func TestAssignDeterministic(t *testing.T) {
	members := []AssignMember{{"z", []string{"t"}}, {"y", []string{"t"}}}
	a1 := fmt.Sprint(flatten(AssignRoundRobin(members, map[string]int32{"t": 5})))
	members[0], members[1] = members[1], members[0]
	a2 := fmt.Sprint(flatten(AssignRoundRobin(members, map[string]int32{"t": 5})))
	if a1 != a2 {
		t.Fatalf("depends on input order: %s vs %s", a1, a2)
	}
}
