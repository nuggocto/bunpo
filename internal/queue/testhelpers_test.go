package queue_test

import (
	"encoding/json/v2"
	"reflect"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
)

// cloneState deep-copies s via JSON so adding a field to State cannot be
// silently forgotten in a hand-written clone helper.
func cloneState(t testing.TB, s *queue.State) *queue.State {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	var out queue.State
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	return &out
}

func normalizeEmptySlices(s *queue.State) {
	if len(s.ActiveLeases) == 0 {
		s.ActiveLeases = nil
	}
	if len(s.RunnerOrder) == 0 {
		s.RunnerOrder = nil
	}
	if len(s.Conflicts) == 0 {
		s.Conflicts = nil
	}
	if len(s.Tests) == 0 {
		s.Tests = nil
	}
	if len(s.Leases) == 0 {
		s.Leases = nil
	}
	if len(s.DealOrder) == 0 {
		s.DealOrder = nil
	}
	if len(s.Runners) == 0 {
		s.Runners = nil
	}
	for i := range s.Tests {
		if len(s.Tests[i].Attempts) == 0 {
			s.Tests[i].Attempts = nil
		}
		if len(s.Tests[i].History) == 0 {
			s.Tests[i].History = nil
		}
	}
}

// equalExact checks full structural equality via reflect.DeepEqual (including
// ephemeral in-memory lease deadlines).
func equalExact(t testing.TB, a, b *queue.State) bool {
	t.Helper()
	ca, cb := cloneState(t, a), cloneState(t, b)
	normalizeEmptySlices(ca)
	normalizeEmptySlices(cb)
	return reflect.DeepEqual(ca, cb)
}

// equalPersistent checks equality of persisted state via reflect.DeepEqual,
// ignoring ephemeral in-memory Lease.DeadlineMS extensions from heartbeats.
func equalPersistent(t testing.TB, a, b *queue.State) bool {
	t.Helper()
	ca, cb := cloneState(t, a), cloneState(t, b)
	normalizeEmptySlices(ca)
	normalizeEmptySlices(cb)
	for i := range ca.Leases {
		ca.Leases[i].DeadlineMS = 0
	}
	for i := range cb.Leases {
		cb.Leases[i].DeadlineMS = 0
	}
	return reflect.DeepEqual(ca, cb)
}
