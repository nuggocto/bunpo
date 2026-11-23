package queue_test

import (
	"encoding/json/v2"
	"reflect"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
)

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

func nilEmpty[T any](s *[]T) {
	if len(*s) == 0 {
		*s = nil
	}
}

func normalizeEmptySlices(s *queue.State) {
	nilEmpty(&s.ActiveLeases)
	nilEmpty(&s.RunnerOrder)
	nilEmpty(&s.Conflicts)
	nilEmpty(&s.Tests)
	nilEmpty(&s.Leases)
	nilEmpty(&s.DealOrder)
	if len(s.Runners) == 0 {
		s.Runners = nil
	}
	for i := range s.Tests {
		nilEmpty(&s.Tests[i].Attempts)
		nilEmpty(&s.Tests[i].History)
	}
}

func equalStates(t testing.TB, a, b *queue.State, ignoreDeadlines bool) bool {
	t.Helper()
	ca, cb := cloneState(t, a), cloneState(t, b)
	normalizeEmptySlices(ca)
	normalizeEmptySlices(cb)
	if ignoreDeadlines {
		for i := range ca.Leases {
			ca.Leases[i].DeadlineMS = 0
		}
		for i := range cb.Leases {
			cb.Leases[i].DeadlineMS = 0
		}
	}
	return reflect.DeepEqual(ca, cb)
}

func equalExact(t testing.TB, a, b *queue.State) bool      { return equalStates(t, a, b, false) }
func equalPersistent(t testing.TB, a, b *queue.State) bool { return equalStates(t, a, b, true) }
