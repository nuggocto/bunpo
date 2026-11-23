package queue_test

import (
	"bytes"
	"encoding/json/v2"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
)

func FuzzEventReplay(f *testing.F) {
	// Seed with valid JSONL event streams and malformed fragments.
	s := queue.NewState(queue.DefaultLimits())
	if evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID:       "seed-1",
		RunnerID:      "r0",
		Config:        defaultConfig(),
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{int64Ptr(100)},
	}); err == nil {
		if raw, err := queue.EncodeEvent(evs[0]); err == nil {
			f.Add(raw)
		}
	}
	f.Add([]byte(`{"type":"expired","at_ms":1000,"expired":{"lease_id":1}}`))
	f.Add([]byte(`{"type":"finalized","at_ms":2000,"finalized":{"reason":"deadline"}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			return
		}
		st := queue.NewState(queue.DefaultLimits())
		for line := range bytes.SplitSeq(data, []byte{'\n'}) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			ev, err := queue.DecodeEvent(line)
			if err != nil {
				continue
			}
			before := st.Clone()
			if err := st.Apply(ev); err != nil {
				if !st.EqualExact(before) {
					t.Fatalf("Apply mutated state on error: %v", err)
				}
			}
		}
		// Basic structural sanity: Counts.Total always equals len(Tests).
		if st.Counts.Total != len(st.Tests) {
			t.Fatalf("Counts.Total = %d != len(Tests) = %d", st.Counts.Total, len(st.Tests))
		}
	})
}

func FuzzDecide(f *testing.F) {
	f.Add([]byte(`{"type":"init","build_id":"b1","runner_id":"r1","config":{"machines":1,"lease_ttl_ms":10000,"build_timeout_ms":60000,"max_attempts":3,"max_batch":10},"tests":["a.py"],"predictions_ms":[100]}`), int64(1000))
	f.Add([]byte(`{"type":"lease","runner_id":"r1","request_id":"q1"}`), int64(1000))
	f.Add([]byte(`{"type":"heartbeat","runner_id":"r1","lease_id":1}`), int64(1000))
	f.Add([]byte(`{"type":"report","runner_id":"r1","lease_id":1,"results":[{"attempt_id":"0.1","test_index":0,"generation":1,"outcome":"passed","timed":true,"duration_ms":50}]}`), int64(1000))
	f.Add([]byte(`{"type":"status"}`), int64(1000))
	f.Add([]byte(`{"type":"list_tests","cursor":0,"limit":10}`), int64(1000))

	f.Fuzz(func(t *testing.T, data []byte, nowMS int64) {
		if len(data) > 64<<10 {
			return
		}
		// Run against both an empty state and a small initialized state.
		states := []*queue.State{queue.NewState(queue.DefaultLimits())}
		initState := queue.NewState(queue.DefaultLimits())
		if evs, _, err := queue.DecideInit(initState, 1000, queue.InitCommand{
			BuildID:       "b1",
			RunnerID:      "r1",
			Config:        defaultConfig(),
			Tests:         []string{"a.py", "b.py"},
			PredictionsMS: []*int64{int64Ptr(100), nil},
		}); err == nil {
			_ = initState.Apply(evs[0])
			if evs2, _, err := queue.DecideLease(initState, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"}); err == nil {
				for _, ev := range evs2 {
					_ = initState.Apply(ev)
				}
			}
		}
		states = append(states, initState)

		var hdr struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &hdr); err != nil {
			return
		}

		for _, st := range states {
			before := st.Clone()
			evs, _, _ := runVectorCommand(st, nowMS, hdr.Type, data)
			if !st.EqualExact(before) {
				t.Fatalf("Decide(%s) mutated state", hdr.Type)
			}
			for _, ev := range evs {
				if err := st.Apply(ev); err != nil {
					t.Fatalf("Apply(%s) of Decide-emitted event failed: %v", ev.Type, err)
				}
			}
		}
	})
}
