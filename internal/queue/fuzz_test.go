package queue_test

import (
	"bytes"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
)

func FuzzEventReplay(f *testing.F) {
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

			before := cloneState(t, st)
			if err := st.Apply(ev); err != nil {
				if !equalExact(t, st, before) {
					t.Fatalf("Apply mutated state on error: %v", err)
				}
			}
		}

		if st.Counts.Total != len(st.Tests) {
			t.Fatalf("Counts.Total = %d != len(Tests) = %d", st.Counts.Total, len(st.Tests))
		}
	})
}
