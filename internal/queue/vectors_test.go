package queue_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
)

type vectorFile struct {
	Name          string           `json:"name"`
	Description   string           `json:"description"`
	MinLeaseTTLMS int64            `json:"min_lease_ttl_ms"`
	Steps         []vectorStep     `json:"steps"`
	ExpectFinal   vectorFinalState `json:"expect_final"`
}

type vectorStep struct {
	NowMS           int64             `json:"now_ms"`
	Command         jsontext.Value    `json:"command"`
	ExpectStatus    int               `json:"expect_status"`
	ExpectErrorCode string            `json:"expect_error_code"`
	ExpectEvents    []queue.EventType `json:"expect_events"`
	ExpectResponse  jsontext.Value    `json:"expect_response"`
}

type vectorFinalState struct {
	Verdict            queue.Verdict     `json:"verdict"`
	Counts             queue.Counts      `json:"counts"`
	ConflictsLen       int               `json:"conflicts_len"`
	ConflictsTruncated bool              `json:"conflicts_truncated"`
	Tests              []vectorFinalTest `json:"tests"`
}

type vectorFinalTest struct {
	TestIndex        int              `json:"test_index"`
	State            queue.TestStatus `json:"state"`
	Generation       int              `json:"generation"`
	Timed            bool             `json:"timed"`
	DurationMS       int64            `json:"duration_ms"`
	HistoryLen       int              `json:"history_len"`
	HistoryTruncated bool             `json:"history_truncated"`
}

func TestSharedVectors(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "vectors", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(paths)
	if len(paths) < 15 {
		t.Fatalf("found only %d vector files; want at least 15", len(paths))
	}

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var vec vectorFile
		if err := json.Unmarshal(data, &vec, json.RejectUnknownMembers(true)); err != nil {
			t.Fatalf("%s: decode vector: %v", path, err)
		}

		t.Run(vec.Name, func(t *testing.T) {
			runVector(t, vec)
		})
	}
}

func runVector(t *testing.T, vec vectorFile) {
	t.Helper()

	lim := queue.DefaultLimits()
	if vec.MinLeaseTTLMS > 0 {
		lim.MinLeaseTTLMS = vec.MinLeaseTTLMS
	}
	st := queue.NewState(lim)
	var log []queue.Event

	for i, step := range vec.Steps {
		var hdr struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(step.Command, &hdr); err != nil {
			t.Fatalf("step %d: decode command type: %v", i, err)
		}

		if hdr.Type == "restart" {
			st.ResetActiveLeaseDeadlines(step.NowMS)
			continue
		}

		before := st.Clone()
		events, respVal, err := runVectorCommand(st, step.NowMS, hdr.Type, step.Command)
		if !st.EqualExact(before) {
			t.Fatalf("step %d (%s): Decide mutated State", i, hdr.Type)
		}

		gotStatus := 200
		gotCode := ""
		if err != nil {
			var qe *queue.Error
			if !errors.As(err, &qe) {
				t.Fatalf("step %d (%s): unexpected non-queue error %v", i, hdr.Type, err)
			}
			gotStatus = qe.HTTPStatus
			gotCode = qe.Code
		}

		if gotStatus != step.ExpectStatus {
			t.Fatalf("step %d (%s): status = %d (err=%v); want %d", i, hdr.Type, gotStatus, err, step.ExpectStatus)
		}
		if gotCode != step.ExpectErrorCode {
			t.Fatalf("step %d (%s): error code = %q; want %q", i, hdr.Type, gotCode, step.ExpectErrorCode)
		}

		gotTypes := make([]queue.EventType, len(events))
		for j, ev := range events {
			gotTypes[j] = ev.Type
		}
		wantTypes := step.ExpectEvents
		if wantTypes == nil {
			wantTypes = []queue.EventType{}
		}
		if !slices.Equal(gotTypes, wantTypes) {
			t.Fatalf("step %d (%s): events = %v; want %v", i, hdr.Type, gotTypes, wantTypes)
		}

		for _, ev := range events {
			// Round-trip every emitted event through JSON to ensure clean wire serialization.
			raw, encErr := queue.EncodeEvent(ev)
			if encErr != nil {
				t.Fatalf("step %d: encode event: %v", i, encErr)
			}
			decoded, decErr := queue.DecodeEvent(raw)
			if decErr != nil {
				t.Fatalf("step %d: decode event: %v", i, decErr)
			}
			if applyErr := st.Apply(decoded); applyErr != nil {
				t.Fatalf("step %d: apply %s: %v", i, ev.Type, applyErr)
			}
			log = append(log, decoded)
		}

		// Apply in-memory deadline extensions for heartbeats and active lease replies.
		if gotStatus == 200 {
			switch r := respVal.(type) {
			case queue.HeartbeatResponse:
				if r.Held {
					var hc queue.HeartbeatCommand
					_ = json.Unmarshal(step.Command, &hc)
					st.ExtendLease(hc.LeaseID, r.DeadlineMS)
				}
			case queue.LeaseResponse:
				if r.Status == queue.LeaseGranted {
					st.ExtendLease(r.LeaseID, r.DeadlineMS)
				}
			}
		}

		if len(step.ExpectResponse) > 0 {
			assertPartialJSON(t, i, respVal, step.ExpectResponse)
		}
	}

	if st.Verdict() != vec.ExpectFinal.Verdict {
		t.Fatalf("final verdict = %q; want %q", st.Verdict(), vec.ExpectFinal.Verdict)
	}
	if st.Counts != vec.ExpectFinal.Counts {
		t.Fatalf("final counts = %+v; want %+v", st.Counts, vec.ExpectFinal.Counts)
	}
	if len(st.Conflicts) != vec.ExpectFinal.ConflictsLen {
		t.Fatalf("final conflicts len = %d; want %d", len(st.Conflicts), vec.ExpectFinal.ConflictsLen)
	}
	if st.ConflictsTruncated != vec.ExpectFinal.ConflictsTruncated {
		t.Fatalf("final conflicts_truncated = %t; want %t", st.ConflictsTruncated, vec.ExpectFinal.ConflictsTruncated)
	}
	for _, wt := range vec.ExpectFinal.Tests {
		gt := st.Tests[wt.TestIndex]
		if gt.Status != wt.State ||
			gt.Generation != wt.Generation ||
			gt.Timed != wt.Timed ||
			gt.DurationMS != wt.DurationMS ||
			len(gt.History) != wt.HistoryLen ||
			gt.HistoryTruncated != wt.HistoryTruncated {
			t.Fatalf("test %d = {state:%s gen:%d timed:%t ms:%d hist:%d trunc:%t}; want %+v",
				wt.TestIndex, gt.Status, gt.Generation, gt.Timed, gt.DurationMS, len(gt.History), gt.HistoryTruncated, wt)
		}
	}

	// Replay emitted events onto a fresh State and check persistent equality.
	replayed := queue.NewState(lim)
	for i, ev := range log {
		if err := replayed.Apply(ev); err != nil {
			t.Fatalf("replay event %d (%s): %v", i, ev.Type, err)
		}
	}
	if !st.EqualPersistent(replayed) {
		t.Fatalf("replayed state differs from live persistent state")
	}
}

func runVectorCommand(st *queue.State, nowMS int64, typ string, raw jsontext.Value) ([]queue.Event, any, error) {
	switch typ {
	case "init":
		var cmd struct {
			Type string `json:"type"`
			queue.InitCommand
		}
		if err := json.Unmarshal(raw, &cmd, json.RejectUnknownMembers(true)); err != nil {
			return nil, nil, err
		}
		return queue.DecideInit(st, nowMS, cmd.InitCommand)

	case "lease":
		var cmd struct {
			Type string `json:"type"`
			queue.LeaseCommand
		}
		if err := json.Unmarshal(raw, &cmd, json.RejectUnknownMembers(true)); err != nil {
			return nil, nil, err
		}
		return queue.DecideLease(st, nowMS, cmd.LeaseCommand)

	case "heartbeat":
		var cmd struct {
			Type string `json:"type"`
			queue.HeartbeatCommand
		}
		if err := json.Unmarshal(raw, &cmd, json.RejectUnknownMembers(true)); err != nil {
			return nil, nil, err
		}
		return queue.DecideHeartbeat(st, nowMS, cmd.HeartbeatCommand)

	case "report":
		var cmd struct {
			Type string `json:"type"`
			queue.ReportCommand
		}
		if err := json.Unmarshal(raw, &cmd, json.RejectUnknownMembers(true)); err != nil {
			return nil, nil, err
		}
		return queue.DecideReport(st, nowMS, cmd.ReportCommand)

	case "status":
		return queue.DecideStatus(st, nowMS)

	case "list_tests":
		var cmd struct {
			Type string `json:"type"`
			queue.ListTestsCommand
		}
		if err := json.Unmarshal(raw, &cmd, json.RejectUnknownMembers(true)); err != nil {
			return nil, nil, err
		}
		return queue.DecideListTests(st, nowMS, cmd.ListTestsCommand)

	default:
		return nil, nil, &queue.Error{HTTPStatus: 400, Code: "unknown_vector_command", Message: typ}
	}
}

func assertPartialJSON(t *testing.T, step int, gotVal any, wantRaw jsontext.Value) {
	t.Helper()

	gotBytes, err := json.Marshal(gotVal)
	if err != nil {
		t.Fatalf("step %d: marshal response: %v", step, err)
	}
	var gotMap map[string]any
	if err := json.Unmarshal(gotBytes, &gotMap); err != nil {
		t.Fatalf("step %d: unmarshal response: %v", step, err)
	}
	var wantMap map[string]any
	if err := json.Unmarshal(wantRaw, &wantMap); err != nil {
		t.Fatalf("step %d: unmarshal expect_response: %v", step, err)
	}
	for k, wv := range wantMap {
		gv, ok := gotMap[k]
		if !ok {
			t.Fatalf("step %d: response missing key %q (got %s)", step, k, gotBytes)
		}
		if !reflect.DeepEqual(gv, wv) {
			t.Fatalf("step %d: response[%q] = %#v; want %#v (full: %s)", step, k, gv, wv, gotBytes)
		}
	}
}
