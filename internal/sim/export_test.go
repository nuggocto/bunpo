package sim_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/queuerunner"
)

type exportedSeed struct {
	Seed          int64          `json:"seed"`
	Name          string         `json:"name"`
	MinLeaseTTLMS int64          `json:"min_lease_ttl_ms"`
	MaxTests      int            `json:"max_tests,omitempty"`
	MaxRunners    int            `json:"max_runners,omitempty"`
	Steps         []exportedStep `json:"steps"`
}

type exportedStep struct {
	Kind            string         `json:"kind"` // "command" or "restart"
	NowMS           int64          `json:"now_ms"`
	Command         jsontext.Value `json:"command,omitempty"`
	ExpectStatus    int            `json:"expect_status,omitempty"`
	ExpectErrorCode string         `json:"expect_error_code,omitempty"`
	ExpectResponse  jsontext.Value `json:"expect_response,omitempty"`
	ExpectEvents    []queue.Event  `json:"expect_events,omitempty"`
	ReloadedEvents  []queue.Event  `json:"reloaded_events,omitempty"`
	LogSynced       bool           `json:"log_synced,omitempty"`
}

type simExporter struct {
	dir           string
	name          string
	seed          int64
	minLeaseTTLMS int64
	stopped       bool
	steps         []exportedStep
	appliedEvents []queue.Event
}

func newSimExporter(dir, name string, seed int64, minLeaseTTLMS int64) *simExporter {
	if dir == "" {
		return nil
	}

	return &simExporter{
		dir:           dir,
		name:          name,
		seed:          seed,
		minLeaseTTLMS: minLeaseTTLMS,
	}
}

func (e *simExporter) recordCommand(nowMS int64, req queuerunner.Request, status int, body []byte, newEvents []queue.Event) error {
	if e == nil || e.stopped {
		return nil
	}

	if status >= 500 {
		return nil
	}

	cmdJSON, err := encodeExportCommand(req)
	if err != nil {
		return err
	}

	var errCode string
	var respJSON jsontext.Value
	if status == 200 {
		respJSON, err = extractCoreResponse(req.Kind, body)
		if err != nil {
			return err
		}
	} else {
		var env struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if uErr := json.Unmarshal(body, &env); uErr != nil {
			return fmt.Errorf("decode error envelope: %w", uErr)
		}

		errCode = env.Error.Code
	}

	evs := newEvents
	if evs == nil {
		evs = []queue.Event{}
	}

	e.appliedEvents = append(e.appliedEvents, evs...)

	e.steps = append(e.steps, exportedStep{
		Kind:            "command",
		NowMS:           nowMS,
		Command:         cmdJSON,
		ExpectStatus:    status,
		ExpectErrorCode: errCode,
		ExpectResponse:  respJSON,
		ExpectEvents:    evs,
	})
	return nil
}

func (e *simExporter) recordRestart(nowMS int64, broken bool, diskBytes []byte) error {
	if e == nil || e.stopped {
		return nil
	}

	if broken {
		e.stopped = true
		return nil
	}

	reloaded := parseDiskEventsSlice(diskBytes)
	if reloaded == nil {
		reloaded = []queue.Event{}
	}

	synced := len(reloaded) == len(e.appliedEvents)
	if synced {
		a, errA := json.Marshal(reloaded, json.Deterministic(true))
		b, errB := json.Marshal(e.appliedEvents, json.Deterministic(true))
		synced = errA == nil && errB == nil && string(a) == string(b)
	}

	e.appliedEvents = append([]queue.Event(nil), reloaded...)

	e.steps = append(e.steps, exportedStep{
		Kind:           "restart",
		NowMS:          nowMS,
		ReloadedEvents: reloaded,
		LogSynced:      synced,
	})
	return nil
}

func (e *simExporter) stop() {
	if e != nil {
		e.stopped = true
	}
}

func (e *simExporter) flush() error {
	if e == nil || len(e.steps) == 0 {
		return nil
	}

	if err := os.MkdirAll(e.dir, 0o755); err != nil {
		return err
	}

	out := exportedSeed{
		Seed:          e.seed,
		Name:          e.name,
		MinLeaseTTLMS: e.minLeaseTTLMS,
		Steps:         e.steps,
	}
	data, err := json.Marshal(out, json.Deterministic(true))
	if err != nil {
		return err
	}

	path := filepath.Join(e.dir, e.name+".json")
	return os.WriteFile(path, data, 0o644)
}

func encodeExportCommand(req queuerunner.Request) (jsontext.Value, error) {
	switch req.Kind {
	case queuerunner.ReqInit:
		return json.Marshal(struct {
			Type string `json:"type"`
			queue.InitCommand
		}{Type: "init", InitCommand: *req.Init}, json.Deterministic(true))
	case queuerunner.ReqLease:
		return json.Marshal(struct {
			Type string `json:"type"`
			queue.LeaseCommand
		}{Type: "lease", LeaseCommand: *req.Lease}, json.Deterministic(true))
	case queuerunner.ReqHeartbeat:
		return json.Marshal(struct {
			Type string `json:"type"`
			queue.HeartbeatCommand
		}{Type: "heartbeat", HeartbeatCommand: *req.Heartbeat}, json.Deterministic(true))
	case queuerunner.ReqReport:
		return json.Marshal(struct {
			Type string `json:"type"`
			queue.ReportCommand
		}{Type: "report", ReportCommand: *req.Report}, json.Deterministic(true))
	case queuerunner.ReqStatus:
		return jsontext.Value(`{"type":"status"}`), nil
	case queuerunner.ReqListTests:
		return json.Marshal(struct {
			Type string `json:"type"`
			queue.ListTestsCommand
		}{Type: "list_tests", ListTestsCommand: *req.ListTests}, json.Deterministic(true))
	default:
		return nil, fmt.Errorf("unknown request kind %q", req.Kind)
	}
}

func extractCoreResponse(kind queuerunner.RequestKind, body []byte) (jsontext.Value, error) {
	switch kind {
	case queuerunner.ReqInit:
		var r queueapi.InitResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, err
		}

		return json.Marshal(r.InitResponse, json.Deterministic(true))
	case queuerunner.ReqLease:
		var r queueapi.LeaseResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, err
		}

		return json.Marshal(r.LeaseResponse, json.Deterministic(true))
	case queuerunner.ReqHeartbeat:
		var r queueapi.HeartbeatResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, err
		}

		return json.Marshal(r.HeartbeatResponse, json.Deterministic(true))
	case queuerunner.ReqReport:
		var r queueapi.ReportResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, err
		}

		return json.Marshal(r.ReportResponse, json.Deterministic(true))
	case queuerunner.ReqStatus:
		var r queueapi.StatusResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, err
		}

		return json.Marshal(r.StatusResponse, json.Deterministic(true))
	case queuerunner.ReqListTests:
		var r queueapi.ListTestsResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, err
		}

		return json.Marshal(r.ListTestsResponse, json.Deterministic(true))
	default:
		return nil, fmt.Errorf("unknown request kind %q", kind)
	}
}
