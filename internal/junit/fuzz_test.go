package junit_test

import (
	"bytes"
	"testing"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/timing"
)

// FuzzRead checks that no input panics and every accepted total stays in
// range.
func FuzzRead(f *testing.F) {
	f.Add([]byte(`<testsuites><testsuite file="a.js"><testcase name="t" time="1e-7"><failure/></testcase></testsuite></testsuites>`))
	f.Add([]byte(`<testsuite><testcase classname="a.spec.js"><skipped/></testcase></testsuite>`))
	f.Add([]byte(`<?xml version="1.0"?><testsuite><testcase file="a.py" time="0.5"/></testsuite>`))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			t.Skip("bounded input keeps each case fast")
		}

		for _, src := range []junit.Source{junit.FileAttribute, junit.Classname, junit.SuiteName} {
			report, err := junit.Read(bytes.NewReader(data), src)
			if err != nil {
				continue
			}

			for path, file := range report.Files {
				if ms := file.MS(); path == "" || file.Cases < 1 || ms < 0 || ms > timing.MaxDurationMS {
					t.Fatalf("accepted out-of-range total %q: %+v", path, file)
				}
			}
		}
	})
}
