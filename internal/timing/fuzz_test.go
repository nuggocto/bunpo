package timing_test

import (
	"bytes"
	"testing"

	"github.com/nuggocto/bunpo/internal/timing"
)

// FuzzDecode checks that no input panics and every accepted file round-trips
// to the same bytes.
func FuzzDecode(f *testing.F) {
	f.Add([]byte(`{"version":1,"run":4,"tests":{"a.py":{"samples":[10,20],"last_seen":4}}}`))
	f.Add([]byte(`{"version":1,"run":0}`))
	f.Add([]byte(`{"version":2}`))
	f.Add([]byte(`{"version":1,"run":1,"tests":{"a":{"samples":[1,2,3,4,5,6],"last_seen":1}}}`))
	f.Add([]byte(`[]`))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			t.Skip("bounded input keeps each case fast")
		}

		file, err := timing.Decode(data)
		if err != nil {
			return
		}

		first, err := timing.Encode(file)
		if err != nil {
			t.Fatalf("Decode accepted a file that Encode rejects: %v", err)
		}

		again, err := timing.Decode(first)
		if err != nil {
			t.Fatalf("Decode rejects Encode output: %v", err)
		}

		second, err := timing.Encode(again)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(first, second) {
			t.Fatalf("round trip changed the file:\n%s\n%s", first, second)
		}
	})
}
