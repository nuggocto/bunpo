package split_test

import (
	"crypto/sha256"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/split"
	"github.com/nuggocto/bunpo/internal/timing"
)

const minute = 60 * 1000

func inventory(t *testing.T, ids ...string) split.Inventory {
	t.Helper()

	inv, err := split.NewInventory(ids, split.POSIX)
	if err != nil {
		t.Fatal(err)
	}

	return inv
}

func mustSplit(t *testing.T, inv split.Inventory, durations map[string]int64, machines int) split.Plan {
	t.Helper()

	p, err := split.Split(inv, durations, machines)
	if err != nil {
		t.Fatal(err)
	}

	return p
}

func TestSplit(t *testing.T) {
	tests := []struct {
		name       string
		ids        []string
		durations  map[string]int64
		machines   int
		wantShards [][]string
		wantLoads  []int64
	}{
		{
			// From PROJECT.md. Splitting by name gives 22 and 12.
			name: "worked example",
			ids:  []string{"a.py", "b.py", "c.py", "d.py", "e.py", "f.py"},
			durations: map[string]int64{
				"a.py": 9 * minute, "b.py": 7 * minute, "c.py": 6 * minute,
				"d.py": 5 * minute, "e.py": 4 * minute, "f.py": 3 * minute,
			},
			machines:   2,
			wantShards: [][]string{{"a.py", "d.py", "f.py"}, {"b.py", "c.py", "e.py"}},
			wantLoads:  []int64{17 * minute, 17 * minute},
		},

		{
			name:       "equal durations deal in path order to the lowest machine",
			ids:        []string{"d.py", "b.py", "c.py", "a.py"},
			durations:  map[string]int64{"a.py": 5, "b.py": 5, "c.py": 5, "d.py": 5},
			machines:   2,
			wantShards: [][]string{{"a.py", "c.py"}, {"b.py", "d.py"}},
			wantLoads:  []int64{10, 10},
		},

		{
			name:       "empty list",
			ids:        nil,
			machines:   3,
			wantShards: [][]string{{}, {}, {}},
			wantLoads:  []int64{0, 0, 0},
		},

		{
			name:       "one test",
			ids:        []string{"only.py"},
			durations:  map[string]int64{"only.py": 42},
			machines:   1,
			wantShards: [][]string{{"only.py"}},
			wantLoads:  []int64{42},
		},

		{
			name:       "more machines than tests",
			ids:        []string{"a.py", "b.py"},
			durations:  map[string]int64{"a.py": 3, "b.py": 8},
			machines:   4,
			wantShards: [][]string{{"b.py"}, {"a.py"}, {}, {}},
			wantLoads:  []int64{8, 3, 0, 0},
		},

		{
			name:       "no history deals evenly",
			ids:        []string{"a.py", "b.py", "c.py", "d.py", "e.py"},
			machines:   2,
			wantShards: [][]string{{"a.py", "c.py", "e.py"}, {"b.py", "d.py"}},
			wantLoads:  []int64{0, 0},
		},

		{
			// With ties broken by index alone, all four land on machine 0.
			name:       "zero durations spread by test count",
			ids:        []string{"a.py", "b.py", "c.py", "d.py"},
			durations:  map[string]int64{"a.py": 0, "b.py": 0, "c.py": 0, "d.py": 0},
			machines:   2,
			wantShards: [][]string{{"a.py", "c.py"}, {"b.py", "d.py"}},
			wantLoads:  []int64{0, 0},
		},

		{
			name:       "a recorded zero stays shorter than one millisecond",
			ids:        []string{"a.py", "b.py"},
			durations:  map[string]int64{"a.py": 0, "b.py": 1},
			machines:   2,
			wantShards: [][]string{{"b.py"}, {"a.py"}},
			wantLoads:  []int64{1, 0},
		},

		{
			name:       "equal loads prefer the machine with fewer tests",
			ids:        []string{"v.py", "w.py", "x.py", "y.py", "z.py"},
			durations:  map[string]int64{"x.py": 2, "y.py": 1, "z.py": 1, "v.py": 0, "w.py": 0},
			machines:   3,
			wantShards: [][]string{{"x.py"}, {"v.py", "y.py"}, {"w.py", "z.py"}},
			wantLoads:  []int64{2, 1, 1},
		},

		{
			// LPT and the optimum are 6, but the usual lower bound is 4: four
			// thirds of that bound is no ceiling for LPT.
			name:       "four jobs of three on three machines",
			ids:        []string{"a.py", "b.py", "c.py", "d.py"},
			durations:  map[string]int64{"a.py": 3, "b.py": 3, "c.py": 3, "d.py": 3},
			machines:   3,
			wantShards: [][]string{{"a.py", "d.py"}, {"b.py"}, {"c.py"}},
			wantLoads:  []int64{6, 3, 3},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := mustSplit(t, inventory(t, tc.ids...), tc.durations, tc.machines)

			if !slices.EqualFunc(p.Shards, tc.wantShards, slices.Equal) {
				t.Fatalf("Shards = %q; want %q", p.Shards, tc.wantShards)
			}

			if !slices.Equal(p.Loads, tc.wantLoads) {
				t.Fatalf("Loads = %v; want %v", p.Loads, tc.wantLoads)
			}
		})
	}
}

func TestSplitFillsUnknownWithMedian(t *testing.T) {
	tests := []struct {
		name        string
		durations   map[string]int64
		wantFill    int64
		wantUnknown int
	}{
		{"odd count of known times", map[string]int64{"a.py": 10, "b.py": 40, "c.py": 20}, 20, 1},
		{"even count rounds half up", map[string]int64{"a.py": 10, "b.py": 21}, 16, 2},
		{"times for tests outside the input are ignored", map[string]int64{"a.py": 10, "gone.py": 1000}, 10, 3},
		{"no history counts every test as zero", nil, 0, 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := inventory(t, "a.py", "b.py", "c.py", "new.py")
			p := mustSplit(t, inv, tc.durations, 2)

			if p.FillMS != tc.wantFill || p.Unknown != tc.wantUnknown {
				t.Fatalf("FillMS, Unknown = %d, %d; want %d, %d", p.FillMS, p.Unknown, tc.wantFill, tc.wantUnknown)
			}

			i := slices.Index(p.Tests, "new.py")
			if p.Durations[i] != tc.wantFill {
				t.Fatalf("new.py predicted at %d ms; want %d", p.Durations[i], tc.wantFill)
			}
		})
	}
}

func TestSplitRejects(t *testing.T) {
	inv := inventory(t, "a.py")

	tests := []struct {
		name      string
		durations map[string]int64
		machines  int
		wantErr   error
	}{
		{"no machines", nil, 0, split.ErrMachines},
		{"at the machine limit", nil, split.MaxMachines, nil},
		{"too many machines", nil, split.MaxMachines + 1, split.ErrMachines},
		{"duration at the limit", map[string]int64{"a.py": timing.MaxDurationMS}, 1, nil},
		{"negative duration", map[string]int64{"a.py": -1}, 1, split.ErrInvalidDuration},
		{"duration above the limit", map[string]int64{"a.py": timing.MaxDurationMS + 1}, 1, split.ErrInvalidDuration},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := split.Split(inv, tc.durations, tc.machines); !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v; want %v", err, tc.wantErr)
			}
		})
	}
}

func TestPlanWarnings(t *testing.T) {
	// A 100-minute file and nine one-minute files cannot balance on two machines.
	dominant := map[string]int64{"big.py": 100 * minute}
	for _, c := range "abcdefghi" {
		dominant[string(c)+".py"] = minute
	}

	type warnings struct {
		dominant bool
		largest  int64
		total    int64
		empty    int
	}

	tests := []struct {
		name      string
		durations map[string]int64
		machines  int
		want      warnings
	}{
		{"dominant file", dominant, 2, warnings{true, 100 * minute, 109 * minute, 0}},
		{"balanced suite", map[string]int64{"a.py": 5, "b.py": 5, "c.py": 5}, 2, warnings{false, 5, 15, 0}},
		{"more machines than tests", map[string]int64{"a.py": 5, "b.py": 5}, 5, warnings{true, 5, 10, 3}},
		{"files recorded at zero", map[string]int64{"a.py": 0, "b.py": 0}, 2, warnings{false, 0, 0, 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ids := slices.Collect(maps.Keys(tc.durations))
			p := mustSplit(t, inventory(t, ids...), tc.durations, tc.machines)

			got := warnings{p.Dominant(), p.LargestMS(), p.TotalMS(), p.EmptyShards()}
			if got != tc.want {
				t.Fatalf("got %+v; want %+v", got, tc.want)
			}
		})
	}
}

// TestDigestEncoding pins the digest bytes from PROJECT.md. Changing them
// needs a new AlgorithmVersion.
func TestDigestEncoding(t *testing.T) {
	p := mustSplit(t, inventory(t, "b.py", "a.py"), map[string]int64{"a.py": 10, "b.py": 0}, 2)

	want := sha256.Sum256([]byte("bunpo split 1\nmachines 2\na.py\x0010\nb.py\x000\n"))
	if p.Digest != want {
		t.Fatalf("Digest = %x; want %x", p.Digest, want)
	}
}

func TestDigestCoversEverythingTheSplitDependsOn(t *testing.T) {
	base := mustSplit(t, inventory(t, "a.py", "b.py"), map[string]int64{"a.py": 10, "b.py": 20}, 2)

	tests := []struct {
		name      string
		ids       []string
		durations map[string]int64
		machines  int
		same      bool
	}{
		{"same inputs", []string{"b.py", "a.py"}, map[string]int64{"a.py": 10, "b.py": 20}, 2, true},
		{"extra timing for a test outside the input", []string{"a.py", "b.py"}, map[string]int64{"a.py": 10, "b.py": 20, "gone.py": 5}, 2, true},
		{"different machine count", []string{"a.py", "b.py"}, map[string]int64{"a.py": 10, "b.py": 20}, 3, false},
		{"different duration", []string{"a.py", "b.py"}, map[string]int64{"a.py": 10, "b.py": 21}, 2, false},
		{"different input", []string{"a.py", "c.py"}, map[string]int64{"a.py": 10, "c.py": 20}, 2, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := mustSplit(t, inventory(t, tc.ids...), tc.durations, tc.machines)
			if (p.Digest == base.Digest) != tc.same {
				t.Fatalf("digest equal = %t; want %t", p.Digest == base.Digest, tc.same)
			}
		})
	}
}

func TestPlanDoesNotShareInventory(t *testing.T) {
	inv := inventory(t, "a.py", "b.py")
	p := mustSplit(t, inv, nil, 1)

	p.Tests[0] = "changed"
	p.Shards[0][0] = "changed"

	if got := inv.IDs(); !slices.Equal(got, []string{"a.py", "b.py"}) {
		t.Fatalf("changing the plan changed the inventory to %q", got)
	}

	if again := mustSplit(t, inv, nil, 1); strings.Join(again.Tests, ",") != "a.py,b.py" {
		t.Fatalf("a second split saw %q", again.Tests)
	}
}
