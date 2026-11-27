package timing_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/timing"
)

func emptyFile() timing.File {
	return timing.File{Tests: map[string]timing.Entry{}}
}

func mustMerge(t *testing.T, base timing.File, reports ...timing.Report) timing.File {
	t.Helper()

	f, err := timing.Merge(base, reports)
	if err != nil {
		t.Fatal(err)
	}

	return f
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name    string
		base    timing.File
		reports []timing.Report
		want    timing.File
	}{
		{
			name: "records new tests from every report",
			base: emptyFile(),
			reports: []timing.Report{
				{"a.py": {Timed: true, MS: 120}},
				{"b.py": {Timed: true, MS: 80}},
			},
			want: timing.File{Run: 1, Tests: map[string]timing.Entry{
				"a.py": {Samples: []int64{120}, LastSeen: 1},
				"b.py": {Samples: []int64{80}, LastSeen: 1},
			}},
		},

		{
			name: "a failed file keeps its history and is marked seen",
			base: timing.File{Run: 1, Tests: map[string]timing.Entry{
				"a.py": {Samples: []int64{100, 110}, LastSeen: 1},
			}},
			reports: []timing.Report{{"a.py": {Timed: false}}},
			want: timing.File{Run: 2, Tests: map[string]timing.Entry{
				"a.py": {Samples: []int64{100, 110}, LastSeen: 2},
			}},
		},

		{
			name:    "a failure on the first run is seen with no samples",
			base:    emptyFile(),
			reports: []timing.Report{{"a.py": {Timed: false}}},
			want: timing.File{Run: 1, Tests: map[string]timing.Entry{
				"a.py": {LastSeen: 1},
			}},
		},

		{
			name: "an unseen test keeps its last-seen run",
			base: timing.File{Run: 4, Tests: map[string]timing.Entry{
				"a.py": {Samples: []int64{5}, LastSeen: 2},
			}},
			reports: []timing.Report{{"b.py": {Timed: true, MS: 9}}},
			want: timing.File{Run: 5, Tests: map[string]timing.Entry{
				"a.py": {Samples: []int64{5}, LastSeen: 2},
				"b.py": {Samples: []int64{9}, LastSeen: 5},
			}},
		},

		{
			name:    "an empty report from an empty shard still counts as a run",
			base:    emptyFile(),
			reports: []timing.Report{{}},
			want:    timing.File{Run: 1, Tests: map[string]timing.Entry{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertFile(t, mustMerge(t, tc.base, tc.reports...), tc.want)
		})
	}
}

func TestMergeRejects(t *testing.T) {
	tests := []struct {
		name    string
		base    timing.File
		reports []timing.Report
		wantErr error
		wantMsg string
	}{
		{
			name:    "a test in two reports",
			base:    emptyFile(),
			reports: []timing.Report{{"a.py": {Timed: true, MS: 1}}, {"a.py": {Timed: true, MS: 2}}},
			wantErr: timing.ErrInvalidReport,
			wantMsg: "a.py",
		},

		{
			name:    "a negative duration",
			base:    emptyFile(),
			reports: []timing.Report{{"a.py": {Timed: true, MS: -1}}},
			wantErr: timing.ErrInvalidReport,
			wantMsg: "a.py",
		},

		{
			name:    "a duration above the limit",
			base:    emptyFile(),
			reports: []timing.Report{{"a.py": {Timed: true, MS: timing.MaxDurationMS + 1}}},
			wantErr: timing.ErrInvalidReport,
			wantMsg: "a.py",
		},

		{
			name:    "no reports",
			base:    emptyFile(),
			reports: nil,
			wantErr: timing.ErrInvalidReport,
		},

		{
			name:    "a non-canonical identifier in a report",
			base:    emptyFile(),
			reports: []timing.Report{{"../a.py": {Timed: true, MS: 1}}},
			wantErr: timing.ErrInvalidReport,
			wantMsg: "../a.py",
		},

		{
			name: "an invalid base file",
			base: timing.File{Run: 1, Tests: map[string]timing.Entry{
				"a.py": {Samples: []int64{1}, LastSeen: 2},
			}},
			reports: []timing.Report{{}},
			wantErr: timing.ErrInvalidFile,
			wantMsg: "a.py",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := timing.Merge(tc.base, tc.reports)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v; want %v", err, tc.wantErr)
			}

			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}
}

func TestMergeKeepsLastFiveSamples(t *testing.T) {
	f := emptyFile()
	for ms := int64(1); ms <= 7; ms++ {
		f = mustMerge(t, f, timing.Report{"a.py": {Timed: true, MS: ms}})
	}

	if got := f.Tests["a.py"].Samples; !slices.Equal(got, []int64{3, 4, 5, 6, 7}) {
		t.Fatalf("samples = %v; want the last five, [3 4 5 6 7]", got)
	}
}

func TestMergePrunesAfterThirtyUnseenRuns(t *testing.T) {
	f := mustMerge(t, emptyFile(), timing.Report{"old.py": {Timed: true, MS: 5}})
	other := timing.Report{"other.py": {Timed: true, MS: 5}}
	for range timing.PruneAfterRuns {
		f = mustMerge(t, f, other)
	}

	if _, ok := f.Tests["old.py"]; !ok {
		t.Fatalf("old.py dropped at run %d, after %d unseen runs; want it kept", f.Run, timing.PruneAfterRuns)
	}

	f = mustMerge(t, f, other)
	if _, ok := f.Tests["old.py"]; ok {
		t.Fatalf("old.py kept at run %d, after %d unseen runs; want it dropped", f.Run, timing.PruneAfterRuns+1)
	}
}

func TestMergeFailingFileIsNotPruned(t *testing.T) {
	f := mustMerge(t, emptyFile(), timing.Report{"flaky.py": {Timed: true, MS: 5}})
	for range timing.PruneAfterRuns + 5 {
		f = mustMerge(t, f, timing.Report{"flaky.py": {Timed: false}})
	}

	if got := f.Tests["flaky.py"].Samples; !slices.Equal(got, []int64{5}) {
		t.Fatalf("samples = %v; want the passing sample [5] kept while the file fails", got)
	}
}

func TestMergeIsOrderIndependent(t *testing.T) {
	base := timing.File{Run: 3, Tests: map[string]timing.Entry{
		"a.py": {Samples: []int64{1, 2}, LastSeen: 3},
		"b.py": {Samples: []int64{9}, LastSeen: 2},
	}}
	r1 := timing.Report{"a.py": {Timed: true, MS: 3}, "c.py": {Timed: false}}
	r2 := timing.Report{"b.py": {Timed: true, MS: 8}}
	r3 := timing.Report{}
	want := mustMerge(t, base, r1, r2, r3)
	assertFile(t, mustMerge(t, base, r3, r2, r1), want)
	assertFile(t, mustMerge(t, base, r2, r1, r3), want)
}

func TestMergeLeavesBaseUnchanged(t *testing.T) {
	samples := []int64{1, 2, 3, 4, 5}
	base := timing.File{Run: 1, Tests: map[string]timing.Entry{
		"a.py": {Samples: samples, LastSeen: 1},
	}}
	mustMerge(t, base, timing.Report{"a.py": {Timed: true, MS: 6}, "b.py": {Timed: true, MS: 1}})

	if base.Run != 1 || len(base.Tests) != 1 || !slices.Equal(samples, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("base changed to %+v", base)
	}
}
