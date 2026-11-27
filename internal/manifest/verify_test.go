package manifest_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/manifest"
)

var digestB = strings.Repeat("b", 64)

func m(index, machines int, digest string, tests ...string) manifest.Manifest {
	return manifest.Manifest{Index: index, Machines: machines, Digest: digest, Tests: tests}
}

func TestVerify(t *testing.T) {
	input := []string{"a.py", "b.py", "c.py", "d.py"}
	tests := []struct {
		name      string
		manifests []manifest.Manifest
		input     []string
		// want holds a substring of each expected problem, in order.
		want []string
	}{
		{
			name: "clean split",
			manifests: []manifest.Manifest{
				m(0, 3, digestA, "a.py", "d.py"), m(1, 3, digestA, "b.py"), m(2, 3, digestA, "c.py"),
			},
			input: input,
		},

		{
			name:      "clean split with an empty shard",
			manifests: []manifest.Manifest{m(0, 2, digestA, "a.py"), m(1, 2, digestA)},
			input:     []string{"a.py"},
		},

		{
			name:  "no manifests",
			input: input,
			want:  []string{"no manifests found"},
		},

		{
			name: "one machine used stale timings",
			manifests: []manifest.Manifest{
				m(0, 3, digestA, "a.py", "d.py"), m(1, 3, digestB, "b.py", "c.py"), m(2, 3, digestA, "c.py"),
			},
			input: input,
			want: []string{
				"machine 1 split a different input or used different timings than the other machines: aaaaaaaaaaaa… on machines 0, 2; bbbbbbbbbbbb… on machine 1",
				"1 test assigned to more than one machine: c.py (machines 1, 2)",
			},
		},

		{
			name:      "digests differ with no majority",
			manifests: []manifest.Manifest{m(0, 2, digestA, "a.py", "b.py"), m(1, 2, digestB, "c.py", "d.py")},
			input:     input,
			want:      []string{"machines split different inputs or used different timings"},
		},

		{
			name:      "a machine is missing",
			manifests: []manifest.Manifest{m(0, 3, digestA, "a.py", "d.py"), m(2, 3, digestA, "c.py")},
			input:     input,
			want:      []string{"no manifest from machine 1 of 3", "1 test in the input but assigned to no machine: b.py"},
		},

		{
			name:      "a machine reported twice",
			manifests: []manifest.Manifest{m(0, 2, digestA, "a.py", "b.py"), m(0, 2, digestA, "a.py", "b.py"), m(1, 2, digestA, "c.py", "d.py")},
			input:     input,
			want:      []string{"2 manifests from machine 0", "2 tests assigned to more than one machine: a.py (machines 0, 0), b.py (machines 0, 0)"},
		},

		{
			name:      "machines disagree on the count",
			manifests: []manifest.Manifest{m(0, 2, digestA, "a.py", "b.py"), m(1, 2, digestA, "c.py", "d.py"), m(2, 3, digestA)},
			input:     input,
			want:      []string{"machines disagree on the machine count: 2 on machines 0, 1; 3 on machine 2"},
		},

		{
			name:      "manifest index out of range",
			manifests: []manifest.Manifest{m(0, 2, digestA, "a.py", "b.py"), m(1, 2, digestA, "c.py", "d.py"), m(5, 2, digestA)},
			input:     input,
			want:      []string{"manifest index 5 is out of range for 2 machines"},
		},

		{
			name:      "assigned tests outside the input",
			manifests: []manifest.Manifest{m(0, 1, digestA, "a.py", "b.py", "c.py", "d.py", "gone.py")},
			input:     input,
			want:      []string{"1 test assigned but not in the input: gone.py"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := manifest.Verify(tc.manifests, tc.input)
			if len(got) != len(tc.want) {
				t.Fatalf("Verify = %q; want %d problems matching %q", got, len(tc.want), tc.want)
			}

			for i, w := range tc.want {
				if !strings.Contains(got[i], w) {
					t.Fatalf("problem %d = %q; want it to contain %q", i, got[i], w)
				}
			}
		})
	}
}

func TestVerifyCapsLongLists(t *testing.T) {
	var input []string
	for i := range 25 {
		input = append(input, fmt.Sprintf("t%02d.py", i))
	}

	got := manifest.Verify([]manifest.Manifest{m(0, 1, digestA)}, input)
	if len(got) != 1 || !strings.HasPrefix(got[0], "25 tests in the input but assigned to no machine: t00.py") ||
		!strings.HasSuffix(got[0], "t09.py, and 15 more") {
		t.Fatalf("Verify = %q; want 10 names and a count of the rest", got)
	}
}

func TestVerifyDoesNotDependOnOrder(t *testing.T) {
	ms := []manifest.Manifest{
		m(0, 3, digestA, "a.py", "extra2.py"),
		m(1, 3, digestB, "b.py", "extra1.py"),
		m(2, 3, digestA, "c.py"),
	}
	want := manifest.Verify(ms, []string{"a.py", "b.py", "c.py", "x.py", "y.py"})
	slices.Reverse(ms)

	if got := manifest.Verify(ms, []string{"y.py", "x.py", "c.py", "b.py", "a.py"}); !slices.Equal(got, want) {
		t.Fatalf("reordered input gave %q; want %q", got, want)
	}
}
