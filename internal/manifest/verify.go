package manifest

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// maxListed caps the tests one problem names.
const maxListed = 10

// Verify compares every manifest with the canonical input and returns each
// problem, in a fixed order. None means the assignment is verified: every
// machine present once, one digest, no test twice, the input covered. It says
// nothing about what actually ran.
func Verify(manifests []Manifest, input []string) []string {
	if len(manifests) == 0 {
		return []string{"no manifests found"}
	}

	problems := machineProblems(manifests)

	byDigest := make(map[string][]int)
	for _, m := range manifests {
		byDigest[m.Digest] = append(byDigest[m.Digest], m.Index)
	}

	if len(byDigest) > 1 {
		problems = append(problems, digestProblem(byDigest))
	}

	return append(problems, coverageProblems(manifests, input)...)
}

// machineProblems checks that every manifest agrees on the machine count and
// that each machine appears exactly once.
func machineProblems(manifests []Manifest) []string {
	var problems []string

	byCount := make(map[int][]int)
	perIndex := make(map[int]int)
	var badIndex []string
	for _, m := range manifests {
		byCount[m.Machines] = append(byCount[m.Machines], m.Index)
		perIndex[m.Index]++
		if m.Machines < 1 || m.Index < 0 || m.Index >= m.Machines {
			badIndex = append(badIndex, fmt.Sprintf("manifest index %d is out of range for %d machines", m.Index, m.Machines))
		}
	}

	slices.Sort(badIndex)
	problems = append(problems, badIndex...)

	if len(byCount) > 1 {
		var parts []string
		for _, n := range slices.Sorted(maps.Keys(byCount)) {
			parts = append(parts, fmt.Sprintf("%d on %s", n, machineList(byCount[n])))
		}

		problems = append(problems, "machines disagree on the machine count: "+strings.Join(parts, "; "))
	}

	machines := largestGroup(byCount)
	for i := range machines {
		if perIndex[i] == 0 {
			problems = append(problems, fmt.Sprintf("no manifest from machine %d of %d", i, machines))
		}
	}

	for _, i := range slices.Sorted(maps.Keys(perIndex)) {
		if perIndex[i] > 1 {
			problems = append(problems, fmt.Sprintf("%d manifests from machine %d", perIndex[i], i))
		}
	}

	return problems
}

// coverageProblems checks that the shares cover the input exactly once.
func coverageProblems(manifests []Manifest, input []string) []string {
	owners := make(map[string][]int)
	for _, m := range manifests {
		for _, id := range m.Tests {
			owners[id] = append(owners[id], m.Index)
		}
	}

	inInput := make(map[string]bool, len(input))
	var twice, missing, extra []string

	for _, id := range input {
		if inInput[id] {
			continue
		}

		inInput[id] = true
		if owners[id] == nil {
			missing = append(missing, id)
		}
	}

	slices.Sort(missing)

	for _, id := range slices.Sorted(maps.Keys(owners)) {
		if len(owners[id]) > 1 {
			twice = append(twice, fmt.Sprintf("%s (%s)", id, machineList(owners[id])))
		}

		if !inInput[id] {
			extra = append(extra, id)
		}
	}

	var problems []string
	if len(twice) > 0 {
		problems = append(problems, listProblem("assigned to more than one machine", twice))
	}

	if len(missing) > 0 {
		problems = append(problems, listProblem("in the input but assigned to no machine", missing))
	}

	if len(extra) > 0 {
		problems = append(problems, listProblem("assigned but not in the input", extra))
	}

	return problems
}

// digestProblem names the machines outside the largest digest group, which
// is what points at a stale cache.
func digestProblem(byDigest map[string][]int) string {
	digests := slices.SortedFunc(maps.Keys(byDigest), func(a, b string) int {
		return cmp.Or(cmp.Compare(len(byDigest[b]), len(byDigest[a])), cmp.Compare(a, b))
	})

	var parts []string
	for _, d := range digests {
		parts = append(parts, fmt.Sprintf("%s… on %s", d[:min(12, len(d))], machineList(byDigest[d])))
	}

	detail := strings.Join(parts, "; ")
	if len(byDigest[digests[0]]) > len(byDigest[digests[1]]) {
		var odd []int
		for _, d := range digests[1:] {
			odd = append(odd, byDigest[d]...)
		}

		return fmt.Sprintf("%s split a different input or used different timings than the other machines: %s",
			machineList(odd), detail)
	}

	return "machines split different inputs or used different timings: " + detail
}

func listProblem(what string, ids []string) string {
	shown := ids[:min(len(ids), maxListed)]
	msg := fmt.Sprintf("%d %s %s: %s", len(ids), plural(len(ids), "test", "tests"), what, strings.Join(shown, ", "))
	if len(ids) > len(shown) {
		msg += fmt.Sprintf(", and %d more", len(ids)-len(shown))
	}

	return msg
}

// largestGroup prefers the larger key on a tie, so map order cannot matter.
func largestGroup(groups map[int][]int) int {
	best, size := 0, -1
	for _, k := range slices.Sorted(maps.Keys(groups)) {
		if len(groups[k]) >= size {
			best, size = k, len(groups[k])
		}
	}

	return best
}

func machineList(indexes []int) string {
	sorted := slices.Sorted(slices.Values(indexes))
	names := make([]string, len(sorted))
	for i, n := range sorted {
		names[i] = strconv.Itoa(n)
	}

	return plural(len(sorted), "machine ", "machines ") + strings.Join(names, ", ")
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}

	return many
}
