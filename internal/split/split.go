// Package split deals test files out to machines by predicted duration and
// computes the digest that shows two machines made the same split.
package split

import (
	"cmp"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/nuggocto/bunpo/internal/timing"
)

const (
	MaxMachines = 256

	// AlgorithmVersion is part of every digest. Raise it with any change that
	// could alter a split.
	AlgorithmVersion = 1
)

var (
	ErrMachines        = errors.New("invalid machine count")
	ErrInvalidDuration = errors.New("invalid duration")
)

// Plan is the split of one inventory across machines.
type Plan struct {
	Machines int

	// Tests is the input in byte order; Durations[i] is the effective
	// duration of Tests[i].
	Tests     []string
	Durations []int64

	// Shards holds each machine's tests in byte order; Loads their totals.
	Shards [][]string
	Loads  []int64

	// Unknown tests had no recorded duration and were given FillMS.
	Unknown int
	FillMS  int64

	// Equal digests mean equal splits.
	Digest [sha256.Size]byte
}

// Split assigns every test in inv to one machine. Durations for tests
// outside inv are ignored. The same inputs always give the same plan.
func Split(inv Inventory, durations map[string]int64, machines int) (Plan, error) {
	if machines < 1 || machines > MaxMachines {
		return Plan{}, fmt.Errorf("%w: %d, want 1 to %d", ErrMachines, machines, MaxMachines)
	}

	effective, fill, unknown, err := effectiveDurations(inv.ids, durations)
	if err != nil {
		return Plan{}, err
	}

	owner, loads, counts := deal(effective, machines)

	shards := make([][]string, machines)
	for m := range shards {
		shards[m] = make([]string, 0, counts[m])
	}

	// The inventory is in byte order, so every shard is too.
	for i, id := range inv.ids {
		shards[owner[i]] = append(shards[owner[i]], id)
	}

	return Plan{
		Machines:  machines,
		Tests:     slices.Clone(inv.ids),
		Durations: effective,
		Shards:    shards,
		Loads:     loads,
		Unknown:   unknown,
		FillMS:    fill,
		Digest:    digest(machines, inv.ids, effective),
	}, nil
}

// effectiveDurations uses each recorded duration as it is and gives tests
// without one the median of the others, or 0 with no history.
func effectiveDurations(ids []string, durations map[string]int64) ([]int64, int64, int, error) {
	effective := make([]int64, len(ids))
	recorded := make([]bool, len(ids))
	known := make([]int64, 0, min(len(ids), len(durations)))

	for i, id := range ids {
		ms, ok := durations[id]
		if !ok {
			continue
		}

		if ms < 0 || ms > timing.MaxDurationMS {
			return nil, 0, 0, fmt.Errorf("%w: test %q has %d ms, want 0 to %d", ErrInvalidDuration, id, ms, timing.MaxDurationMS)
		}

		effective[i], recorded[i] = ms, true
		known = append(known, ms)
	}

	fill, _ := timing.Median(known)

	unknown := 0
	for i := range effective {
		if !recorded[i] {
			effective[i] = fill
			unknown++
		}
	}

	return effective, fill, unknown, nil
}

// deal gives tests out longest first, each to the machine with the least
// load, then fewest tests, then lowest index. Without the test count, files
// recorded at 0 ms would all land on machine 0.
func deal(effective []int64, machines int) (owner []int, loads []int64, counts []int) {
	// The inputs are in byte order and the sort is stable, so equal
	// durations go in path order.
	order := make([]int, len(effective))
	for i := range order {
		order[i] = i
	}

	slices.SortStableFunc(order, func(a, b int) int {
		return cmp.Compare(effective[b], effective[a])
	})

	owner = make([]int, len(effective))
	loads = make([]int64, machines)
	counts = make([]int, machines)

	for _, t := range order {
		target := 0
		for m := 1; m < machines; m++ {
			if loads[m] < loads[target] || (loads[m] == loads[target] && counts[m] < counts[target]) {
				target = m
			}
		}

		owner[t] = target
		loads[target] += effective[t]
		counts[target]++
	}

	return owner, loads, counts
}

// digest hashes "bunpo split <version>\nmachines <n>\n", then
// "<path>\x00<ms>\n" per test in byte order. Paths hold no NUL or newline,
// so the encoding is unambiguous.
func digest(machines int, ids []string, effective []int64) [sha256.Size]byte {
	h := sha256.New()

	// Writes to a hash never fail.
	_, _ = fmt.Fprintf(h, "bunpo split %d\nmachines %d\n", AlgorithmVersion, machines)

	buf := make([]byte, 0, 128)
	for i, id := range ids {
		buf = append(buf[:0], id...)
		buf = append(buf, 0)
		buf = strconv.AppendInt(buf, effective[i], 10)
		buf = append(buf, '\n')

		_, _ = h.Write(buf)
	}

	return [sha256.Size]byte(h.Sum(nil))
}

func (p Plan) TotalMS() int64 {
	var total int64
	for _, l := range p.Loads {
		total += l
	}

	return total
}

// LargestMS returns the longest single test, or 0 for an empty plan.
func (p Plan) LargestMS() int64 {
	if len(p.Durations) == 0 {
		return 0
	}

	return slices.Max(p.Durations)
}

// Dominant reports whether one test is longer than the average machine load,
// which no file-level split can balance.
func (p Plan) Dominant() bool {
	return p.LargestMS()*int64(p.Machines) > p.TotalMS()
}

func (p Plan) EmptyShards() int {
	empty := 0
	for _, s := range p.Shards {
		if len(s) == 0 {
			empty++
		}
	}

	return empty
}
