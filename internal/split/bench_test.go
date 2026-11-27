package split_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/nuggocto/bunpo/internal/split"
)

// BenchmarkSplit checks the budget in PROJECT.md: under one second for
// 50,000 tests.
func BenchmarkSplit(b *testing.B) {
	const tests = 50_000

	r := rand.New(rand.NewPCG(1, 2))
	raw := make([]string, tests)
	durations := make(map[string]int64, tests)

	for i := range raw {
		raw[i] = fmt.Sprintf("pkg%d/sub%d/test_%d.py", i%97, i%13, i)
		if i%10 != 0 {
			durations[raw[i]] = int64(r.IntN(600_000))
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		inv, err := split.NewInventory(raw, split.POSIX)
		if err != nil {
			b.Fatal(err)
		}

		if _, err := split.Split(inv, durations, split.MaxMachines); err != nil {
			b.Fatal(err)
		}
	}
}
