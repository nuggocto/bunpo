package sim_test

import (
	"os"
	"strconv"
	"testing"
)

const defaultSimSeeds = 50

func runSeed(t *testing.T, seed int64) (string, simStats) {
	t.Helper()
	w, err := newSimWorld(seed)
	if err != nil {
		t.Fatalf("seed %d setup failed: %v", seed, err)
	}
	if err := w.run(); err != nil {
		t.Fatalf("seed %d failed: %v\n\nReplay:\n  BUNPO_SIM_SEED=%d go test -v -run TestSim ./internal/sim\n\nLast %d trace lines:\n%s",
			seed, err, seed, traceTail, w.trace.tail())
	}
	return w.trace.digest(), w.stats
}

func TestSim(t *testing.T) {
	if raw := os.Getenv("BUNPO_SIM_SEED"); raw != "" {
		seed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid BUNPO_SIM_SEED %q: %v", raw, err)
		}
		digest, stats := runSeed(t, seed)
		t.Logf("seed=%d digest=%s %s", seed, digest[:16], stats.summary())
		return
	}

	count := defaultSimSeeds
	if raw := os.Getenv("BUNPO_SIM_COUNT"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			t.Fatalf("invalid BUNPO_SIM_COUNT %q", raw)
		}
		count = n
	}

	var total simStats
	for i := range count {
		_, st := runSeed(t, int64(1000+i))
		total.add(st)
	}
	t.Logf("sim coverage (%d seeds): %s", count, total.summary())

	if count >= 20 {
		required := []statKind{
			stReqDrop, stRespDrop, stReqDup, stRespDup, stRestart, stClockJump,
			stDiskUnwritten, stDiskUncertain, stKill, stPause, stDivInit, stReportOutage,
			stStaleResult, stStaleRequest, stLeaseHeld, stSoloRetry, stExhausted,
			stDeadline, stConflictAfterPassed, stReportAcrossRestart, stBroken,
			stDupReport, stStatusReport,
		}
		for _, k := range required {
			if total[k] == 0 {
				t.Errorf("simulator never reached required fault/state %q across %d seeds", statNames[k], count)
			}
		}
	}
}

func TestSimDeterminism(t *testing.T) {
	seeds := []int64{1000, 1004, 1012}
	hashes := make(map[string]int64, len(seeds))
	for _, seed := range seeds {
		h1, _ := runSeed(t, seed)
		h2, _ := runSeed(t, seed)
		if h1 != h2 {
			t.Fatalf("seed %d is non-deterministic: run1=%s run2=%s", seed, h1, h2)
		}
		if prev, dup := hashes[h1]; dup {
			t.Fatalf("distinct seeds %d and %d produced identical trace hash %s", prev, seed, h1)
		}
		hashes[h1] = seed
	}
}
