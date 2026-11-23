package sim_test

import (
	"os"
	"strconv"
	"testing"
)

const defaultSimSeeds = 80

type namedScenario struct {
	name    string
	seed    int64
	profile faultProfile
}

// A few named scenarios guarantee key edge-case combinations, while the main
// loop runs 100% random swarm profiles across defaultSimSeeds seeds.
var namedScenarios = []namedScenario{
	{"conflict-after-passed", 10, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, conflictAfterPassed: true, cmdSkip: true}},
	{"report-outage-past-ttl", 11, faultProfile{poisonIdx: -1, netMaxDelayMS: 20, reportOutage: true, expectCleanPass: true, cmdSkip: true}},
	{"restart-during-report", 12, faultProfile{poisonIdx: -1, netMaxDelayMS: 60, serverRestart: true, reqDupPct: 15, respDropPct: 12, respDupPct: 10, cmdSkip: true}},
	{"old-report-and-siblings", 13, faultProfile{poisonIdx: -1, netMaxDelayMS: 40, cmdOldReport: true, cmdNoReport: true, cmdMixedSibling: true, cmdSigSibling: true, cmdFail: true, cmdSkip: true}},
	{"pause-stale-and-lease-held", 14, faultProfile{poisonIdx: -1, netMaxDelayMS: 40, runnerPause: true, reqDupPct: 35, respDropPct: 15, cmdCrash: true}},
	{"poison-exhaustion", 15, faultProfile{poisonIdx: 0, netMaxDelayMS: 30, cmdCrash: true}},
	{"deadline-and-clock-back", 16, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, shortDeadline: true, serverClockJump: true, clockBackward: true, cmdHang: true}},
	{"uncertain-disk-broken", 17, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, diskUncertainPct: 25, diskUnwrittenPct: 15}},
	{"extra-file-and-429-503", 18, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, cmdExtraFile: true, http503Pct: 15, http429Pct: 15}},
}

func runSeed(t *testing.T, seed int64, override *faultProfile) (string, simStats) {
	t.Helper()
	w, err := newSimWorld(seed, override)
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
		digest, stats := runSeed(t, seed, nil)
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
	total := make(simStats)
	merge := func(seed int64, p *faultProfile) {
		_, st := runSeed(t, seed, p)
		for k, v := range st {
			total[k] += v
		}
	}
	for _, sc := range namedScenarios {
		p := sc.profile
		merge(sc.seed, &p)
	}
	for i := range count {
		merge(int64(1+i), nil)
	}
	t.Logf("sim coverage (%d named + %d random seeds): %s", len(namedScenarios), count, total.summary())
	if count >= 40 {
		for _, k := range statNames {
			if total[k] == 0 {
				t.Errorf("simulator never reached required fault/state %q across %d seeds", k, count)
			}
		}
	}
}

func TestSimDeterminism(t *testing.T) {
	hashes := make(map[string]int64, 3)
	for _, seed := range []int64{1, 14, 42} {
		h1, _ := runSeed(t, seed, nil)
		if h2, _ := runSeed(t, seed, nil); h1 != h2 {
			t.Fatalf("seed %d is non-deterministic: %s != %s", seed, h1, h2)
		}
		if prev, dup := hashes[h1]; dup {
			t.Fatalf("distinct seeds %d and %d produced identical trace hash %s", prev, seed, h1)
		}
		hashes[h1] = seed
	}
}
