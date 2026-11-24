package sim_test

import (
	"fmt"
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

// Named scenarios pin key edge-case combinations, while the random subtest runs
// fully randomized swarm profiles across defaultSimSeeds seeds.
var namedScenarios = []namedScenario{
	{"conflict-after-passed", 10, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, conflictAfterPassed: true, cmdSkip: true}},
	{"report-outage-past-ttl", 11, faultProfile{poisonIdx: -1, netMaxDelayMS: 20, reportOutage: true, expectCleanPass: true, cmdSkip: true}},
	{"restart-during-report", 12, faultProfile{poisonIdx: -1, netMaxDelayMS: 60, serverRestart: true, reqDupPct: 15, respDropPct: 12, respDupPct: 10, cmdSkip: true}},
	{"old-report-and-siblings", 13, faultProfile{poisonIdx: -1, netMaxDelayMS: 40, cmdOldReport: true, cmdNoReport: true, cmdMixedSibling: true, cmdSigSibling: true, cmdNonZeroClean: true, cmdFail: true, cmdSkip: true}},
	{"pause-stale-and-lease-held", 14, faultProfile{poisonIdx: -1, netMaxDelayMS: 40, runnerPause: true, reqDupPct: 35, respDropPct: 15, cmdCrash: true}},
	{"poison-exhaustion", 15, faultProfile{poisonIdx: 0, netMaxDelayMS: 30, cmdCrash: true}},
	{"deadline-and-clock-back", 16, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, shortDeadline: true, serverClockJump: true, clockBackward: true, cmdHang: true}},
	{"uncertain-disk-broken", 17, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, diskUncertainPct: 25, diskUnwrittenPct: 15}},
	{"extra-file-and-429-503", 18, faultProfile{poisonIdx: -1, netMaxDelayMS: 30, cmdExtraFile: true, http503Pct: 15, http429Pct: 15}},
	{"mid-append-crash-and-torn-tail", 19, faultProfile{poisonIdx: -1, netMaxDelayMS: 40, crashMidAppendPct: 25, diskUnwrittenPct: 15, cmdSkip: true}},
	{"restart-downtime-past-ttl", 20, faultProfile{poisonIdx: -1, netMaxDelayMS: 20, serverRestart: true, restartDowntime: true, expectCleanPass: true}},
	{"bitrot-crc-check", 21, faultProfile{poisonIdx: -1, netMaxDelayMS: 20, injectBitrot: true}},
	{"retention-eviction-removes-file", 22, faultProfile{poisonIdx: -1, netMaxDelayMS: 20, checkRetentionEvict: true}},
	{"load-fsync-after-proc-kill", 23, faultProfile{poisonIdx: -1, netMaxDelayMS: 20, procKillThenPowerLoss: true}},
	{"read-eio-during-load", 24, faultProfile{poisonIdx: -1, netMaxDelayMS: 20, serverRestart: true, loadReadErrPct: 50}},
}

func runSeed(t *testing.T, scenarioName string, seed int64, override *faultProfile) (string, simStats) {
	t.Helper()
	w, err := newSimWorld(seed, override)
	if err != nil {
		t.Fatalf("seed %d setup failed: %v", seed, err)
	}
	if err := w.run(); err != nil {
		replay := fmt.Sprintf("BUNPO_SIM_SEED=%d go test -v -run '^TestSim$' ./internal/sim", seed)
		if scenarioName != "" {
			replay = fmt.Sprintf("go test -v -run '^TestSim/%s$' ./internal/sim", scenarioName)
		}
		t.Fatalf("seed %d failed: %v\n\nReplay:\n  %s\n\nLast %d trace lines:\n%s",
			seed, err, replay, traceTail, w.trace.tail())
	}
	return w.trace.digest(), w.stats
}

func TestSim(t *testing.T) {
	if raw := os.Getenv("BUNPO_SIM_SEED"); raw != "" {
		seed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid BUNPO_SIM_SEED %q: %v", raw, err)
		}
		digest, stats := runSeed(t, "", seed, nil)
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
	merge := func(st simStats) {
		for k, v := range st {
			total[k] += v
		}
	}

	for _, sc := range namedScenarios {
		t.Run(sc.name, func(t *testing.T) {
			p := sc.profile
			_, st := runSeed(t, sc.name, sc.seed, &p)
			merge(st)
		})
	}

	ranRandom := 0
	t.Run("random", func(t *testing.T) {
		for i := range count {
			_, st := runSeed(t, "", int64(1+i), nil)
			merge(st)
			ranRandom++
		}
	})

	if ranRandom >= 40 {
		t.Logf("sim coverage (%d named + %d random seeds): %s", len(namedScenarios), ranRandom, total.summary())
		for _, k := range statNames {
			if total[k] == 0 {
				t.Errorf("simulator never reached required fault/state %q across %d seeds", k, ranRandom)
			}
		}
	}
}

func TestSimDeterminism(t *testing.T) {
	hashes := make(map[string]int64, 3)
	for _, seed := range []int64{1, 14, 42} {
		h1, _ := runSeed(t, "", seed, nil)
		if h2, _ := runSeed(t, "", seed, nil); h1 != h2 {
			t.Fatalf("seed %d is non-deterministic: %s != %s", seed, h1, h2)
		}
		if prev, dup := hashes[h1]; dup {
			t.Fatalf("distinct seeds %d and %d produced identical trace hash %s", prev, seed, h1)
		}
		hashes[h1] = seed
	}
}
