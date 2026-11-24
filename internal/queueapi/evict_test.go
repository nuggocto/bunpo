package queueapi

import (
	"context"
	"slices"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
)

func TestPlanEvictionExactBounds(t *testing.T) {
	const (
		now = int64(100_000)
		ret = int64(50_000)
		ttl = int64(10_000)
	)
	run := func(id string, created, lastAct int64) evictItem {
		return evictItem{id: id, kind: kindRunning, createdAtMS: created, lastActivityMS: lastAct, leaseTTL: ttl}
	}
	fin := func(id string, created, finished int64) evictItem {
		return evictItem{id: id, kind: kindFinished, createdAtMS: created, finishedAtMS: finished}
	}

	cases := []struct {
		name       string
		items      []evictItem
		maxRun     int
		maxBld     int
		slot       int
		wantEvict  []string
		wantRefuse bool
	}{
		{
			name:   "below capacity admits without eviction",
			items:  []evictItem{run("r1", 1, now), fin("f1", 1, now-ret+1)},
			maxRun: 2, maxBld: 3, slot: 1,
		},
		{
			name:   "exact retention boundary evicts (now-fin == ret)",
			items:  []evictItem{fin("exp", 1, now-ret), fin("keep", 1, now-ret+1)},
			maxRun: 2, maxBld: 5, slot: 1,
			wantEvict: []string{"exp"},
		},
		{
			name:   "running at exact maxRunning refuses active",
			items:  []evictItem{run("r1", 1, now), run("r2", 2, now-2*ttl)},
			maxRun: 2, maxBld: 4, slot: 1,
			wantRefuse: true,
		},
		{
			name:   "busy entry counts toward running capacity",
			items:  []evictItem{{id: "busy", kind: kindBusy}, run("r1", 1, now)},
			maxRun: 2, maxBld: 4, slot: 1,
			wantRefuse: true,
		},
		{
			name:   "idle running strictly past 2*ttl evicted for running slot",
			items:  []evictItem{run("active", 1, now-2*ttl), run("idle-new", 2, now-2*ttl-1), run("idle-old", 3, now-2*ttl-500)},
			maxRun: 2, maxBld: 4, slot: 1,
			wantEvict: []string{"idle-old", "idle-new"},
		},
		{
			name: "total capacity priority: broken before finished before idle running",
			items: []evictItem{
				run("idle", 1, now-2*ttl-1),
				fin("fin-new", 2, now-10),
				fin("fin-old", 1, now-20),
				{id: "brk", kind: kindBroken},
			},
			maxRun: 4, maxBld: 4, slot: 1,
			wantEvict: []string{"brk"},
		},
		{
			name: "total capacity evicts oldest finished then idle running",
			items: []evictItem{
				run("idle", 1, now-2*ttl-1),
				fin("fin-b", 2, now-20),
				fin("fin-a", 1, now-20),
			},
			maxRun: 4, maxBld: 2, slot: 1,
			wantEvict: []string{"fin-a", "fin-b"},
		},
		{
			name:   "total capacity falls back to idle running when no broken or finished",
			items:  []evictItem{run("active", 1, now), run("idle", 2, now-2*ttl-1)},
			maxRun: 4, maxBld: 2, slot: 1,
			wantEvict: []string{"idle"},
		},
		{
			name:   "total capacity refuses when all active running",
			items:  []evictItem{run("a", 1, now), run("b", 2, now)},
			maxRun: 4, maxBld: 2, slot: 1,
			wantRefuse: true,
		},
		{
			name:   "skipEvict prevents re-evicting failed remove",
			items:  []evictItem{{id: "stuck", kind: kindFinished, finishedAtMS: now - ret, skipEvict: true}, run("a", 1, now)},
			maxRun: 4, maxBld: 2, slot: 1,
			wantRefuse: true,
		},
		{
			name: "startup load (slot=0) evicts expired and excess running oldest-first",
			items: []evictItem{
				fin("exp", 1, now-ret),
				run("r-new", 20, now),
				run("r-old", 10, now),
				fin("f-keep", 5, now-10),
			},
			maxRun: 1, maxBld: 2, slot: 0,
			wantEvict: []string{"exp", "r-old"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, refuse := planEviction(tc.items, now, ret, tc.maxRun, tc.maxBld, tc.slot)
			if (refuse != nil) != tc.wantRefuse || !slices.Equal(got, tc.wantEvict) {
				t.Fatalf("planEviction = (%v, refuse=%v); want (%v, refuse=%t)", got, refuse, tc.wantEvict, tc.wantRefuse)
			}
		})
	}

	st := queue.NewState(queue.DefaultLimits())
	if isExpired(st, now, ret) {
		t.Fatal("uninitialized state must not be expired")
	}
	st.Initialized = true
	st.FinishedAtMS = now - ret
	if !isExpired(st, now, ret) || isExpired(st, now-1, ret) {
		t.Fatal("isExpired boundary failed at exact retentionMS")
	}
}

type countRemoveLog struct {
	removed []string
}

func (l *countRemoveLog) Append(context.Context, string, []queue.Event) error { return nil }
func (l *countRemoveLog) Remove(_ context.Context, buildID string) error {
	l.removed = append(l.removed, buildID)
	return nil
}

func TestEvictLockedRechecksActivityUnderLock(t *testing.T) {
	log := &countRemoveLog{}
	h, err := NewHandler(HandlerConfig{
		WriteToken: "write-secret-token-0123456789abcdef",
		ReadToken:  "read-secret-token-fedcba9876543210",
		Log:        log,
	})
	if err != nil {
		t.Fatal(err)
	}
	st := queue.NewState(queue.DefaultLimits())
	st.Initialized = true
	st.BuildID = "active-again"
	st.Config.LeaseTTLMS = 10_000
	entry := &buildEntry{state: st, lastActivityMS: 1_000}
	h.builds["active-again"] = entry

	// Plan captured the build while idle at lastActivityMS=1000.
	planned := entryToEvictItem("active-again", entry, false)

	// Before evictLocked runs, a heartbeat advances lastActivityMS.
	entry.lastActivityMS = 25_000

	entry.mu.Lock()
	res := h.evictLocked(context.Background(), "active-again", entry, &planned)
	if res != evictSkipped {
		t.Fatalf("evictLocked = %d; want evictSkipped (%d)", res, evictSkipped)
	}
	if len(log.removed) != 0 || h.builds["active-again"] != entry || entry.evicted {
		t.Fatalf("active build was removed despite activity update: removed=%v evicted=%t", log.removed, entry.evicted)
	}
}
