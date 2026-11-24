package queueapi_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

func sampleBuildEvents(t *testing.T, buildID string) (initEvs, leaseEvs, repEvs []queue.Event) {
	t.Helper()
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	st := queue.NewState(lim)
	cfg := queue.Config{
		Machines:       1,
		LeaseTTLMS:     10_000,
		BuildTimeoutMS: 60_000,
		MaxAttempts:    3,
		MaxBatch:       1,
	}
	pred := int64(100)
	var err error
	initEvs, _, err = queue.DecideInit(st, 1000, queue.InitCommand{
		BuildID:       buildID,
		RunnerID:      "r1",
		Config:        cfg,
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{&pred},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range initEvs {
		if err := st.Apply(ev); err != nil {
			t.Fatal(err)
		}
	}
	var leaseResp queue.LeaseResponse
	leaseEvs, leaseResp, err = queue.DecideLease(st, 1100, queue.LeaseCommand{
		RunnerID:  "r1",
		RequestID: "q1",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range leaseEvs {
		if err := st.Apply(ev); err != nil {
			t.Fatal(err)
		}
	}
	repEvs, _, err = queue.DecideReport(st, 1200, queue.ReportCommand{
		RunnerID: "r1",
		LeaseID:  leaseResp.LeaseID,
		Results: []queue.ReportResult{{
			AttemptID:  "0.1",
			TestIndex:  0,
			Generation: 1,
			Outcome:    queue.OutcomePassed,
			Timed:      true,
			DurationMS: 50,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return initEvs, leaseEvs, repEvs
}

func TestDiskLogFormatGolden(t *testing.T) {
	dir := t.TempDir()
	fs, err := queueapi.OpenOSLogFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	dl := queueapi.NewDiskLog(fs, nil)
	initEvs, leaseEvs, repEvs := sampleBuildEvents(t, "golden-1")
	for _, batch := range [][]queue.Event{initEvs, leaseEvs, repEvs} {
		if err := dl.Append(context.Background(), "golden-1", batch); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(filepath.Join(dir, queueapi.BuildFileName("golden-1")))
	if err != nil {
		t.Fatal(err)
	}
	const want = "bunpo-queue-log 1\n" +
		"5358374d [{\"type\":\"initialized\",\"at_ms\":1000,\"initialized\":{\"build_id\":\"golden-1\",\"digest\":\"56e035ea0ec29bf819c491da91d950f55d0f74e42964d013ec32ca0b8cbc0109\",\"config\":{\"machines\":1,\"lease_ttl_ms\":10000,\"build_timeout_ms\":60000,\"max_attempts\":3,\"max_batch\":1},\"deadline_ms\":61000,\"tests\":[\"a.py\"],\"predictions_ms\":[100]}}]\n" +
		"b5b08950 [{\"type\":\"leased\",\"at_ms\":1100,\"leased\":{\"lease_id\":1,\"runner_id\":\"r1\",\"machine_index\":null,\"request_id\":\"q1\",\"deadline_ms\":11100,\"attempts\":[{\"test_index\":0,\"generation\":1}]}}]\n" +
		"360a2494 [{\"type\":\"reported\",\"at_ms\":1200,\"reported\":{\"lease_id\":1,\"runner_id\":\"r1\",\"close_lease\":true,\"results\":[{\"test_index\":0,\"generation\":1,\"outcome\":\"passed\",\"timed\":true,\"duration_ms\":50,\"disposition\":\"accepted\"}]}}]\n"
	if string(got) != want {
		t.Fatalf("golden mismatch:\ngot:\n%s\nwant:\n%s", string(got), want)
	}
}

func TestDiskLogDamageAndRecovery(t *testing.T) {
	baseDir := t.TempDir()
	baseFS, err := queueapi.OpenOSLogFS(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	baseLog := queueapi.NewDiskLog(baseFS, nil)
	initEvs, leaseEvs, repEvs := sampleBuildEvents(t, "dmg-1")
	for _, b := range [][]queue.Event{initEvs, leaseEvs, repEvs} {
		if err := baseLog.Append(context.Background(), "dmg-1", b); err != nil {
			t.Fatal(err)
		}
	}
	goodName := queueapi.BuildFileName("dmg-1")
	fullRaw, err := os.ReadFile(filepath.Join(baseDir, goodName))
	if err != nil {
		t.Fatal(err)
	}
	if err := baseFS.Close(); err != nil {
		t.Fatal(err)
	}

	lines := bytes.SplitAfter(fullRaw, []byte{'\n'})
	lines = lines[:len(lines)-1]
	rec1End := len(lines[0]) + len(lines[1])
	rec2End := rec1End + len(lines[2])

	cases := []struct {
		name        string
		fileName    string
		checkID     string
		mutate      func([]byte) []byte
		wantDamaged bool
		wantTrunc   bool
	}{
		{
			name:        "flip middle byte",
			fileName:    goodName,
			checkID:     "dmg-1",
			mutate:      func(b []byte) []byte { cp := bytes.Clone(b); cp[rec1End+20] ^= 1; return cp },
			wantDamaged: true,
		},
		{
			name:     "flip middle digit (CRC bitrot)",
			fileName: goodName,
			checkID:  "dmg-1",
			mutate: func(b []byte) []byte {
				return bytes.Replace(b, []byte(`"deadline_ms":11100`), []byte(`"deadline_ms":11101`), 1)
			},
			wantDamaged: true,
		},
		{
			name:      "flip last record truncates tail",
			fileName:  goodName,
			checkID:   "dmg-1",
			mutate:    func(b []byte) []byte { cp := bytes.Clone(b); cp[len(cp)-5] ^= 1; return cp },
			wantTrunc: true,
		},
		{
			name:     "wrong version header",
			fileName: goodName,
			checkID:  "dmg-1",
			mutate: func(b []byte) []byte {
				return bytes.Replace(b, []byte("bunpo-queue-log 1\n"), []byte("bunpo-queue-log 2\n"), 1)
			},
			wantDamaged: true,
		},
		{
			name:        "record of another build",
			fileName:    queueapi.BuildFileName("other-build"),
			checkID:     "other-build",
			mutate:      func(b []byte) []byte { return bytes.Clone(b) },
			wantDamaged: true,
		},
		{
			name:        "duplicated record refused by Apply",
			fileName:    goodName,
			checkID:     "dmg-1",
			mutate:      func(b []byte) []byte { return append(bytes.Clone(b), lines[2]...) },
			wantDamaged: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fs, err := queueapi.OpenOSLogFS(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer fs.Close()

			sInit, _, _ := sampleBuildEvents(t, "healthy-sibling")
			if err := queueapi.NewDiskLog(fs, nil).Append(context.Background(), "healthy-sibling", sInit); err != nil {
				t.Fatal(err)
			}
			mutated := tc.mutate(fullRaw)
			if err := os.WriteFile(filepath.Join(dir, tc.fileName), mutated, 0o600); err != nil {
				t.Fatal(err)
			}
			loaded, err := queueapi.NewDiskLog(fs, nil).Load(queue.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(dir, tc.fileName))
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantDamaged {
				if len(loaded.Damaged) != 1 || loaded.Damaged[0] != tc.fileName || len(loaded.Builds) != 1 || !bytes.Equal(after, mutated) {
					t.Fatalf("damaged mismatch: loaded=%+v fileModified=%t", loaded, !bytes.Equal(after, mutated))
				}
				_, c := newTestServer(t, queueapi.HandlerConfig{
					InitialBuilds:  loaded.Builds,
					InitialDamaged: loaded.Damaged,
				})
				_, err := c.Status(context.Background(), tc.checkID)
				assertAPIErr(t, err, 500, queueapi.ErrCodeInternalError)
			} else if tc.wantTrunc {
				if loaded.Truncated != 1 || len(loaded.Damaged) != 0 || len(loaded.Builds) != 2 || len(after) != rec2End {
					t.Fatalf("trunc mismatch: loaded=%+v len=%d want=%d", loaded, len(after), rec2End)
				}
			}
		})
	}

	// Cut at every byte of a 2-record log (header + init + lease).
	twoRec := fullRaw[:rec2End]
	for cut := range len(twoRec) {
		dir := t.TempDir()
		fs, err := queueapi.OpenOSLogFS(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, goodName), twoRec[:cut], 0o600); err != nil {
			fs.Close()
			t.Fatal(err)
		}
		loaded, err := queueapi.NewDiskLog(fs, nil).Load(queue.DefaultLimits())
		if closeErr := fs.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil || len(loaded.Damaged) != 0 ||
			(cut < rec1End && (loaded.Removed != 1 || len(loaded.Builds) != 0)) ||
			(cut == rec1End && (len(loaded.Builds) != 1 || loaded.Truncated != 0 || len(loaded.Builds[0].Leases) != 0)) ||
			(cut > rec1End && (len(loaded.Builds) != 1 || loaded.Truncated != 1 || len(loaded.Builds[0].Leases) != 0)) {
			t.Fatalf("cut=%d (rec1End=%d): err=%v loaded=%+v", cut, rec1End, err, loaded)
		}
	}
}

type faultLogFS struct {
	base            *queueapi.OSLogFS
	failReadAtByte  int // >= 0: return EIO once Read reaches this offset
	failCreateWrite bool
	fileSyncs       int
	dirSyncs        int
}

func (f *faultLogFS) ReadDir() ([]string, error) { return f.base.ReadDir() }
func (f *faultLogFS) Remove(name string) error   { return f.base.Remove(name) }
func (f *faultLogFS) SyncDir() error {
	f.dirSyncs++
	return f.base.SyncDir()
}
func (f *faultLogFS) OpenFile(name string, flag int, perm os.FileMode) (queueapi.LogFile, error) {
	lf, err := f.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultLogFile{
		LogFile:        lf,
		fs:             f,
		failReadAtByte: f.failReadAtByte,
		failWrite:      f.failCreateWrite && (flag&os.O_CREATE != 0),
	}, nil
}

type faultLogFile struct {
	queueapi.LogFile
	fs             *faultLogFS
	pos            int
	failReadAtByte int
	failWrite      bool
	closed         bool
}

func (f *faultLogFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.failReadAtByte >= 0 {
		if f.pos >= f.failReadAtByte {
			return 0, errors.New("simulated EIO on read")
		}
		if len(p) > f.failReadAtByte-f.pos {
			p = p[:f.failReadAtByte-f.pos]
		}
	}
	n, err := f.LogFile.Read(p)
	f.pos += n
	if err == nil && f.failReadAtByte >= 0 && f.pos >= f.failReadAtByte {
		return n, errors.New("simulated EIO on read")
	}
	return n, err
}

func (f *faultLogFile) Write(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.failWrite {
		return 0, errors.New("simulated ENOSPC on create")
	}
	return f.LogFile.Write(p)
}

func (f *faultLogFile) Sync() error {
	if f.closed {
		return os.ErrClosed
	}
	f.fs.fileSyncs++
	return f.LogFile.Sync()
}

func (f *faultLogFile) Close() error {
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	return f.LogFile.Close()
}

func TestDiskLogReadErrorCreateENOSPCAndLoadFsync(t *testing.T) {
	dir := t.TempDir()
	baseFS, err := queueapi.OpenOSLogFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer baseFS.Close()

	initEvs, leaseEvs, repEvs := sampleBuildEvents(t, "io-1")
	name := queueapi.BuildFileName("io-1")
	dl := queueapi.NewDiskLog(baseFS, nil)
	for _, b := range [][]queue.Event{initEvs, leaseEvs, repEvs} {
		if err := dl.Append(context.Background(), "io-1", b); err != nil {
			t.Fatal(err)
		}
	}
	origBytes, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.SplitAfter(origBytes, []byte{'\n'})
	rec1End := len(lines[0]) + len(lines[1])
	rec2End := rec1End + len(lines[2])

	// Read error at header, first record, Peek after record 2, or near EOF must
	// mark the file damaged and NEVER delete or truncate good data.
	for _, tc := range []struct {
		name   string
		failAt int
	}{
		{"EIO in header", 5},
		{"EIO in first record", len(lines[0]) + 10},
		{"EIO at Peek after record 2", rec2End},
		{"EIO near end of record 3", len(origBytes) - 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ffs := &faultLogFS{base: baseFS, failReadAtByte: tc.failAt}
			loaded, err := queueapi.NewDiskLog(ffs, nil).Load(queue.DefaultLimits())
			if err != nil {
				t.Fatalf("Load returned err: %v", err)
			}
			if len(loaded.Damaged) != 1 || loaded.Damaged[0] != name || loaded.Truncated != 0 || loaded.Removed != 0 {
				t.Fatalf("Load = %+v; want damaged=[%s], truncated=0, removed=0", loaded, name)
			}
			after, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("file was deleted on read error: %v", err)
			}
			if !bytes.Equal(after, origBytes) {
				t.Fatalf("file was modified on read error: len=%d want=%d", len(after), len(origBytes))
			}
		})
	}

	// Load must fsync every accepted file and the directory once.
	t.Run("Load fsyncs accepted file and directory", func(t *testing.T) {
		ffs := &faultLogFS{base: baseFS, failReadAtByte: -1}
		loaded, err := queueapi.NewDiskLog(ffs, nil).Load(queue.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded.Builds) != 1 || ffs.fileSyncs < 1 || ffs.dirSyncs < 1 {
			t.Fatalf("Load sync counts: builds=%d fileSyncs=%d dirSyncs=%d", len(loaded.Builds), ffs.fileSyncs, ffs.dirSyncs)
		}
	})

	// Full disk on create write must close once, remove file, and return ErrAppendUnwritten (503).
	t.Run("create write error returns ErrAppendUnwritten without double close", func(t *testing.T) {
		ffs := &faultLogFS{base: baseFS, failReadAtByte: -1, failCreateWrite: true}
		dl := queueapi.NewDiskLog(ffs, nil)
		ev, _, _ := sampleBuildEvents(t, "enospc-init")
		err := dl.Append(context.Background(), "enospc-init", ev)
		if !errors.Is(err, queueapi.ErrAppendUnwritten) {
			t.Fatalf("Append err = %v; want wrapped ErrAppendUnwritten", err)
		}
		if _, statErr := os.Stat(filepath.Join(dir, queueapi.BuildFileName("enospc-init"))); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("partial file left behind after failed create: %v", statErr)
		}
	})
}

type failRemoveLog struct {
	*queueapi.DiskLog
	failRemove bool
}

func (f *failRemoveLog) Remove(ctx context.Context, buildID string) error {
	if f.failRemove {
		return errors.New("simulated remove error")
	}
	return f.DiskLog.Remove(ctx, buildID)
}

func TestDiskLogEvictionAndFailedRemovePreservesEntry(t *testing.T) {
	dir := t.TempDir()
	fs, err := queueapi.OpenOSLogFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	nowMS := int64(1_000_000)
	log := &failRemoveLog{DiskLog: queueapi.NewDiskLog(fs, nil)}
	_, c := newTestServer(t, queueapi.HandlerConfig{
		Clock:       func() int64 { return nowMS },
		MaxBuilds:   2,
		MaxRunning:  2,
		RetentionMS: 10_000,
		Log:         log,
	})
	ctx := context.Background()
	initOne := func(id string) error {
		_, err := c.Init(ctx, queue.InitCommand{
			BuildID:       id,
			RunnerID:      "r1",
			Config:        defaultTestCfg,
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		})
		return err
	}
	if err := initOne("ev-1"); err != nil {
		t.Fatal(err)
	}
	lease, err := c.Lease(ctx, "ev-1", queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Report(ctx, "ev-1", queue.ReportCommand{
		RunnerID: "r1",
		LeaseID:  lease.LeaseID,
		Results: []queue.ReportResult{{
			AttemptID:  "0.1",
			TestIndex:  0,
			Generation: 1,
			Outcome:    queue.OutcomePassed,
			Timed:      true,
			DurationMS: 10,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	nowMS += 20_000
	log.failRemove = true
	assertAPIErr(t, initOne("ev-1"), 503, queueapi.ErrCodeStorageError)
	if _, err := os.Stat(filepath.Join(dir, queueapi.BuildFileName("ev-1"))); err != nil {
		t.Fatalf("file missing after failed remove: %v", err)
	}

	log.failRemove = false
	if err := initOne("ev-1"); err != nil {
		t.Fatalf("re-init after Remove fixed failed: %v", err)
	}
	entries, err := fs.ReadDir()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !strings.HasPrefix(entries[0], queueapi.BuildFileName("ev-1")) {
		t.Fatalf("unexpected data dir entries: %v", entries)
	}
}

func TestDiskLogSizeMismatchAndBrokenEvictionKeepFile(t *testing.T) {
	dir := t.TempDir()
	fs, err := queueapi.OpenOSLogFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	dl1 := queueapi.NewDiskLog(fs, nil)
	_, c := newTestServer(t, queueapi.HandlerConfig{
		MaxBuilds:  1,
		MaxRunning: 1,
		Log:        dl1,
	})
	ctx := context.Background()
	initCmd := func(id string) queue.InitCommand {
		return queue.InitCommand{
			BuildID:       id,
			RunnerID:      "r1",
			Config:        defaultTestCfg,
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		}
	}
	if _, err := c.Init(ctx, initCmd("split-1")); err != nil {
		t.Fatal(err)
	}

	// Simulate a second server on the same --data directory appending to split-1.
	dl2 := queueapi.NewDiskLog(fs, nil)
	if _, err := dl2.Load(queue.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	_, leaseEvs, _ := sampleBuildEvents(t, "split-1")
	if err := dl2.Append(ctx, "split-1", leaseEvs); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, queueapi.BuildFileName("split-1"))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// dl1 detects sz != expectedSize: must return 500 (broken) and leave file untouched.
	_, err = c.Lease(ctx, "split-1", queue.LeaseCommand{RunnerID: "r1", RequestID: "q-other"})
	assertAPIErr(t, err, 500, queueapi.ErrCodeInternalError)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("file changed on size mismatch: len=%d want=%d", len(after), len(before))
	}

	// Admitting healthy-2 evicts broken split-1 under MaxBuilds=1, leaving
	// split-1's file untouched on disk and refusing re-init of split-1 with 500.
	if _, err := c.Init(ctx, initCmd("healthy-2")); err != nil {
		t.Fatalf("Init healthy-2 should evict broken split-1: %v", err)
	}
	afterEvict, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("broken build file was deleted on eviction: %v", err)
	}
	if !bytes.Equal(afterEvict, before) {
		t.Fatalf("broken build file changed on eviction")
	}
	_, err = c.Init(ctx, initCmd("split-1"))
	assertAPIErr(t, err, 500, queueapi.ErrCodeInternalError)
}
