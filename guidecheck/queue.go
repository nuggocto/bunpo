package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	queueWriteToken = "guidecheck-write-token-0123456789abcdef"
	queueReadToken  = "guidecheck-read-token-fedcba9876543210"
)

type queueScenario struct {
	name            string
	machines        int
	keepFailing     bool
	decoys          bool
	noTests         bool
	emptyList       bool
	killMachine0    bool
	killerFile      bool
	playwrightSetup bool
}

var queueScenarios = []queueScenario{
	{name: "queue two machines", machines: 2},
	{name: "queue decoys", machines: 2, decoys: true},
	{name: "queue failing file", machines: 2, keepFailing: true},
	{name: "queue failed discovery", machines: 1, noTests: true},
	{name: "queue empty list", machines: 1, emptyList: true},
	{name: "queue machine killed", machines: 2, killMachine0: true},
	{name: "queue runner-killing file", machines: 2, killerFile: true},
	{name: "queue playwright setup", machines: 2, playwrightSetup: true},
}

type queueServer struct {
	url string
	cmd *exec.Cmd
}

func startQueueServer(work, serverBin string) (*queueServer, error) {
	if serverBin == "" {
		serverBin = filepath.Join(work, "bin", "bunpo-server")
		build := exec.Command("go", "build", "-o", serverBin, "./cmd/bunpo-server")
		if out, err := build.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("build bunpo-server: %v\n%s", err, out)
		}
	}

	cmd := exec.Command(serverBin, "--addr", "127.0.0.1:0")
	cmd.Env = append(os.Environ(),
		"BUNPO_QUEUE_TOKEN="+queueWriteToken,
		"BUNPO_QUEUE_READ_TOKEN="+queueReadToken,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start bunpo-server: %w", err)
	}

	addrCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		var logs strings.Builder
		for sc.Scan() {
			line := sc.Text()
			logs.WriteString(line)
			logs.WriteByte('\n')
			var rec struct {
				Msg  string `json:"msg"`
				Addr string `json:"addr"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "listening" && rec.Addr != "" {
				addrCh <- rec.Addr
				_, _ = io.Copy(io.Discard, stderr)
				return
			}
		}
		errCh <- fmt.Errorf("bunpo-server exited before listening:\n%s", logs.String())
	}()

	select {
	case addr := <-addrCh:
		return &queueServer{url: "http://" + addr, cmd: cmd}, nil
	case err := <-errCh:
		_ = cmd.Wait()
		return nil, err
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		return nil, errors.New("timed out waiting for bunpo-server to listen")
	}
}

func (s *queueServer) stop() {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

func checkQueue(dir string, r runner, g guide, qs queueScenario, path, fixtures, serverURL string) error {
	if qs.emptyList {
		return checkQueueEmpty(dir, r, g, path, fixtures, serverURL)
	}

	if qs.noTests {
		m := filepath.Join(dir, "machine-0")
		if err := prepareQueueSuite(m, r, g, qs, fixtures); err != nil {
			return err
		}
		env := queueEnv(path, serverURL, 0, 1, fmt.Sprintf("gc-%s-notests", r.name))
		code, output := shell(m, g.queueMachineScript, "", env)
		if code == 0 {
			return fmt.Errorf("queue run passed with no test files to find\n%s", output)
		}
		return nil
	}

	// Discover the expected canonical file list on a clean prepared copy.
	probe := filepath.Join(dir, "probe")
	if err := prepareQueueSuite(probe, r, g, qs, fixtures); err != nil {
		return err
	}
	expected, err := discoverFiles(probe, g.listCommand, path)
	if err != nil {
		return err
	}
	if qs.playwrightSetup && slices.Contains(expected, "tests/auth.setup.js") {
		return fmt.Errorf("setup file tests/auth.setup.js leaked into discovery: %v", expected)
	}

	buildRunID := fmt.Sprintf("gc-%s-%s", r.name, strings.ReplaceAll(qs.name, " ", "-"))
	machineScript := g.queueMachineScript
	switch {
	case qs.killMachine0:
		machineScript = spliceQueueFlags(machineScript, "--lease-ttl 10s --max-batch 1")
	case qs.killerFile:
		machineScript = spliceQueueFlags(machineScript, "--max-attempts 2")
	}

	machineDirs := make([]string, qs.machines)
	for i := range qs.machines {
		m := filepath.Join(dir, fmt.Sprintf("machine-%d", i))
		if err := prepareQueueSuite(m, r, g, qs, fixtures); err != nil {
			return err
		}
		if qs.playwrightSetup {
			code, out := shell(m, "npx playwright test --project=setup --no-deps --reporter=list", "", map[string]string{"PATH": path})
			if code != 0 {
				return fmt.Errorf("machine %d setup project exited %d\n%s", i, code, out)
			}
		}
		machineDirs[i] = m
	}

	if qs.killMachine0 {
		if err := runKilledMachineScenario(dir, machineDirs, machineScript, path, serverURL, buildRunID); err != nil {
			return err
		}
	} else {
		codes := make([]int, qs.machines)
		outs := make([]string, qs.machines)
		var wg sync.WaitGroup
		for i := range qs.machines {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				env := queueEnv(path, serverURL, idx, qs.machines, buildRunID)
				codes[idx], outs[idx] = shell(machineDirs[idx], machineScript, "", env)
			}(i)
		}
		wg.Wait()

		wantCode := 0
		if qs.keepFailing || qs.killerFile {
			wantCode = 1
		}
		for i := range qs.machines {
			if codes[i] != wantCode {
				return fmt.Errorf("machine %d exited %d, want %d\n%s", i, codes[i], wantCode, outs[i])
			}
		}
	}

	// Final bunpo job: --status, exact accepted-result verification, and merge.
	final := filepath.Join(dir, "final")
	if err := os.MkdirAll(filepath.Join(final, ".bunpo"), 0o755); err != nil {
		return err
	}
	statusEnv := map[string]string{
		"PATH":                   path,
		"BUNPO_QUEUE_URL":        serverURL,
		"BUNPO_QUEUE_READ_TOKEN": queueReadToken,
		"GITHUB_RUN_ID":          buildRunID,
		"GITHUB_RUN_ATTEMPT":     "1",
	}
	statusCode, statusOut := shell(final, g.queueStatusScript, "", statusEnv)
	wantStatus := 0
	if qs.keepFailing || qs.killerFile {
		wantStatus = 1
	}
	if statusCode != wantStatus {
		return fmt.Errorf("status exited %d, want %d\n%s", statusCode, wantStatus, statusOut)
	}

	buildID := buildRunID + "-1"
	if err := verifyQueueAccepted(serverURL, buildID, expected, r, qs); err != nil {
		return fmt.Errorf("%v\nstatus output:\n%s", err, statusOut)
	}

	reportPath := filepath.Join(final, ".bunpo", "report-0.json")
	if qs.killerFile {
		if _, err := os.Stat(reportPath); !errors.Is(err, fs.ErrNotExist) {
			return errors.New("unresolved build should not write report-0.json")
		}
		return nil
	}

	reported, err := readReported(reportPath)
	if err != nil {
		return fmt.Errorf("status report: %v\n%s", err, statusOut)
	}
	if !slices.Equal(reported, expected) {
		return fmt.Errorf("report-0.json reported %q, want %q\n%s", reported, expected, statusOut)
	}

	if code, out := shell(final, g.queueMergeScript, "", map[string]string{"PATH": path}); code != 0 {
		return fmt.Errorf("merge exited %d\n%s", code, out)
	}
	return nil
}

func checkQueueEmpty(dir string, r runner, g guide, path, fixtures, serverURL string) error {
	m := filepath.Join(dir, "empty")
	if err := prepareQueueSuite(m, r, g, queueScenario{}, fixtures); err != nil {
		return err
	}

	emptyFile := filepath.Join(m, "empty.txt")
	if err := os.WriteFile(emptyFile, nil, 0o644); err != nil {
		return err
	}
	code, out := shell(m, "bash run-batch.sh", "", map[string]string{
		"PATH":             path,
		"BUNPO_TESTS_FILE": emptyFile,
	})
	if code != 0 {
		return fmt.Errorf("run-batch.sh with empty file exited %d\n%s", code, out)
	}
	if _, err := os.Stat(filepath.Join(m, r.reportFile)); !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("run-batch.sh wrote %s on an empty BUNPO_TESTS_FILE", r.reportFile)
	}

	// Empty stdin into bunpo --queue must fail without running tests.
	env := queueEnv(path, serverURL, 0, 1, fmt.Sprintf("gc-%s-empty", r.name))
	idx := strings.Index(g.queueMachineScript, "bunpo --queue")
	if idx < 0 {
		return errors.New("cannot locate bunpo --queue invocation in guide script")
	}
	emptyCmd := g.queueMachineScript[idx:]
	code, out = shell(m, emptyCmd, "", env)
	if code == 0 {
		return fmt.Errorf("bunpo --queue with empty stdin succeeded\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(m, r.reportFile)); !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("bunpo --queue wrote %s on empty stdin", r.reportFile)
	}
	return nil
}

func runKilledMachineScenario(dir string, machineDirs []string, script, path, serverURL, buildRunID string) error {
	marker := filepath.Join(dir, "victim-started")
	env0 := queueEnv(path, serverURL, 0, len(machineDirs), buildRunID)
	env0["BUNPO_KILL_MARKER"] = marker

	cmd0 := exec.Command("bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script)
	cmd0.Dir = machineDirs[0]
	cmd0.Env = mergeEnv(env0)
	cmd0.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var out0 bytes.Buffer
	cmd0.Stdout, cmd0.Stderr = &out0, &out0
	if err := cmd0.Start(); err != nil {
		return fmt.Errorf("start machine 0: %w", err)
	}

	done0 := make(chan error, 1)
	go func() { done0 <- cmd0.Wait() }()

	deadline := time.Now().Add(20 * time.Second)
	started := false
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			started = true
			break
		}
		select {
		case err := <-done0:
			return fmt.Errorf("machine 0 exited before starting held batch: %v\n%s", err, out0.String())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !started {
		_ = syscall.Kill(-cmd0.Process.Pid, syscall.SIGKILL)
		<-done0
		return fmt.Errorf("machine 0 never reached held batch\n%s", out0.String())
	}

	// Kill machine 0's entire process group mid-batch to simulate runner preemption.
	_ = syscall.Kill(-cmd0.Process.Pid, syscall.SIGKILL)
	<-done0

	env1 := queueEnv(path, serverURL, 1, len(machineDirs), buildRunID)
	code1, out1 := shell(machineDirs[1], script, "", env1)
	if code1 != 0 {
		return fmt.Errorf("surviving machine 1 exited %d\n%s", code1, out1)
	}
	return nil
}

func prepareQueueSuite(dir string, r runner, g guide, qs queueScenario, fixtures string) error {
	baseScenario := scenario{decoys: qs.decoys, noTests: qs.noTests}
	if err := prepareSuite(dir, r, g, baseScenario, fixtures); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "run-batch.sh"), []byte(g.queueBatchScript), 0o755); err != nil {
		return err
	}
	if qs.noTests {
		return nil
	}
	if !qs.keepFailing {
		_ = os.Remove(filepath.Join(dir, filepath.FromSlash(r.failing)))
	}
	if qs.killMachine0 {
		p := filepath.Join(dir, filepath.FromSlash(r.holdPath))
		if err := os.WriteFile(p, []byte(r.holdBody), 0o644); err != nil {
			return err
		}
	}
	if qs.killerFile {
		p := filepath.Join(dir, filepath.FromSlash(r.killPath))
		if err := os.WriteFile(p, []byte(r.killBody), 0o644); err != nil {
			return err
		}
	}
	if qs.playwrightSetup {
		cfg := `const { defineConfig } = require("@playwright/test");
module.exports = defineConfig({
  testDir: "tests",
  projects: [
    { name: "setup", testMatch: /.*\.setup\.js/ },
    { name: "e2e", dependencies: ["setup"], testIgnore: /.*\.setup\.js/ },
  ],
});
`
		setupTest := `const fs = require("node:fs");
const { test } = require("@playwright/test");
test("authenticate", async () => {
  fs.mkdirSync("playwright/.auth", { recursive: true });
  fs.writeFileSync("playwright/.auth/state.json", JSON.stringify({ ok: true }));
});
`
		usesAuth := `const fs = require("node:fs");
const { test, expect } = require("@playwright/test");
test("uses saved auth state", async () => {
  const data = JSON.parse(fs.readFileSync("playwright/.auth/state.json", "utf8"));
  expect(data.ok).toBe(true);
});
`
		if err := os.WriteFile(filepath.Join(dir, "playwright.config.js"), []byte(cfg), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "tests", "auth.setup.js"), []byte(setupTest), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "tests", "uses_auth.spec.js"), []byte(usesAuth), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func discoverFiles(dir, listCommand, path string) ([]string, error) {
	cmd := exec.Command("bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", listCommand)
	cmd.Dir = dir
	cmd.Env = mergeEnv(map[string]string{"PATH": path})
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("discovery failed: %v\n%s%s", err, stdout.String(), stderr.String())
	}
	var files []string
	for line := range strings.Lines(stdout.String()) {
		p := strings.TrimSpace(line)
		p = strings.TrimPrefix(p, "./")
		if p != "" {
			files = append(files, p)
		}
	}
	slices.Sort(files)
	files = slices.Compact(files)
	return files, nil
}

func verifyQueueAccepted(serverURL, buildID string, expected []string, r runner, qs queueScenario) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL+"/api/v1/builds/"+buildID+"/tests?limit=1000", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+queueReadToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("query build tests: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET /tests returned %d: %s", resp.StatusCode, body)
	}

	var parsed struct {
		Tests []struct {
			Test       string `json:"test"`
			State      string `json:"state"`
			Generation int    `json:"generation"`
			Attempts   []struct {
				Status string `json:"status"`
			} `json:"attempts"`
		} `json:"tests"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("decode /tests: %w", err)
	}

	gotPaths := make([]string, len(parsed.Tests))
	for i, tv := range parsed.Tests {
		gotPaths[i] = tv.Test
		accepted := 0
		for _, a := range tv.Attempts {
			if a.Status == "passed" || a.Status == "failed" || a.Status == "skipped" {
				accepted++
			}
		}
		switch {
		case qs.killerFile && tv.Test == r.killPath:
			if tv.State != "unresolved" || accepted != 0 || tv.Generation != 2 {
				return fmt.Errorf("killer file %s: state=%s accepted=%d gen=%d; want unresolved, 0, 2",
					tv.Test, tv.State, accepted, tv.Generation)
			}
		case qs.keepFailing && tv.Test == r.failing:
			if tv.State != "failed" || accepted != 1 {
				return fmt.Errorf("failing file %s: state=%s accepted=%d; want failed, 1",
					tv.Test, tv.State, accepted)
			}
		default:
			if tv.State != "passed" || accepted != 1 {
				return fmt.Errorf("file %s: state=%s accepted=%d; want passed, 1",
					tv.Test, tv.State, accepted)
			}
		}
		if qs.killMachine0 && tv.Test == r.holdPath && tv.Generation < 2 {
			return fmt.Errorf("held file %s had generation %d; expected retry (>=2) after machine 0 kill",
				tv.Test, tv.Generation)
		}
	}
	slices.Sort(gotPaths)
	if !slices.Equal(gotPaths, expected) {
		return fmt.Errorf("server tests = %q, want %q", gotPaths, expected)
	}
	return nil
}

func queueEnv(path, serverURL string, index, total int, buildRunID string) map[string]string {
	return map[string]string{
		"PATH":               path,
		"INDEX":              fmt.Sprint(index),
		"TOTAL":              fmt.Sprint(total),
		"BUNPO_QUEUE_URL":    serverURL,
		"BUNPO_QUEUE_TOKEN":  queueWriteToken,
		"GITHUB_RUN_ID":      buildRunID,
		"GITHUB_RUN_ATTEMPT": "1",
	}
}

func spliceQueueFlags(script, extraFlags string) string {
	return strings.Replace(script, "-- bash run-batch.sh", extraFlags+" -- bash run-batch.sh", 1)
}

func mergeEnv(overrides map[string]string) []string {
	env := os.Environ()
	for k, v := range overrides {
		env = append(env, k+"="+v)
	}
	return env
}
