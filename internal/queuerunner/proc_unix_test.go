//go:build unix

package queuerunner

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStopProcessGroupKillsGrandchildIgnoringSIGTERM(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "grandchild.pid")
	readyFile := filepath.Join(dir, "leader.ready")

	cmd := exec.Command(os.Args[0], "-test.run=^TestProcGroupHelper$")
	cmd.Env = append(os.Environ(),
		"BUNPO_PROC_HELPER=leader",
		"BUNPO_PROC_PIDFILE="+pidFile,
		"BUNPO_PROC_READYFILE="+readyFile,
	)
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start leader: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	// Wait until leader has started the grandchild and the grandchild has installed
	// its SIGTERM ignore handler.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(readyFile); err == nil {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
		t.Fatalf("grandchild pid file not ready: %v", err)
	}
	grandchildPID, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil || grandchildPID <= 0 {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
		t.Fatalf("invalid grandchild pid %q: %v", rawPID, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(grandchildPID, syscall.SIGKILL) })

	if err := syscall.Kill(grandchildPID, 0); err != nil {
		t.Fatalf("grandchild %d not alive before stopProcessGroup: %v", grandchildPID, err)
	}

	// Leader exits immediately on SIGTERM, well before grace; stopProcessGroup must
	// still SIGKILL the process group so the SIGTERM-ignoring grandchild dies.
	start := time.Now()
	_ = stopProcessGroup(cmd, done, 5*time.Second)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stopProcessGroup waited %v even though leader exited immediately", elapsed)
	}

	// Poll briefly for init/subreaper to reap the killed grandchild.
	reapDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(reapDeadline) {
		if err := syscall.Kill(grandchildPID, 0); err == syscall.ESRCH {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("grandchild %d survived stopProcessGroup after leader exited on SIGTERM", grandchildPID)
}

func TestProcGroupHelper(t *testing.T) {
	role := os.Getenv("BUNPO_PROC_HELPER")
	if role == "" {
		return
	}
	pidFile := os.Getenv("BUNPO_PROC_PIDFILE")
	readyFile := os.Getenv("BUNPO_PROC_READYFILE")

	switch role {
	case "grandchild":
		signal.Ignore(syscall.SIGTERM)
		_ = os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0o644)
		_ = os.WriteFile(readyFile, []byte("ok"), 0o644)
		time.Sleep(30 * time.Second)
		os.Exit(0)

	case "leader":
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM)

		child := exec.Command(os.Args[0], "-test.run=^TestProcGroupHelper$")
		child.Env = append(os.Environ(),
			"BUNPO_PROC_HELPER=grandchild",
			"BUNPO_PROC_PIDFILE="+pidFile,
			"BUNPO_PROC_READYFILE="+readyFile,
		)
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		// Wait for SIGTERM, then exit immediately without touching the grandchild.
		<-sigCh
		os.Exit(0)
	}
}
