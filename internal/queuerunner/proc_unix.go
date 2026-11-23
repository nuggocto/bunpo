//go:build unix

package queuerunner

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func stopProcessGroup(cmd *exec.Cmd, done <-chan error, grace time.Duration) error {
	if cmd.Process != nil {
		// Negative PID signals the entire process group created by Setpgid.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	if grace <= 0 {
		grace = 5 * time.Second
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case err := <-done:
		return err
	case <-timer.C:
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return <-done
	}
}

func processSignaled(ps *os.ProcessState) bool {
	if ps == nil {
		return true
	}
	if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return true
	}
	code := ps.ExitCode()
	return code < 0 || code > 128
}
