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
	pgid := 0
	if cmd.Process != nil {
		pgid = cmd.Process.Pid
		// Negative PID signals the entire process group created by Setpgid.
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	}
	if grace <= 0 {
		grace = 5 * time.Second
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()

	var err error
	select {
	case err = <-done:
	case <-timer.C:
		if pgid > 0 {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		}
		err = <-done
	}
	// Even after the group leader exits, grandchildren (such as browsers under
	// a shell wrapper) may have ignored SIGTERM. SIGKILL the whole process group;
	// pgid cannot be reused while any member lives, and ESRCH is harmless.
	if pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	return err
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
