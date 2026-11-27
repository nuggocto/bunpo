//go:build !unix

package queuerunner

import (
	"os"
	"os/exec"
	"time"
)

func configureProcessGroup(_ *exec.Cmd) {}

func stopProcessGroup(cmd *exec.Cmd, done <-chan error, _ time.Duration) error {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}

	return <-done
}

func processSignaled(ps *os.ProcessState) bool {
	return ps == nil || ps.ExitCode() < 0
}
