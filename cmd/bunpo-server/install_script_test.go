//go:build unix

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestInstallScriptSelectsCLIOnly verifies scripts/install.sh installs only the
// bunpo CLI binary and ignores bunpo-server-* even when both appear in SHA256SUMS.
func TestInstallScriptSelectsCLIOnly(t *testing.T) {
	relDir := t.TempDir()
	runnerTemp := t.TempDir()
	ghPath := filepath.Join(t.TempDir(), "gh_path")
	ghOutput := filepath.Join(t.TempDir(), "gh_output")
	if err := os.WriteFile(ghPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ghOutput, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	cliBytes := []byte("#!/bin/sh\necho cli-ok\n")
	srvBytes := []byte("#!/bin/sh\necho server-should-not-be-installed\n")

	cliName := "bunpo-linux-amd64"
	srvName := "bunpo-server-linux-amd64"
	if err := os.WriteFile(filepath.Join(relDir, cliName), cliBytes, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(relDir, srvName), srvBytes, 0o755); err != nil {
		t.Fatal(err)
	}

	cliSum := sha256.Sum256(cliBytes)
	srvSum := sha256.Sum256(srvBytes)
	// Put bunpo-server first so a loose prefix match would pick the wrong entry.
	sums := fmt.Sprintf("%s  %s\n%s  %s\n",
		hex.EncodeToString(srvSum[:]), srvName,
		hex.EncodeToString(cliSum[:]), cliName,
	)
	if err := os.WriteFile(filepath.Join(relDir, "SHA256SUMS"), []byte(sums), 0o644); err != nil {
		t.Fatal(err)
	}

	scriptPath, err := filepath.Abs(filepath.Join("..", "..", "scripts", "install.sh"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", scriptPath)
	cmd.Env = append(os.Environ(),
		"ACTION_REPOSITORY=nuggocto/bunpo",
		"RUNNER_OS_NAME=Linux",
		"RUNNER_ARCH_NAME=X64",
		"RUNNER_TEMP="+runnerTemp,
		"GITHUB_PATH="+ghPath,
		"GITHUB_OUTPUT="+ghOutput,
		"BUNPO_RELEASE_DIR="+relDir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, out)
	}

	pathBytes, err := os.ReadFile(ghPath)
	if err != nil {
		t.Fatal(err)
	}
	installDir := strings.TrimSpace(string(pathBytes))
	if installDir == "" {
		t.Fatal("GITHUB_PATH was not populated")
	}

	gotCLI, err := os.ReadFile(filepath.Join(installDir, "bunpo"))
	if err != nil {
		t.Fatalf("installed bunpo missing: %v", err)
	}
	if string(gotCLI) != string(cliBytes) {
		t.Fatalf("installed bunpo has wrong contents: %q", gotCLI)
	}
	if _, err := os.Stat(filepath.Join(installDir, "bunpo-server")); !os.IsNotExist(err) {
		t.Fatalf("bunpo-server must not be installed by action install.sh")
	}
	if _, err := os.Stat(filepath.Join(installDir, srvName)); !os.IsNotExist(err) {
		t.Fatalf("%s must not be copied by action install.sh", srvName)
	}
}
