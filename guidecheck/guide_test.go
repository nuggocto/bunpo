//go:build unix

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGuidesParseAndShellSyntax verifies every runner guide's static and
// live-queue workflows parse cleanly and contain valid bash syntax.
func TestGuidesParseAndShellSyntax(t *testing.T) {
	for _, r := range runners {
		t.Run(r.name, func(t *testing.T) {
			path := filepath.Join("..", "docs", "guides", r.name+".md")
			g, err := readGuide(path)
			if err != nil {
				t.Fatalf("readGuide(%s): %v", path, err)
			}

			scripts := []struct {
				name string
				body string
			}{
				{"static machine", g.machineScript},
				{"queue run-batch.sh", g.queueBatchScript},
				{"queue machine", g.queueMachineScript},
				{"queue status", g.queueStatusScript},
				{"queue merge", g.queueMergeScript},
			}
			for _, sc := range scripts {
				if strings.TrimSpace(sc.body) == "" {
					t.Fatalf("%s guide missing %s script", r.name, sc.name)
				}

				cmd := exec.Command("bash", "-n")
				cmd.Stdin = strings.NewReader(sc.body)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%s (%s) bash syntax error: %v\n%s", r.name, sc.name, err, out)
				}
			}
		})
	}
}

func TestFixturesDirCheck(t *testing.T) {
	base := t.TempDir()
	dummyBunpo := filepath.Join(base, "bunpo")
	if err := os.WriteFile(dummyBunpo, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	okDir := filepath.Join(base, "ok-0700")
	if err := os.Mkdir(okDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(okDir, 0o700); err != nil {
		t.Fatal(err)
	}

	worldWritable := filepath.Join(base, "bad-0777")
	if err := os.Mkdir(worldWritable, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(worldWritable, 0o777); err != nil {
		t.Fatal(err)
	}

	symDir := filepath.Join(base, "bad-symlink")
	if err := os.Symlink(okDir, symDir); err != nil {
		t.Fatal(err)
	}

	t.Run("owned 0700 accepted", func(t *testing.T) {
		if _, err := toolPath(t.TempDir(), dummyBunpo, okDir); err != nil {
			t.Fatalf("toolPath(0700) unexpected error: %v", err)
		}
	})

	t.Run("0777 rejected", func(t *testing.T) {
		if _, err := toolPath(t.TempDir(), dummyBunpo, worldWritable); err == nil {
			t.Fatalf("toolPath(0777) succeeded; want error rejecting group/world-writable directory")
		}
	})

	t.Run("symlink rejected", func(t *testing.T) {
		if _, err := toolPath(t.TempDir(), dummyBunpo, symDir); err == nil {
			t.Fatalf("toolPath(symlink) succeeded; want error rejecting symlink")
		}
	})
}
