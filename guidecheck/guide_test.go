//go:build unix

package main

import (
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
