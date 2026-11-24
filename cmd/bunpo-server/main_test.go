package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestRunConfigAndPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	notADir := filepath.Join(t.TempDir(), "file-not-dir")
	if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	noEnv := func(string) string { return "" }
	validEnv := func(k string) string {
		switch k {
		case "BUNPO_QUEUE_TOKEN":
			return "write-secret-token-0123456789abcdef"
		case "BUNPO_QUEUE_READ_TOKEN":
			return "read-secret-token-fedcba9876543210"
		default:
			return ""
		}
	}

	cases := []struct {
		name     string
		args     []string
		env      func(string) string
		wantCode int
	}{
		{"missing tokens", nil, noEnv, 2},
		{"unexpected positional argument", []string{"extra"}, noEnv, 2},
		{"help flag", []string{"-h"}, noEnv, 0},
		{"unknown flag", []string{"--nope"}, validEnv, 2},
		{"port already in use", []string{"--addr", ln.Addr().String()}, validEnv, 1},
		{"--data pointing at a file", []string{"--addr", "127.0.0.1:0", "--data", notADir}, validEnv, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := run(tc.args, tc.env); code != tc.wantCode {
				t.Fatalf("run(%v) = %d; want %d", tc.args, code, tc.wantCode)
			}
		})
	}
}
