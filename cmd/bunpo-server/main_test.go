package main

import (
	"net"
	"testing"
)

func TestRunConfigAndPortInUse(t *testing.T) {
	noEnv := func(string) string { return "" }
	if code := run(nil, noEnv); code != 2 {
		t.Fatalf("missing tokens: code = %d; want 2", code)
	}
	if code := run([]string{"extra"}, noEnv); code != 2 {
		t.Fatalf("unexpected arg: code = %d; want 2", code)
	}
	if code := run([]string{"-h"}, noEnv); code != 0 {
		t.Fatalf("-h: code = %d; want 0", code)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	validEnv := func(k string) string {
		if k == "BUNPO_QUEUE_TOKEN" {
			return "write-secret-token-0123456789abcdef"
		}
		if k == "BUNPO_QUEUE_READ_TOKEN" {
			return "read-secret-token-fedcba9876543210"
		}
		return ""
	}
	if code := run([]string{"--addr", ln.Addr().String()}, validEnv); code != 1 {
		t.Fatalf("port in use: code = %d; want 1", code)
	}
}
