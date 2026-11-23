package main

import "testing"

func TestRunRejectsBadConfig(t *testing.T) {
	noEnv := func(string) string { return "" }
	if code := run(nil, noEnv); code != 2 {
		t.Fatalf("missing tokens: code = %d; want 2", code)
	}

	shortEnv := func(k string) string {
		if k == "BUNPO_QUEUE_TOKEN" {
			return "short"
		}
		return "read-secret-token-0123456789abcdef"
	}
	if code := run(nil, shortEnv); code != 2 {
		t.Fatalf("short token: code = %d; want 2", code)
	}

	sameEnv := func(string) string { return "same-secret-token-0123456789abcdef" }
	if code := run(nil, sameEnv); code != 2 {
		t.Fatalf("equal tokens: code = %d; want 2", code)
	}

	if code := run([]string{"extra"}, sameEnv); code != 2 {
		t.Fatalf("unexpected arg: code = %d; want 2", code)
	}
	if code := run([]string{"-h"}, noEnv); code != 0 {
		t.Fatalf("-h: code = %d; want 0", code)
	}
}
