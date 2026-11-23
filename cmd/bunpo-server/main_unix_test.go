//go:build unix

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRunSIGTERMCompletesInFlightRequestAndExits0(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "bunpo-server")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build bunpo-server: %v\n%s", err, out)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	cmd := exec.Command(bin, "--addr", addr)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Env = []string{
		"BUNPO_QUEUE_TOKEN=" + validTestEnv("BUNPO_QUEUE_TOKEN"),
		"BUNPO_QUEUE_READ_TOKEN=" + validTestEnv("BUNPO_QUEUE_READ_TOKEN"),
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	})

	hc := &http.Client{Timeout: 2 * time.Second}
	baseURL := "http://" + addr

	// Wait until /healthz answers 200 OK.
	readyDeadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := hc.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if time.Now().After(readyDeadline) {
			t.Fatalf("bunpo-server did not become healthy: %s", stderr.String())
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Open a raw TCP connection and send a POST /init with valid headers and
	// half of the JSON body so the server handler is actively blocked reading it.
	body := `{"version":1,"runner_id":"r1","machine_index":0,"config":{"machines":1,"lease_ttl_ms":60000,"build_timeout_ms":300000,"max_attempts":3,"max_batch":1},"tests":["tests/a.py"],"predictions_ms":[100]}`
	splitAt := len(body) / 2
	reqHead := fmt.Sprintf(
		"POST /api/v1/builds/sigterm-inflight/init HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		addr, validTestEnv("BUNPO_QUEUE_TOKEN"), len(body), body[:splitAt],
	)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte(reqHead)); err != nil {
		t.Fatal(err)
	}

	// Confirm the server is processing requests after the partial POST was sent.
	resp, err := hc.Get(baseURL + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz after partial POST = (%v, %v); want 200", resp, err)
	}
	resp.Body.Close()

	// Send SIGTERM while the POST /init request is in-flight.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	// Wait until Shutdown closes the listening socket (new connections refused)
	// while our in-flight connection stays open.
	shutDeadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			break
		}
		_ = c.Close()
		if time.Now().After(shutDeadline) {
			t.Fatal("listener stayed open after SIGTERM")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Confirm the server has not exited yet while the in-flight POST is still open.
	select {
	case <-done:
		t.Fatalf("bunpo-server exited before in-flight request finished (err=%v, stderr=%s)", waitErr, stderr.String())
	default:
	}

	// Finish sending the request body; the server must return 200 OK and then exit 0.
	if _, err := conn.Write([]byte(body[splitAt:])); err != nil {
		t.Fatal(err)
	}
	httpResp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("reading in-flight POST response: %v (stderr=%s)", err, stderr.String())
	}
	httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		t.Fatalf("in-flight POST status = %d; want 200 (stderr=%s)", httpResp.StatusCode, stderr.String())
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for bunpo-server to exit after in-flight request finished (stderr=%s)", stderr.String())
	}
	if waitErr != nil {
		t.Fatalf("bunpo-server exit after SIGTERM = %v; want 0 (stderr=%s)", waitErr, stderr.String())
	}
}
