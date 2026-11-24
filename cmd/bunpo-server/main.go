// Command bunpo-server runs the standalone HTTP server for Bunpō's live queue.
//
// TLS is terminated by the operator's reverse proxy (or omitted on loopback /
// private cluster networks). Tokens are read from BUNPO_QUEUE_TOKEN and
// BUNPO_QUEUE_READ_TOKEN only, never flags. Run at most one server per --data
// directory.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nuggocto/bunpo/internal/dashboard"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

const (
	defaultAddr       = "127.0.0.1:8080"
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 10 * time.Second
	maxHeaderBytes    = 64 << 10
)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stderr))
}

func run(args []string, getenv func(string) string, stderr io.Writer) int {
	fs := flag.NewFlagSet("bunpo-server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", defaultAddr, "TCP address to listen on")
	dataDir := fs.String("data", "", "directory for persistent build logs (omit for in-memory only)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "bunpo-server: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	writeTok, readTok := getenv("BUNPO_QUEUE_TOKEN"), getenv("BUNPO_QUEUE_READ_TOKEN")
	if err := queueapi.ValidateTokens(writeTok, readTok); err != nil {
		fmt.Fprintf(stderr, "bunpo-server: %v\n", err)
		return 2
	}

	logger := slog.New(slog.NewJSONHandler(stderr, nil))

	// Listen first so a second server on the same address fails before touching --data.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(stderr, "bunpo-server: listen %s: %v\n", *addr, err)
		return 1
	}

	var eventLog queueapi.EventLog = queueapi.NopEventLog{}
	var loaded queueapi.LoadResult
	if *dataDir != "" {
		osFS, err := queueapi.OpenOSLogFS(*dataDir)
		if err == nil {
			defer osFS.Close()
			diskLog := queueapi.NewDiskLog(osFS, logger)
			loaded, err = diskLog.Load(queue.DefaultLimits())
			eventLog = diskLog
		}
		if err != nil {
			_ = ln.Close()
			fmt.Fprintf(stderr, "bunpo-server: data dir: %v\n", err)
			return 1
		}
		logger.Info("loaded build logs", "data_dir", *dataDir, "builds", len(loaded.Builds), "damaged", len(loaded.Damaged), "truncated", loaded.Truncated, "removed", loaded.Removed)
	} else {
		logger.Info("running in memory only; pass --data DIR to persist builds across restarts")
	}

	handler, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: writeTok, ReadToken: readTok, Limits: queue.DefaultLimits(), Log: eventLog, Logger: logger,
		InitialBuilds: loaded.Builds, InitialDamaged: loaded.Damaged,
	})
	if err != nil {
		_ = ln.Close()
		fmt.Fprintf(stderr, "bunpo-server: %v\n", err)
		return 2
	}

	srv := &http.Server{
		Handler:           dashboard.Wrap(handler),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", ln.Addr().String())
		serveErr <- srv.Serve(ln)
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped with error", "err", err)
			return 1
		}
		return 0

	case <-ctx.Done():
		logger.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			logger.Error("shutdown failed", "err", err)
			_ = srv.Close()
			return 1
		}
		<-serveErr
		return 0
	}
}
