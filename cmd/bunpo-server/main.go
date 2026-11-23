// Command bunpo-server runs the standalone HTTP server for Bunpō's live queue.
//
// TLS is terminated by the operator's reverse proxy (or omitted on loopback /
// private cluster networks). Tokens are read from BUNPO_QUEUE_TOKEN and
// BUNPO_QUEUE_READ_TOKEN only, never flags.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	os.Exit(run(os.Args[1:], os.Getenv))
}

func run(args []string, getenv func(string) string) int {
	fs := flag.NewFlagSet("bunpo-server", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr, "TCP address to listen on")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "bunpo-server: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	handler, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: getenv("BUNPO_QUEUE_TOKEN"),
		ReadToken:  getenv("BUNPO_QUEUE_READ_TOKEN"),
		Limits:     queue.DefaultLimits(),
		Log:        queueapi.NopEventLog{},
		Logger:     logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "bunpo-server: %v\n", err)
		return 2
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bunpo-server: listen %s: %v\n", *addr, err)
		return 1
	}

	srv := &http.Server{
		Handler:           handler,
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
