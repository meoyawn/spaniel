// Command spaniel serves the Spaniel trace receiver and query API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/meoyawn/spaniel"
)

const (
	shutdownTimeout    = 10 * time.Second
	healthcheckTimeout = 2 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := flag.String("addr", "", "listener address")
	databasePath := flag.String("db", "", "SQLite database path")
	maxStorageBytes := flag.Int64("max-storage-bytes", 0,
		"evict the oldest traces past this many database bytes (default $SPANIEL_MAX_STORAGE_BYTES, else 1 GiB)")
	healthcheckURL := flag.String("healthcheck", "", "probe one Spaniel health URL and exit")
	flag.Parse()
	if *healthcheckURL != "" {
		return checkHealth(*healthcheckURL)
	}

	if *maxStorageBytes == 0 {
		parsed, err := storageLimitFromEnvironment()
		if err != nil {
			return err
		}
		*maxStorageBytes = parsed
	}
	server, err := spaniel.NewServer(*addr, spaniel.Config{
		DatabasePath: *databasePath, MaxStorageBytes: *maxStorageBytes,
	})
	if err != nil {
		return fmt.Errorf("create Spaniel: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve Spaniel on %q: %w", *addr, err)
		}
		return nil
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down Spaniel on %q: %w", *addr, err)
	}
	if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve Spaniel on %q: %w", *addr, err)
	}
	return nil
}

func checkHealth(rawURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("build Spaniel health request %q: %w", rawURL, err)
	}
	client := &http.Client{Timeout: healthcheckTimeout}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("request Spaniel health URL %q: %w", rawURL, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"spaniel health URL %q status = %d, want %d",
			rawURL,
			response.StatusCode,
			http.StatusOK,
		)
	}
	return nil
}

// storageLimitFromEnvironment reads SPANIEL_MAX_STORAGE_BYTES; zero keeps the default.
func storageLimitFromEnvironment() (int64, error) {
	configured := os.Getenv("SPANIEL_MAX_STORAGE_BYTES")
	if configured == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(configured, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse SPANIEL_MAX_STORAGE_BYTES: %w", err)
	}
	return parsed, nil
}
