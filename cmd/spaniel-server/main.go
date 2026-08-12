// Command spaniel-server serves the Spaniel trace receiver and query API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/meoyawn/spaniel"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := flag.String("addr", "", "listener address")
	databasePath := flag.String("db", "", "SQLite database path")
	healthcheckURL := flag.String("healthcheck", "", "probe one Spaniel health URL and exit")
	flag.Parse()
	if *healthcheckURL != "" {
		return checkHealth(*healthcheckURL)
	}

	server, err := spaniel.NewServer(*addr, spaniel.Config{DatabasePath: *databasePath})
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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("build Spaniel health request %q: %w", rawURL, err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("request Spaniel health URL %q: %w", rawURL, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("spaniel health URL %q status = %d, want %d", rawURL, response.StatusCode, http.StatusOK)
	}
	return nil
}
