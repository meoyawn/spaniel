package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meoyawn/spaniel"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestLaunchTrimsDatabaseOverItsLimit(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "spaniel.sqlite")
	client := newClient(t)
	server, origin := startLimitedServer(t, client, databasePath, 0)
	const traces = 40
	for index := range traces {
		ingestLargeTrace(t, client, origin, index)
	}
	shutdownServer(t, client, server)

	const limit = 1 << 20
	_, origin = startLimitedServer(t, client, databasePath, limit)
	assertTraceStatus(t, client, origin, 0, http.StatusNotFound)
	assertTraceStatus(t, client, origin, traces-1, http.StatusOK)
	info, err := os.Stat(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > limit {
		t.Fatalf("database is %d bytes after launch, over its %d byte limit", info.Size(), limit)
	}
	// Ingestion keeps working on the trimmed database.
	ingestLargeTrace(t, client, origin, traces)
	assertTraceStatus(t, client, origin, traces, http.StatusOK)
}

func startLimitedServer(
	t *testing.T, client *http.Client, databasePath string, maxStorageBytes int64,
) (*http.Server, string) {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := spaniel.NewServer(listener.Addr().String(), spaniel.Config{
		DatabasePath: databasePath, MaxStorageBytes: maxStorageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		client.CloseIdleConnections()
		_ = server.Shutdown(context.Background())
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("stop Spaniel HTTP server: %v", err)
		}
	})
	return server, "http://" + listener.Addr().String()
}

func largeTraceID(index int) string {
	return fmt.Sprintf("%032x", index+1)
}

// ingestLargeTrace stores one span carrying 64 KiB, so a few dozen traces
// cross a small limit without slow setup.
func ingestLargeTrace(t *testing.T, client *http.Client, origin string, index int) {
	t.Helper()
	traces := ptrace.NewTraces()
	resource := traces.ResourceSpans().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "spaniel-e2e")
	span := resource.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(traceID(t, largeTraceID(index)))
	span.SetSpanID(spanID(t, fmt.Sprintf("%016x", index+1)))
	span.SetName("large span")
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_000, 0)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_001, 0)))
	span.Attributes().PutStr("payload", strings.Repeat(fmt.Sprintf("%x", index%16), 64<<10))
	payload, err := ptraceotlp.NewExportRequestFromTraces(traces).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/v1/traces", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("ingest status = %d: %s", response.StatusCode, body)
	}
}

func assertTraceStatus(t *testing.T, client *http.Client, origin string, index, status int) {
	t.Helper()
	response := get(t, client, origin+"/api/v1/traces/"+largeTraceID(index))
	defer response.Body.Close()
	if response.StatusCode != status {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("trace %d status = %d, want %d: %.200s", index, response.StatusCode, status, body)
	}
}
