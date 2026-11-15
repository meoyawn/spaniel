package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/meoyawn/spaniel"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/goleak"
)

const (
	testTraceID = "0123456789abcdef0123456789abcdef"
	testSpanID  = "0123456789abcdef"
)

func TestMain(main *testing.M) {
	runSupervisorIfRequested()
	goleak.VerifyTestMain(main)
}

func TestSQLiteServerHTTP(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "spaniel.sqlite")
	client := newClient(t)
	server, origin := startServer(t, client, databasePath)
	ingestTrace(t, client, origin)
	assertGCXTrace(t, readOK(t, client, origin+"/api/v1/traces/"+testTraceID))
	assertJaegerTrace(t, readOK(t, client, origin+"/api/v1/traces/"+testTraceID+"?format=jaeger"))
	shutdownServer(t, client, server)
	_, restartedOrigin := startServer(t, client, databasePath)
	readOK(t, client, restartedOrigin+"/api/v1/traces/"+testTraceID)
}

func readOK(t *testing.T, client *http.Client, rawURL string) []byte {
	t.Helper()
	response := get(t, client, rawURL)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET trace status = %d: %s", response.StatusCode, body)
	}
	return body
}

func assertGCXTrace(t *testing.T, body []byte) {
	t.Helper()
	var trace struct {
		Batches []struct {
			ScopeSpans []struct {
				Spans []struct {
					TraceID string `json:"traceId"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"batches"`
	}
	decodeJSON(t, body, &trace)
	if len(trace.Batches) != 2 {
		t.Fatalf("gcx trace = %s", body)
	}
	for _, batch := range trace.Batches {
		if len(batch.ScopeSpans) != 1 || len(batch.ScopeSpans[0].Spans) != 1 ||
			batch.ScopeSpans[0].Spans[0].TraceID != testTraceID {
			t.Fatalf("gcx trace = %s", body)
		}
	}
}

func assertJaegerTrace(t *testing.T, body []byte) {
	t.Helper()
	var trace struct {
		Data []struct {
			TraceID   string         `json:"traceID"`
			Spans     []any          `json:"spans"`
			Processes map[string]any `json:"processes"`
		} `json:"data"`
	}
	decodeJSON(t, body, &trace)
	if len(trace.Data) != 1 || trace.Data[0].TraceID != testTraceID ||
		len(trace.Data[0].Spans) != 2 || len(trace.Data[0].Processes) != 1 {
		t.Fatalf("jaeger trace = %s", body)
	}
}

func TestCommandUsesDefaultSQLitePathAndGCXFormat(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "spaniel.sqlite")
	client := newClient(t)
	origin := startCommand(t, client, databasePath)
	ingestTrace(t, client, origin)
	assertGCXTrace(t, readOK(t, client, origin+"/api/v1/traces/"+testTraceID))
}

func startCommand(t *testing.T, client *http.Client, databasePath string) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	serverOutput := &bytes.Buffer{}
	server, err := startSupervisor(t.Context(), []supervisedCommand{{
		Args: []string{"go", "run", "./cmd/spaniel", "-addr", address},
		Dir:  "..",
		Env:  append(os.Environ(), "SPANIEL_DATABASE_PATH="+databasePath),
	}}, supervisorOptions{Stderr: serverOutput, Stdout: serverOutput})
	if err != nil {
		t.Fatalf("start Spaniel server CLI: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.shutdown(shutdownCtx, syscall.SIGTERM); err != nil {
			t.Errorf("stop Spaniel server CLI: %v", err)
		}
	})

	origin := "http://" + address
	waitForHealth(t, client, origin, serverOutput)
	return origin
}

func waitForHealth(t *testing.T, client *http.Client, origin string, serverOutput *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, origin+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Spaniel server CLI did not become healthy: %s", serverOutput)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func startServer(t *testing.T, client *http.Client, databasePath string) (*http.Server, string) {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := spaniel.NewServer(
		listener.Addr().String(),
		spaniel.Config{DatabasePath: databasePath},
	)
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

func shutdownServer(t *testing.T, client *http.Client, server *http.Server) {
	t.Helper()
	// An unused pooled dial is StateNew on the server. Close the test-owned pool
	// before shutdown instead of waiting for its first-request timeout.
	client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func ingestTrace(t *testing.T, client *http.Client, origin string) {
	t.Helper()
	traces := ptrace.NewTraces()
	resource := traces.ResourceSpans().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "spaniel-e2e")
	scope := resource.ScopeSpans().AppendEmpty()
	scope.Scope().SetName("spaniel.e2e")
	span := scope.Spans().AppendEmpty()
	span.SetTraceID(traceID(t, testTraceID))
	span.SetSpanID(spanID(t, testSpanID))
	span.SetName("persisted span")
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_000, 0)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_001, 0)))
	span.Attributes().PutStr("e2e.proof", "sqlite")
	secondSpan := scope.Spans().AppendEmpty()
	secondSpan.SetTraceID(traceID(t, testTraceID))
	secondSpan.SetSpanID(spanID(t, "fedcba9876543210"))
	secondSpan.SetParentSpanID(spanID(t, testSpanID))
	secondSpan.SetName("persisted child span")
	secondSpan.SetStartTimestamp(
		pcommon.NewTimestampFromTime(time.Unix(1_700_000_000, 500_000_000)),
	)
	secondSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_001, 0)))
	payload, err := ptraceotlp.NewExportRequestFromTraces(traces).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		origin+"/v1/traces",
		bytes.NewReader(payload),
	)
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

func get(t *testing.T, client *http.Client, rawURL string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func newClient(t *testing.T) *http.Client {
	t.Helper()
	// Each test owns its pool, so closing unused dials cannot affect siblings.
	client := &http.Client{
		Transport: &http.Transport{},
		Timeout:   time.Second,
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func decodeJSON(t *testing.T, body []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(body, destination); err != nil {
		t.Fatalf("decode JSON: %v: %s", err, body)
	}
}

func traceID(t *testing.T, value string) pcommon.TraceID {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	var id pcommon.TraceID
	copy(id[:], decoded)
	return id
}

func spanID(t *testing.T, value string) pcommon.SpanID {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	var id pcommon.SpanID
	copy(id[:], decoded)
	return id
}
