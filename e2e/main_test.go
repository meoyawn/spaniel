package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
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
	goleak.VerifyTestMain(main)
}

func TestSQLiteServerAndTraceCLI(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "spaniel.sqlite")
	server, origin := startServer(t, databasePath)
	ingestTrace(t, origin)

	gcx := runTraceCLI(t, databasePath, "gcx")
	var gcxTrace struct {
		Batches []struct {
			ScopeSpans []struct {
				Spans []struct {
					TraceID string `json:"traceId"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"batches"`
	}
	decodeJSON(t, gcx, &gcxTrace)
	if len(gcxTrace.Batches) != 1 || len(gcxTrace.Batches[0].ScopeSpans) != 1 ||
		len(gcxTrace.Batches[0].ScopeSpans[0].Spans) != 1 ||
		gcxTrace.Batches[0].ScopeSpans[0].Spans[0].TraceID != testTraceID {
		t.Fatalf("gcx trace = %s", gcx)
	}

	jaeger := runTraceCLI(t, databasePath, "jaeger")
	var jaegerTrace struct {
		Data []struct {
			TraceID   string         `json:"traceID"`
			Spans     []any          `json:"spans"`
			Processes map[string]any `json:"processes"`
		} `json:"data"`
	}
	decodeJSON(t, jaeger, &jaegerTrace)
	if len(jaegerTrace.Data) != 1 || jaegerTrace.Data[0].TraceID != testTraceID ||
		len(jaegerTrace.Data[0].Spans) != 1 || len(jaegerTrace.Data[0].Processes) != 1 {
		t.Fatalf("jaeger trace = %s", jaeger)
	}

	shutdownServer(t, server)
	_, restartedOrigin := startServer(t, databasePath)
	response := get(t, restartedOrigin+"/api/v1/traces/"+testTraceID)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("persisted trace status = %d: %s", response.StatusCode, body)
	}

	const readers = 8
	results := make(chan error, readers)
	for range readers {
		go func() {
			command := exec.CommandContext(t.Context(), "go", "run", "./cmd/spaniel", "-db", databasePath, "-format", "gcx", testTraceID)
			command.Dir = ".."
			output, err := command.CombinedOutput()
			if err != nil {
				results <- fmt.Errorf("concurrent trace read: %w: %s", err, output)
				return
			}
			results <- nil
		}()
	}
	for range readers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func startServer(t *testing.T, databasePath string) (*http.Server, string) {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := spaniel.NewServer(listener.Addr().String(), spaniel.Config{DatabasePath: databasePath})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		<-done
	})
	return server, "http://" + listener.Addr().String()
}

func shutdownServer(t *testing.T, server *http.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func ingestTrace(t *testing.T, origin string) {
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
	payload, err := ptraceotlp.NewExportRequestFromTraces(traces).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/v1/traces", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	response, err := newClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("ingest status = %d: %s", response.StatusCode, body)
	}
}

func runTraceCLI(t *testing.T, databasePath string, format string) []byte {
	t.Helper()
	command := exec.CommandContext(t.Context(), "go", "run", "./cmd/spaniel", "-db", databasePath, "-format", format, testTraceID)
	command.Dir = ".."
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run trace CLI: %v: %s", err, output)
	}
	return output
}

func get(t *testing.T, rawURL string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := newClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func newClient() *http.Client {
	return &http.Client{Timeout: time.Second}
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
