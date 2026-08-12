package spaniel

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/goleak"
)

const testTraceID = "0123456789abcdef0123456789abcdef"

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestQueryBlocksUntilMatchingSpanArrives(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	result := make(chan queryResponse, 1)
	go func() {
		result <- queryTrace(t, client, origin, traceQuery{TraceID: testTraceID, RequiredSpans: []requiredSpan{{ServiceName: "worker", Name: "job", MinCount: 1}}})
	}()

	select {
	case <-result:
		t.Fatal("query completed before ingestion")
	case <-time.After(20 * time.Millisecond):
	}
	ingest(t, client, origin, testTraces(t, testTraceID, "0000000000000001", "worker", "job"), false)
	select {
	case response := <-result:
		if len(response.Spans) != 1 || response.Revision != 1 {
			t.Fatalf("query response = %#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("query did not wake after matching ingestion")
	}
}

func TestJSONAndProtobufIngestionAndMinCount(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	ingest(t, client, origin, testTraces(t, testTraceID, "0000000000000001", "api", "request"), false)
	ingest(t, client, origin, testTraces(t, testTraceID, "0000000000000002", "api", "request"), true)
	response := queryTrace(t, client, origin, traceQuery{TraceID: testTraceID, RequiredSpans: []requiredSpan{{ServiceName: "api", Name: "request", MinCount: 2}}})
	if len(response.Spans) != 2 || len(response.ResourceSpans) == 0 {
		t.Fatalf("response spans/resourceSpans = %d/%s", len(response.Spans), response.ResourceSpans)
	}
}

func TestJSONAndProtobufMetricIngestion(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	for _, asJSON := range []bool{false, true} {
		metrics := pmetric.NewMetrics()
		metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("test.metric")
		exportRequest := pmetricotlp.NewExportRequestFromMetrics(metrics)
		var body []byte
		var contentType string
		var err error
		if asJSON {
			body, err = exportRequest.MarshalJSON()
			contentType = "application/json"
		} else {
			body, err = exportRequest.MarshalProto()
			contentType = "application/x-protobuf"
		}
		if err != nil {
			t.Fatalf("encode metric request: %v", err)
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/v1/metrics", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", contentType)
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("ingest metrics: %v", err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("metric ingestion status=%d, want 200", response.StatusCode)
		}
	}
}

func TestDifferentTraceDoesNotWakeQuery(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	requestBody, _ := json.Marshal(traceQuery{TraceID: testTraceID, RequiredSpans: []requiredSpan{{ServiceName: "api", Name: "request", MinCount: 1}}})
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/api/v1/traces/query", bytes.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")
	done := make(chan int, 1)
	go func() {
		response, err := client.Do(request)
		if err != nil {
			done <- 0
			return
		}
		defer response.Body.Close()
		done <- response.StatusCode
	}()
	ingest(t, client, origin, testTraces(t, "1123456789abcdef0123456789abcdef", "0000000000000001", "api", "request"), false)
	if status := <-done; status == http.StatusOK {
		t.Fatal("different trace ingestion completed query")
	}
}

func TestQueryParksUntilParentArrivesAcrossBatches(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	result := make(chan queryResponse, 1)
	go func() {
		result <- queryTrace(t, client, origin, traceQuery{TraceID: testTraceID, RequiredSpans: []requiredSpan{{ServiceName: "worker", Name: "child", MinCount: 1}}})
	}()
	ingest(t, client, origin, testTracesWithParent(t, testTraceID, "0000000000000002", "0000000000000001", "worker", "child"), false)
	select {
	case <-result:
		t.Fatal("query completed while child parent was unresolved")
	case <-time.After(20 * time.Millisecond):
	}
	ingest(t, client, origin, testTraces(t, testTraceID, "0000000000000001", "worker", "parent"), false)
	select {
	case response := <-result:
		if len(response.Spans) != 2 || !bytes.Contains(response.ResourceSpans, []byte("parentSpanId")) {
			t.Fatalf("parent relationship missing from response: %#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("query did not wake after parent arrived")
	}
}

func TestConcurrentBatchesDoNotCompleteBeforeSharedParent(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	result := make(chan queryResponse, 1)
	go func() {
		result <- queryTrace(t, client, origin, traceQuery{
			TraceID: testTraceID,
			RequiredSpans: []requiredSpan{
				{ServiceName: "worker", Name: "first child", MinCount: 1},
				{ServiceName: "worker", Name: "second child", MinCount: 1},
			},
		})
	}()
	parentID := "0000000000000009"
	batches := []ptrace.Traces{
		testTracesWithParent(t, testTraceID, "0000000000000001", parentID, "worker", "first child"),
		testTracesWithParent(t, testTraceID, "0000000000000002", parentID, "worker", "second child"),
	}
	done := make(chan struct{}, len(batches))
	for _, batch := range batches {
		go func() {
			ingest(t, client, origin, batch, false)
			done <- struct{}{}
		}()
	}
	for range batches {
		<-done
	}
	select {
	case <-result:
		t.Fatal("query completed before shared parent arrived")
	case <-time.After(20 * time.Millisecond):
	}
	ingest(t, client, origin, testTraces(t, testTraceID, parentID, "worker", "parent"), false)
	select {
	case response := <-result:
		if len(response.Spans) != 3 {
			t.Fatalf("concurrent query span count = %d, want 3", len(response.Spans))
		}
	case <-time.After(time.Second):
		t.Fatal("query did not complete after shared parent arrived")
	}
}

func TestAllowedExternalParent(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	parentID := "00000000000000aa"
	ingest(t, client, origin, testTracesWithParent(t, testTraceID, "0000000000000001", parentID, "api", "request"), false)
	response := queryTrace(t, client, origin, traceQuery{
		TraceID:                      testTraceID,
		RequiredSpans:                []requiredSpan{{ServiceName: "api", Name: "request", MinCount: 1}},
		AllowedExternalParentSpanIDs: []string{parentID},
	})
	if response.Spans[0].ParentSpanID != parentID {
		t.Fatalf("external parent = %q, want %q", response.Spans[0].ParentSpanID, parentID)
	}
}

func TestUnresolvedParentReturnsDetailedError(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	parentID := "00000000000000bb"
	ingest(t, client, origin, testTracesWithParent(t, testTraceID, "0000000000000001", parentID, "api", "request"), false)
	status, body := rawQuery(t, client, origin, traceQuery{
		TraceID:             testTraceID,
		RequiredSpans:       []requiredSpan{{ServiceName: "api", Name: "request", MinCount: 1}},
		TimeoutMilliseconds: 20,
	})
	if status != http.StatusConflict || !bytes.Contains(body, []byte(parentID)) || !bytes.Contains(body, []byte("0000000000000001")) {
		t.Fatalf("unresolved parent response = %d %s", status, body)
	}
}

func TestSelfParentAndCyclesFailGraphValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		spans ptrace.Traces
		want  string
	}{
		{name: "self parent", spans: testTracesWithParent(t, testTraceID, "0000000000000001", "0000000000000001", "api", "request"), want: "own parent"},
		{name: "two span cycle", spans: mergeTraces(
			testTracesWithParent(t, testTraceID, "0000000000000001", "0000000000000002", "api", "request"),
			testTracesWithParent(t, testTraceID, "0000000000000002", "0000000000000001", "api", "child"),
		), want: "parent cycle"},
		{name: "longer cycle", spans: mergeTraces(
			testTracesWithParent(t, testTraceID, "0000000000000001", "0000000000000002", "api", "request"),
			testTracesWithParent(t, testTraceID, "0000000000000002", "0000000000000003", "api", "middle"),
			testTracesWithParent(t, testTraceID, "0000000000000003", "0000000000000001", "api", "last"),
		), want: "parent cycle"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, origin := startTestServer(t, Config{})
			client := newTestClient(t)
			ingest(t, client, origin, test.spans, false)
			status, body := rawQuery(t, client, origin, traceQuery{TraceID: testTraceID, RequiredSpans: []requiredSpan{{ServiceName: "api", Name: "request", MinCount: 1}}})
			if status != http.StatusConflict || !bytes.Contains(body, []byte(test.want)) {
				t.Fatalf("graph response = %d %s, want %q", status, body, test.want)
			}
		})
	}
}

func TestCrossTraceParentIsNotAccepted(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	parentID := "0000000000000009"
	ingest(t, client, origin, testTraces(t, "1123456789abcdef0123456789abcdef", parentID, "api", "other trace parent"), false)
	ingest(t, client, origin, testTracesWithParent(t, testTraceID, "0000000000000001", parentID, "api", "request"), false)
	status, body := rawQuery(t, client, origin, traceQuery{TraceID: testTraceID, RequiredSpans: []requiredSpan{{ServiceName: "api", Name: "request", MinCount: 1}}, TimeoutMilliseconds: 20})
	if status != http.StatusConflict || !bytes.Contains(body, []byte("missing parent")) {
		t.Fatalf("cross-trace parent response = %d %s", status, body)
	}
}

func TestDuplicateSpanProducesDiagnostic(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	traces := testTraces(t, testTraceID, "0000000000000001", "api", "request")
	ingest(t, client, origin, traces, false)
	ingest(t, client, origin, traces, false)
	response, err := client.Get(origin + "/api/v1/diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("diagnostics status = %d, want %d", response.StatusCode, http.StatusConflict)
	}
}

func TestConflictingSpanProducesDiagnostic(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{})
	client := newTestClient(t)
	ingest(t, client, origin, testTraces(t, testTraceID, "0000000000000001", "api", "first"), false)
	ingest(t, client, origin, testTraces(t, testTraceID, "0000000000000001", "api", "conflict"), false)
	response, err := client.Get(origin + "/api/v1/diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusConflict || !bytes.Contains(body, []byte("conflicting_span_id")) {
		t.Fatalf("conflict diagnostics = %d %s", response.StatusCode, body)
	}
}

func TestMalformedRequestsAreControlled(t *testing.T) {
	t.Parallel()
	_, origin := startTestServer(t, Config{MaxBodyBytes: 32})
	client := newTestClient(t)
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/v1/traces", bytes.NewBufferString("not otlp"))
	request.Header.Set("Content-Type", "text/plain")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("unsupported content status = %d", response.StatusCode)
	}
}

func startTestServer(t *testing.T, config Config) (*http.Server, string) {
	t.Helper()
	config.DatabasePath = filepath.Join(t.TempDir(), "spaniel.sqlite")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(listener.Addr().String(), config)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	return server, "http://" + listener.Addr().String()
}

func newTestClient(t *testing.T) *http.Client {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{Transport: transport}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func testTraces(t *testing.T, traceID string, spanID string, serviceName string, name string) ptrace.Traces {
	t.Helper()
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", serviceName)
	span := resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	traceBytes, err := hex.DecodeString(traceID)
	if err != nil {
		t.Fatal(err)
	}
	spanBytes, err := hex.DecodeString(spanID)
	if err != nil {
		t.Fatal(err)
	}
	var decodedTraceID pcommon.TraceID
	copy(decodedTraceID[:], traceBytes)
	var decodedSpanID pcommon.SpanID
	copy(decodedSpanID[:], spanBytes)
	span.SetTraceID(decodedTraceID)
	span.SetSpanID(decodedSpanID)
	span.SetName(name)
	return traces
}

func testTracesWithParent(t *testing.T, traceID string, spanID string, parentID string, serviceName string, name string) ptrace.Traces {
	t.Helper()
	traces := testTraces(t, traceID, spanID, serviceName, name)
	parentBytes, err := hex.DecodeString(parentID)
	if err != nil {
		t.Fatal(err)
	}
	var decodedParentID pcommon.SpanID
	copy(decodedParentID[:], parentBytes)
	traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetParentSpanID(decodedParentID)
	return traces
}

func mergeTraces(items ...ptrace.Traces) ptrace.Traces {
	result := ptrace.NewTraces()
	for _, item := range items {
		resources := item.ResourceSpans()
		for index := range resources.Len() {
			resources.At(index).CopyTo(result.ResourceSpans().AppendEmpty())
		}
	}
	return result
}

func ingest(t *testing.T, client *http.Client, origin string, traces ptrace.Traces, protobuf bool) {
	t.Helper()
	exportRequest := ptraceotlp.NewExportRequestFromTraces(traces)
	contentType := "application/json"
	body, err := exportRequest.MarshalJSON()
	if protobuf {
		contentType = "application/x-protobuf"
		body, err = exportRequest.MarshalProto()
	}
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/v1/traces", bytes.NewReader(body))
	request.Header.Set("Content-Type", contentType)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("ingest status = %d: %s", response.StatusCode, message)
	}
}

func queryTrace(t *testing.T, client *http.Client, origin string, query traceQuery) queryResponse {
	t.Helper()
	body, _ := json.Marshal(query)
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/api/v1/traces/query", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("query status = %d: %s", response.StatusCode, message)
	}
	var result queryResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func rawQuery(t *testing.T, client *http.Client, origin string, query traceQuery) (int, []byte) {
	t.Helper()
	body, _ := json.Marshal(query)
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/api/v1/traces/query", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(response.Body)
	return response.StatusCode, responseBody
}
