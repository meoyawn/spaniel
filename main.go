package spaniel

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

const (
	databasePathEnvironment = "SPANIEL_DATABASE_PATH"
	defaultDatabasePath     = "/tmp/spaniel/spaniel.sqlite"
	defaultMaxBodyBytes     = 16 << 20
	defaultQueryTimeout     = 30 * time.Second
	readHeaderTimeout       = 5 * time.Second
	traceIDLength           = 32
)

func DefaultDatabasePath() string {
	if databasePath := os.Getenv(databasePathEnvironment); databasePath != "" {
		return databasePath
	}
	return defaultDatabasePath
}

type Config struct {
	DatabasePath string
	MaxBodyBytes int64
}

type Diagnostic struct {
	ID      int64  `json:"id"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
	TraceID string `json:"traceId,omitempty"`
	SpanID  string `json:"spanId,omitempty"`
}

type requiredSpan struct {
	ServiceName string `json:"serviceName"`
	Name        string `json:"name"`
	MinCount    int    `json:"minCount"`
}

type traceQuery struct {
	TraceID                      string         `json:"traceId"`
	RequiredSpans                []requiredSpan `json:"requiredSpans"`
	AllowedExternalParentSpanIDs []string       `json:"allowedExternalParentSpanIds,omitempty"`
	TimeoutMilliseconds          int            `json:"timeoutMs,omitempty"`
}

type normalizedSpan struct {
	TraceID      string `json:"traceId"`
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
	Name         string `json:"name"`
	ServiceName  string `json:"serviceName"`
}

type queryResponse struct {
	TraceID       string           `json:"traceId"`
	Revision      uint64           `json:"revision"`
	Spans         []normalizedSpan `json:"spans"`
	ResourceSpans json.RawMessage  `json:"resourceSpans"`
	Diagnostics   []Diagnostic     `json:"diagnostics"`
}

type serverHandler struct {
	maxBodyBytes int64
	store        *store
}

func NewServer(addr string, config Config) (*http.Server, error) {
	if addr == "" {
		return nil, fmt.Errorf("Spaniel listener address %q is empty", addr)
	}
	if config.DatabasePath == "" {
		config.DatabasePath = DefaultDatabasePath()
	}
	if config.MaxBodyBytes < 0 {
		return nil, fmt.Errorf("Spaniel max body bytes %d is negative", config.MaxBodyBytes)
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = defaultMaxBodyBytes
	}

	traceStore, err := newStore(config.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("open Spaniel database %q: %w", config.DatabasePath, err)
	}
	handler := &serverHandler{maxBodyBytes: config.MaxBodyBytes, store: traceStore}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", handler.ingest)
	mux.HandleFunc("POST /v1/metrics", handler.ingestMetrics)
	mux.HandleFunc("POST /api/v1/traces/query", handler.query)
	mux.HandleFunc("GET /api/v1/traces/{traceId}", handler.snapshot)
	mux.HandleFunc("GET /api/v1/diagnostics", handler.getDiagnostics)
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: readHeaderTimeout}
	server.RegisterOnShutdown(traceStore.close)
	return server, nil
}

func (handler *serverHandler) ingestMetrics(response http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, handler.maxBodyBytes))
	if err != nil {
		http.Error(response, "read OTLP metric body: "+err.Error(), http.StatusBadRequest)
		return
	}
	exportRequest := pmetricotlp.NewExportRequest()
	contentType := contentType(request)
	switch contentType {
	case "application/json":
		err = exportRequest.UnmarshalJSON(body)
	case "application/x-protobuf", "application/protobuf":
		err = exportRequest.UnmarshalProto(body)
	default:
		http.Error(response, "unsupported OTLP Content-Type "+strconv.Quote(contentType), http.StatusUnsupportedMediaType)
		return
	}
	if err != nil {
		http.Error(response, "decode OTLP metric body: "+err.Error(), http.StatusBadRequest)
		return
	}

	exportResponse := pmetricotlp.NewExportResponse()
	encoded, err := marshalMetricResponse(exportResponse, contentType, response)
	if err != nil {
		http.Error(response, "encode OTLP metric response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write(encoded)
}

func marshalMetricResponse(exportResponse pmetricotlp.ExportResponse, contentType string, response http.ResponseWriter) ([]byte, error) {
	if contentType == "application/json" {
		response.Header().Set("Content-Type", "application/json")
		return exportResponse.MarshalJSON()
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	return exportResponse.MarshalProto()
}

func (handler *serverHandler) ingest(response http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, handler.maxBodyBytes))
	if err != nil {
		http.Error(response, "read OTLP trace body: "+err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	exportRequest := ptraceotlp.NewExportRequest()
	requestContentType := contentType(request)
	switch requestContentType {
	case "application/json":
		err = exportRequest.UnmarshalJSON(body)
	case "application/x-protobuf", "application/protobuf":
		err = exportRequest.UnmarshalProto(body)
	default:
		http.Error(response, "unsupported OTLP Content-Type "+strconv.Quote(requestContentType), http.StatusUnsupportedMediaType)
		return
	}
	if err != nil {
		http.Error(response, "decode OTLP trace body: "+err.Error(), http.StatusBadRequest)
		return
	}

	rejected, messages, err := handler.store.commit(request.Context(), exportRequest.Traces())
	if err != nil {
		http.Error(response, "store OTLP trace body: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	exportResponse := ptraceotlp.NewExportResponse()
	if rejected > 0 {
		partial := exportResponse.PartialSuccess()
		partial.SetRejectedSpans(int64(rejected))
		partial.SetErrorMessage(strings.Join(messages, "; "))
	}
	encoded, err := marshalTraceResponse(exportResponse, requestContentType, response)
	if err != nil {
		http.Error(response, "encode OTLP trace response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write(encoded)
}

func marshalTraceResponse(exportResponse ptraceotlp.ExportResponse, contentType string, response http.ResponseWriter) ([]byte, error) {
	if contentType == "application/json" {
		response.Header().Set("Content-Type", "application/json")
		return exportResponse.MarshalJSON()
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	return exportResponse.MarshalProto()
}

func contentType(request *http.Request) string {
	return strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0])
}

func (handler *serverHandler) query(response http.ResponseWriter, request *http.Request) {
	var query traceQuery
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, handler.maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&query); err != nil {
		http.Error(response, "decode trace query: "+err.Error(), http.StatusBadRequest)
		return
	}
	traceID, err := canonicalTraceID(query.TraceID)
	if err != nil {
		handler.store.addDiagnostic(Diagnostic{Kind: "malformed_trace_id", Message: err.Error()})
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	if len(query.RequiredSpans) == 0 {
		http.Error(response, "requiredSpans must contain at least one selector", http.StatusBadRequest)
		return
	}
	for index, required := range query.RequiredSpans {
		if required.ServiceName == "" || required.Name == "" || required.MinCount < 1 {
			http.Error(response, fmt.Sprintf("requiredSpans[%d] must have non-empty serviceName/name and minCount >= 1", index), http.StatusBadRequest)
			return
		}
	}
	allowedExternalParents := make(map[string]bool, len(query.AllowedExternalParentSpanIDs))
	for index, parentID := range query.AllowedExternalParentSpanIDs {
		canonical, canonicalErr := canonicalSpanID(parentID)
		if canonicalErr != nil {
			http.Error(response, fmt.Sprintf("allowedExternalParentSpanIds[%d]: %v", index, canonicalErr), http.StatusBadRequest)
			return
		}
		allowedExternalParents[canonical] = true
	}
	queryTimeout := defaultQueryTimeout
	if query.TimeoutMilliseconds < 0 {
		http.Error(response, "timeoutMs must be positive", http.StatusBadRequest)
		return
	}
	if query.TimeoutMilliseconds > 0 {
		queryTimeout = time.Duration(query.TimeoutMilliseconds) * time.Millisecond
	}
	queryContext, cancel := context.WithTimeout(request.Context(), queryTimeout)
	defer cancel()

	result, status, err := handler.store.wait(queryContext, traceID, query.RequiredSpans, allowedExternalParents)
	if err != nil {
		http.Error(response, err.Error(), status)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (handler *serverHandler) snapshot(response http.ResponseWriter, request *http.Request) {
	traceID, err := canonicalTraceID(request.PathValue("traceId"))
	if err != nil {
		handler.store.addDiagnostic(Diagnostic{Kind: "malformed_trace_id", Message: err.Error()})
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	result, found, err := handler.store.snapshot(request.Context(), traceID)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(response, "trace not found", http.StatusNotFound)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (handler *serverHandler) getDiagnostics(response http.ResponseWriter, request *http.Request) {
	after := int64(0)
	if value := request.URL.Query().Get("after"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			http.Error(response, "after must be a non-negative integer", http.StatusBadRequest)
			return
		}
		after = parsed
	}
	diagnostics, revision, err := handler.store.allDiagnostics(request.Context(), after)
	if err != nil {
		http.Error(response, "read diagnostics: "+err.Error(), http.StatusInternalServerError)
		return
	}
	status := http.StatusOK
	if len(diagnostics) > 0 {
		status = http.StatusConflict
	}
	writeJSON(response, status, struct {
		Revision    int64        `json:"revision"`
		Diagnostics []Diagnostic `json:"diagnostics"`
	}{Revision: revision, Diagnostics: diagnostics})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func canonicalTraceID(value string) (string, error) {
	if len(value) != traceIDLength {
		return "", fmt.Errorf("traceId %q must be 32 hexadecimal characters", value)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || bytes.Equal(decoded, make([]byte, len(decoded))) {
		return "", fmt.Errorf("traceId %q must be a nonzero 32-character hexadecimal ID", value)
	}
	return strings.ToLower(value), nil
}

func canonicalSpanID(value string) (string, error) {
	if len(value) != 16 {
		return "", fmt.Errorf("span ID %q must be 16 hexadecimal characters", value)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || bytes.Equal(decoded, make([]byte, len(decoded))) {
		return "", fmt.Errorf("span ID %q must be a nonzero 16-character hexadecimal ID", value)
	}
	return strings.ToLower(value), nil
}
