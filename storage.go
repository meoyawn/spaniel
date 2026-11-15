package spaniel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	_ "modernc.org/sqlite" // Register the SQLite database driver.
)

const (
	defaultMaxDiagnostics  = 4096
	maxDiagnosticsPerTrace = 256
	databaseDirectoryMode  = 0o700
	databaseMaxConnections = 16
)

type storedSpan struct {
	fingerprint [sha256.Size]byte
	normalized  normalizedSpan
	payload     []byte
}

type spanSelector struct {
	name        string
	serviceName string
}

type traceRecord struct {
	counts      map[spanSelector]int
	diagnostics []Diagnostic
	revision    uint64
	spans       map[string]storedSpan
}

type writeRequest struct {
	run    func() writeResult
	result chan writeResult
}

type writeResult struct {
	rejected int
	messages []string
	err      error
}

type store struct {
	db            *sql.DB
	writes        chan writeRequest
	done          chan struct{}
	closeOnce     sync.Once
	notifications map[string]chan struct{}
	notifyMu      sync.Mutex
}

func newStore(ctx context.Context, databasePath string) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(databasePath), databaseDirectoryMode); err != nil {
		return nil, fmt.Errorf("create SQLite directory for %q: %w", databasePath, err)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(databaseMaxConnections)
	for _, statement := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		`CREATE TABLE IF NOT EXISTS traces (
			trace_id TEXT PRIMARY KEY,
			revision INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS spans (
			trace_id TEXT NOT NULL REFERENCES traces(trace_id) ON DELETE CASCADE,
			span_id TEXT NOT NULL,
			parent_span_id TEXT NOT NULL,
			name TEXT NOT NULL,
			service_name TEXT NOT NULL,
			fingerprint BLOB NOT NULL,
			payload BLOB NOT NULL,
			PRIMARY KEY (trace_id, span_id)
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS diagnostics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			message TEXT NOT NULL,
			trace_id TEXT NOT NULL,
			span_id TEXT NOT NULL
		) STRICT`,
		"CREATE INDEX IF NOT EXISTS diagnostics_trace_id_id ON diagnostics(trace_id, id)",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("initialize SQLite with %q: %w", statement, err)
		}
	}
	result := &store{
		db:            database,
		writes:        make(chan writeRequest),
		done:          make(chan struct{}),
		notifications: make(map[string]chan struct{}),
	}
	go result.writeLoop()
	return result, nil
}

func (traceStore *store) close() {
	traceStore.closeOnce.Do(func() {
		close(traceStore.writes)
		<-traceStore.done
		_ = traceStore.db.Close()
	})
}

func (traceStore *store) writeLoop() {
	defer close(traceStore.done)
	for request := range traceStore.writes {
		request.result <- request.run()
	}
}

func (traceStore *store) write(ctx context.Context, run func() writeResult) (writeResult, error) {
	request := writeRequest{run: run, result: make(chan writeResult, 1)}
	select {
	case traceStore.writes <- request:
	case <-ctx.Done():
		return writeResult{}, ctx.Err()
	}
	select {
	case result := <-request.result:
		return result, nil
	case <-ctx.Done():
		return writeResult{}, ctx.Err()
	}
}

func (traceStore *store) commit(ctx context.Context, traces ptrace.Traces) (int, []string, error) {
	result, err := traceStore.write(ctx, func() writeResult {
		rejected, messages, commitErr := traceStore.commitNow(ctx, traces)
		return writeResult{rejected: rejected, messages: messages, err: commitErr}
	})
	if err != nil {
		return 0, nil, err
	}
	return result.rejected, result.messages, result.err
}

type traceBatch struct {
	transaction   *sql.Tx
	changedTraces map[string]bool
	rejected      int
	messages      []string
}

func (traceStore *store) commitNow(ctx context.Context, traces ptrace.Traces) (int, []string, error) {
	transaction, err := traceStore.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = transaction.Rollback() }()
	batch := traceBatch{transaction: transaction, changedTraces: make(map[string]bool)}
	resources := traces.ResourceSpans()
	for index := range resources.Len() {
		if err := batch.commitResource(ctx, resources.At(index)); err != nil {
			return 0, nil, err
		}
	}
	if err := trimDiagnosticsTx(ctx, transaction); err != nil {
		return 0, nil, err
	}
	if err := transaction.Commit(); err != nil {
		return 0, nil, err
	}
	for traceID := range batch.changedTraces {
		traceStore.notify(traceID)
	}
	return batch.rejected, batch.messages, nil
}

func (batch *traceBatch) commitResource(ctx context.Context, resourceSpan ptrace.ResourceSpans) error {
	serviceName := resourceServiceName(resourceSpan.Resource())
	scopes := resourceSpan.ScopeSpans()
	for scopeIndex := range scopes.Len() {
		scope := scopes.At(scopeIndex)
		spans := scope.Spans()
		for spanIndex := range spans.Len() {
			if err := batch.commitSpan(ctx, resourceSpan, scope, spans.At(spanIndex), serviceName); err != nil {
				return err
			}
		}
	}
	return nil
}

func (batch *traceBatch) commitSpan(
	ctx context.Context,
	resourceSpan ptrace.ResourceSpans,
	scopeSpan ptrace.ScopeSpans,
	span ptrace.Span,
	serviceName string,
) error {
	traceID := span.TraceID().String()
	spanID := span.SpanID().String()
	if span.TraceID().IsEmpty() || span.SpanID().IsEmpty() {
		diagnostic := Diagnostic{
			Kind: "rejected_span", Message: "span has empty trace or span ID", TraceID: traceID, SpanID: spanID,
		}
		if err := insertDiagnosticTx(ctx, batch.transaction, diagnostic); err != nil {
			return err
		}
		batch.rejected++
		batch.messages = append(batch.messages, diagnostic.Message)
		return nil
	}
	encoded, err := (&ptrace.JSONMarshaler{}).MarshalTraces(singleSpanPayload(resourceSpan, scopeSpan, span))
	if err != nil {
		return fmt.Errorf("marshal stored span %s/%s: %w", traceID, spanID, err)
	}
	stored := storedSpan{
		fingerprint: sha256.Sum256(encoded), payload: encoded,
		normalized: normalizedSpan{
			TraceID: traceID, SpanID: spanID, ParentSpanID: parentSpanID(span), Name: span.Name(), ServiceName: serviceName,
		},
	}
	changed, err := insertSpan(ctx, batch.transaction, stored)
	if err != nil || !changed {
		return err
	}
	batch.changedTraces[traceID] = true
	for _, diagnostic := range counterDiagnostics(resourceSpan, scopeSpan, span, traceID, spanID, serviceName) {
		if err := insertDiagnosticTx(ctx, batch.transaction, diagnostic); err != nil {
			return err
		}
	}
	return nil
}

func insertSpan(ctx context.Context, transaction *sql.Tx, span storedSpan) (bool, error) {
	traceID, spanID := span.normalized.TraceID, span.normalized.SpanID
	var existing []byte
	err := transaction.QueryRowContext(ctx,
		"SELECT fingerprint FROM spans WHERE trace_id = ? AND span_id = ?", traceID, spanID,
	).Scan(&existing)
	if err == nil {
		diagnostic := Diagnostic{Kind: "duplicate_span_id", Message: "duplicate span ID", TraceID: traceID, SpanID: spanID}
		if !bytes.Equal(existing, span.fingerprint[:]) {
			diagnostic.Kind, diagnostic.Message = "conflicting_span_id", "conflicting span ID"
		}
		return false, insertDiagnosticTx(ctx, transaction, diagnostic)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if _, err := transaction.ExecContext(ctx,
		`INSERT INTO traces(trace_id, revision) VALUES (?, 1)
   ON CONFLICT(trace_id) DO UPDATE SET revision = revision + 1`, traceID,
	); err != nil {
		return false, err
	}
	_, err = transaction.ExecContext(ctx,
		`INSERT INTO spans(trace_id, span_id, parent_span_id, name, service_name, fingerprint, payload)
   VALUES (?, ?, ?, ?, ?, ?, ?)`,
		traceID, spanID, span.normalized.ParentSpanID, span.normalized.Name, span.normalized.ServiceName,
		span.fingerprint[:], span.payload,
	)
	return err == nil, err
}

func parentSpanID(span ptrace.Span) string {
	if span.ParentSpanID().IsEmpty() {
		return ""
	}
	return span.ParentSpanID().String()
}

func singleSpanPayload(
	resourceSpan ptrace.ResourceSpans,
	scopeSpan ptrace.ScopeSpans,
	span ptrace.Span,
) ptrace.Traces {
	result := ptrace.NewTraces()
	resultResource := result.ResourceSpans().AppendEmpty()
	resourceSpan.Resource().CopyTo(resultResource.Resource())
	resultResource.SetSchemaUrl(resourceSpan.SchemaUrl())
	resultScope := resultResource.ScopeSpans().AppendEmpty()
	scopeSpan.Scope().CopyTo(resultScope.Scope())
	resultScope.SetSchemaUrl(scopeSpan.SchemaUrl())
	span.CopyTo(resultScope.Spans().AppendEmpty())
	return result
}

func resourceServiceName(resource pcommon.Resource) string {
	value, ok := resource.Attributes().Get("service.name")
	if !ok || value.Type() != pcommon.ValueTypeStr {
		return ""
	}
	return value.Str()
}

func counterDiagnostics(
	resourceSpan ptrace.ResourceSpans,
	scopeSpan ptrace.ScopeSpans,
	span ptrace.Span,
	traceID string,
	spanID string,
	serviceName string,
) []Diagnostic {
	var diagnostics []Diagnostic
	if serviceName == "" {
		diagnostics = append(
			diagnostics,
			Diagnostic{
				Kind:    "missing_service_name",
				Message: "resource is missing service.name",
				TraceID: traceID,
				SpanID:  spanID,
			},
		)
	}
	for _, counter := range []struct {
		name  string
		value uint32
	}{
		{name: "resource dropped attributes", value: resourceSpan.Resource().DroppedAttributesCount()},
		{name: "scope dropped attributes", value: scopeSpan.Scope().DroppedAttributesCount()},
		{name: "span dropped attributes", value: span.DroppedAttributesCount()},
		{name: "span dropped events", value: span.DroppedEventsCount()},
		{name: "span dropped links", value: span.DroppedLinksCount()},
	} {
		if counter.value > 0 {
			diagnostics = append(
				diagnostics,
				Diagnostic{
					Kind:    "dropped_data",
					Message: fmt.Sprintf("%s = %d", counter.name, counter.value),
					TraceID: traceID,
					SpanID:  spanID,
				},
			)
		}
	}
	return diagnostics
}

func (traceStore *store) addDiagnostic(ctx context.Context, diagnostic Diagnostic) error {
	result, err := traceStore.write(ctx, func() writeResult {
		return writeResult{err: traceStore.insertDiagnostic(ctx, diagnostic)}
	})
	if err != nil {
		return err
	}
	return result.err
}

func (traceStore *store) insertDiagnostic(ctx context.Context, diagnostic Diagnostic) error {
	transaction, err := traceStore.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = transaction.Rollback() }()
	if err := insertDiagnosticTx(ctx, transaction, diagnostic); err != nil {
		return err
	}
	if err := trimDiagnosticsTx(ctx, transaction); err != nil {
		return err
	}
	return transaction.Commit()
}

func insertDiagnosticTx(ctx context.Context, transaction *sql.Tx, diagnostic Diagnostic) error {
	_, err := transaction.ExecContext(ctx,
		"INSERT INTO diagnostics(kind, message, trace_id, span_id) VALUES (?, ?, ?, ?)",
		diagnostic.Kind, diagnostic.Message, diagnostic.TraceID, diagnostic.SpanID,
	)
	return err
}

func trimDiagnosticsTx(ctx context.Context, transaction *sql.Tx) error {
	_, err := transaction.ExecContext(ctx,
		"DELETE FROM diagnostics WHERE id <= COALESCE((SELECT max(id) - ? FROM diagnostics), 0)",
		defaultMaxDiagnostics,
	)
	return err
}

func (traceStore *store) allDiagnostics(
	ctx context.Context,
	after int64,
) ([]Diagnostic, int64, error) {
	rows, err := traceStore.db.QueryContext(
		ctx,
		"SELECT id, kind, message, trace_id, span_id FROM diagnostics WHERE id > ? ORDER BY id",
		after,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var diagnostics []Diagnostic
	revision := after
	for rows.Next() {
		var diagnostic Diagnostic
		if err := rows.Scan(
			&diagnostic.ID,
			&diagnostic.Kind,
			&diagnostic.Message,
			&diagnostic.TraceID,
			&diagnostic.SpanID,
		); err != nil {
			return nil, 0, err
		}
		diagnostics = append(diagnostics, diagnostic)
		revision = diagnostic.ID
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if len(diagnostics) == 0 {
		if err := traceStore.db.QueryRowContext(ctx, "SELECT COALESCE(max(id), 0) FROM diagnostics").
			Scan(&revision); err != nil {
			return nil, 0, err
		}
	}
	return diagnostics, revision, nil
}

func (traceStore *store) wait(
	ctx context.Context,
	timeout time.Duration,
	traceID string,
	required []requiredSpan,
	allowedExternalParents map[string]bool,
) (queryResponse, int, error) {
	queryContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		notify := traceStore.notification(traceID)
		record, found, err := traceStore.loadRecord(queryContext, traceID)
		if err != nil {
			return queryResponse{}, http.StatusInternalServerError, err
		}
		if found && requirementsMet(record, required) {
			missing, diagnostic := validateGraph(record, allowedExternalParents)
			if diagnostic != nil {
				return traceStore.diagnosticFailure(ctx, *diagnostic)
			}
			if len(missing) == 0 {
				result, snapshotErr := snapshotRecord(traceID, record)
				return result, http.StatusOK, snapshotErr
			}
		}
		select {
		case <-queryContext.Done():
			if diagnostic := expiredQueryDiagnostic(traceID, record, required, allowedExternalParents); diagnostic != nil {
				// The query deadline must not prevent its unresolved-parent diagnostic
				// from being committed. The caller's request context still bounds this write.
				return traceStore.diagnosticFailure(ctx, *diagnostic)
			}
			return queryResponse{}, http.StatusRequestTimeout, fmt.Errorf(
				"wait for required spans in trace %s: %w", traceID, queryContext.Err(),
			)
		case <-notify:
		}
	}
}

func expiredQueryDiagnostic(
	traceID string,
	record *traceRecord,
	required []requiredSpan,
	allowedExternalParents map[string]bool,
) *Diagnostic {
	missing, diagnostic := validateGraph(record, allowedExternalParents)
	if diagnostic != nil {
		return diagnostic
	}
	if len(missing) == 0 || !requirementsMet(record, required) {
		return nil
	}
	return &Diagnostic{
		Kind: "unresolved_parent", TraceID: traceID,
		Message: fmt.Sprintf("trace %s has unresolved parent edges: %s", traceID, strings.Join(missing, ", ")),
	}
}

func (traceStore *store) diagnosticFailure(ctx context.Context, diagnostic Diagnostic) (queryResponse, int, error) {
	if err := traceStore.addDiagnostic(ctx, diagnostic); err != nil {
		return queryResponse{}, http.StatusServiceUnavailable, fmt.Errorf("record %s diagnostic: %w", diagnostic.Kind, err)
	}
	return queryResponse{}, http.StatusConflict, errors.New(diagnostic.Message)
}

func (traceStore *store) notification(traceID string) <-chan struct{} {
	traceStore.notifyMu.Lock()
	defer traceStore.notifyMu.Unlock()
	notify := traceStore.notifications[traceID]
	if notify == nil {
		notify = make(chan struct{})
		traceStore.notifications[traceID] = notify
	}
	return notify
}

func (traceStore *store) notify(traceID string) {
	traceStore.notifyMu.Lock()
	if notify := traceStore.notifications[traceID]; notify != nil {
		close(notify)
	}
	traceStore.notifications[traceID] = make(chan struct{})
	traceStore.notifyMu.Unlock()
}

func (traceStore *store) loadRecord(
	ctx context.Context,
	traceID string,
) (*traceRecord, bool, error) {
	record := &traceRecord{counts: make(map[spanSelector]int), spans: make(map[string]storedSpan)}
	if err := traceStore.db.QueryRowContext(ctx, "SELECT revision FROM traces WHERE trace_id = ?", traceID).
		Scan(&record.revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return record, false, nil
		}
		return nil, false, err
	}
	rows, err := traceStore.db.QueryContext(
		ctx,
		`SELECT span_id, parent_span_id, name, service_name, fingerprint, payload
   FROM spans WHERE trace_id = ? ORDER BY span_id`,
		traceID,
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	for rows.Next() {
		span := storedSpan{normalized: normalizedSpan{TraceID: traceID}}
		var fingerprint []byte
		if err := rows.Scan(
			&span.normalized.SpanID,
			&span.normalized.ParentSpanID,
			&span.normalized.Name,
			&span.normalized.ServiceName,
			&fingerprint,
			&span.payload,
		); err != nil {
			return nil, false, err
		}
		copy(span.fingerprint[:], fingerprint)
		record.spans[span.normalized.SpanID] = span
		record.counts[spanSelector{name: span.normalized.Name, serviceName: span.normalized.ServiceName}]++
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	record.diagnostics, err = traceStore.traceDiagnostics(ctx, traceID)
	return record, true, err
}

func (traceStore *store) traceDiagnostics(ctx context.Context, traceID string) ([]Diagnostic, error) {
	rows, err := traceStore.db.QueryContext(ctx,
		"SELECT id, kind, message, trace_id, span_id FROM diagnostics WHERE trace_id = ? ORDER BY id DESC LIMIT ?",
		traceID, maxDiagnosticsPerTrace,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var diagnostics []Diagnostic
	for rows.Next() {
		var diagnostic Diagnostic
		if err := rows.Scan(
			&diagnostic.ID, &diagnostic.Kind, &diagnostic.Message, &diagnostic.TraceID, &diagnostic.SpanID,
		); err != nil {
			return nil, err
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	return diagnostics, rows.Err()
}

func snapshotRecord(traceID string, record *traceRecord) (queryResponse, error) {
	result := queryResponse{
		TraceID:     traceID,
		Revision:    record.revision,
		Diagnostics: append([]Diagnostic(nil), record.diagnostics...),
	}
	traces := ptrace.NewTraces()
	spans := make([]storedSpan, 0, len(record.spans))
	for _, span := range record.spans {
		spans = append(spans, span)
	}
	sort.Slice(
		spans,
		func(left int, right int) bool { return spans[left].normalized.SpanID < spans[right].normalized.SpanID },
	)
	for _, span := range spans {
		result.Spans = append(result.Spans, span.normalized)
		decoded, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(span.payload)
		if err != nil {
			return queryResponse{}, fmt.Errorf(
				"decode trace %s span %s payload: %w",
				traceID,
				span.normalized.SpanID,
				err,
			)
		}
		decoded.ResourceSpans().At(0).CopyTo(traces.ResourceSpans().AppendEmpty())
	}
	encoded, err := ptraceotlp.NewExportRequestFromTraces(traces).MarshalJSON()
	if err != nil {
		return queryResponse{}, fmt.Errorf("marshal trace %s snapshot: %w", traceID, err)
	}
	var envelope struct {
		ResourceSpans json.RawMessage `json:"resourceSpans"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return queryResponse{}, fmt.Errorf("decode trace %s snapshot envelope: %w", traceID, err)
	}
	if envelope.ResourceSpans == nil {
		envelope.ResourceSpans = json.RawMessage("[]")
	}
	result.ResourceSpans = envelope.ResourceSpans
	return result, nil
}

func validateGraph(
	record *traceRecord,
	allowedExternalParents map[string]bool,
) ([]string, *Diagnostic) {
	parents := make(map[string]string, len(record.spans))
	for spanID, span := range record.spans {
		parentID := span.normalized.ParentSpanID
		parents[spanID] = parentID
		if parentID == spanID {
			return nil, &Diagnostic{
				Kind:    "self_parent",
				Message: fmt.Sprintf("span %s is its own parent", spanID),
				TraceID: span.normalized.TraceID,
				SpanID:  spanID,
			}
		}
	}
	var missing []string
	for spanID, parentID := range parents {
		if parentID == "" || allowedExternalParents[parentID] {
			continue
		}
		if _, ok := parents[parentID]; !ok {
			missing = append(
				missing,
				fmt.Sprintf("child %s -> missing parent %s", spanID, parentID),
			)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return missing, nil
	}
	return nil, validateParentCycles(record, parents, allowedExternalParents)
}

func validateParentCycles(
	record *traceRecord,
	parents map[string]string,
	allowedExternalParents map[string]bool,
) *Diagnostic {
	for start := range parents {
		path := make(map[string]int)
		var chain []string
		current := start
		for current != "" && !allowedExternalParents[current] {
			if offset, ok := path[current]; ok {
				cycle := append(slices.Clone(chain[offset:]), current)
				span := record.spans[start].normalized
				return &Diagnostic{
					Kind:    "parent_cycle",
					Message: "parent cycle: " + strings.Join(cycle, " -> "),
					TraceID: span.TraceID,
					SpanID:  start,
				}
			}
			path[current] = len(chain)
			chain = append(chain, current)
			parentID, ok := parents[current]
			if !ok {
				break
			}
			current = parentID
		}
	}
	return nil
}

func requirementsMet(record *traceRecord, required []requiredSpan) bool {
	for _, item := range required {
		if record.counts[spanSelector{name: item.Name, serviceName: item.ServiceName}] < item.MinCount {
			return false
		}
	}
	return true
}
