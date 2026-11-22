package spaniel

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	// Past the limit, eviction trims to half of it, so cleanup runs rarely
	// and in bulk rather than on every write near the limit.
	evictionTargetPercent = 50
	// One small transaction per writer turn keeps ingestion interleaved with
	// cleanup; WAL keeps queries reading throughout.
	evictionBatchTraces = 64
	// Before serving nothing waits on the writer, so launch evicts in bulk.
	launchEvictionBatchTraces = 4096
)

// usedBytes counts pages holding data; freed pages are reused before the file
// grows, so the file stays near the limit without vacuuming at runtime.
// It returns the bytes in use and the file's page bytes.
func (traceStore *store) usedBytes(ctx context.Context) (int64, int64, error) {
	var pageSize, pageCount, freePages int64
	for _, pragma := range []struct {
		name  string
		value *int64
	}{{"page_size", &pageSize}, {"page_count", &pageCount}, {"freelist_count", &freePages}} {
		if err := traceStore.db.QueryRowContext(ctx, "PRAGMA "+pragma.name).Scan(pragma.value); err != nil {
			return 0, 0, fmt.Errorf("read SQLite %s: %w", pragma.name, err)
		}
	}
	return (pageCount - freePages) * pageSize, pageCount * pageSize, nil
}

func (traceStore *store) evictionTarget() int64 {
	return traceStore.maxStorageBytes / 100 * evictionTargetPercent
}

// storageLimits bounds the database and sets how often its size is checked.
type storageLimits struct {
	MaxBytes      int64
	CheckInterval time.Duration
}

// evictLoop checks the size on a timer rather than per write. When over the
// limit it trims one writer turn at a time, so ingestion queued meanwhile
// commits between eviction batches.
func (traceStore *store) evictLoop(ctx context.Context) {
	defer close(traceStore.evictorDone)
	ticker := time.NewTicker(traceStore.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		used, _, err := traceStore.usedBytes(ctx)
		if err != nil || used <= traceStore.maxStorageBytes {
			continue
		}
		for {
			result, err := traceStore.write(ctx, func() writeResult {
				evicted, err := traceStore.evictBatchAboveTarget(ctx, evictionBatchTraces)
				return writeResult{evicted: evicted, err: err}
			})
			if err == nil {
				err = result.err
			}
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("spaniel: evict oldest traces: %v", err)
				}
				break
			}
			if result.evicted == 0 {
				break
			}
		}
	}
}

// evictBatchAboveTarget deletes just enough of the oldest whole traces to
// cover the bytes above the eviction target, at most batchTraces per
// transaction. It returns zero once the database is at or below the target.
func (traceStore *store) evictBatchAboveTarget(ctx context.Context, batchTraces int) (int64, error) {
	used, _, err := traceStore.usedBytes(ctx)
	if err != nil || used <= traceStore.evictionTarget() {
		return 0, err
	}
	transaction, err := traceStore.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = transaction.Rollback() }()
	var victims string
	if err := transaction.QueryRowContext(ctx, `WITH oldest AS (
			SELECT traces.rowid AS position, traces.trace_id,
				(SELECT coalesce(sum(length(payload)), 0) FROM spans WHERE spans.trace_id = traces.trace_id) AS bytes
			FROM traces ORDER BY traces.rowid LIMIT ?
		), running AS (
			SELECT trace_id, sum(bytes) OVER (ORDER BY position) - bytes AS freed_before FROM oldest
		)
		SELECT coalesce(json_group_array(trace_id), '[]') FROM running WHERE freed_before < ?`,
		batchTraces, used-traceStore.evictionTarget(),
	).Scan(&victims); err != nil {
		return 0, fmt.Errorf("select oldest traces: %w", err)
	}
	const selected = "SELECT value FROM json_each(?)"
	for _, statement := range []string{
		"DELETE FROM spans WHERE trace_id IN (" + selected + ")",
		"DELETE FROM diagnostics WHERE trace_id IN (" + selected + ")",
	} {
		if _, err := transaction.ExecContext(ctx, statement, victims); err != nil {
			return 0, fmt.Errorf("evict oldest traces: %w", err)
		}
	}
	result, err := transaction.ExecContext(ctx, "DELETE FROM traces WHERE trace_id IN ("+selected+")", victims)
	if err != nil {
		return 0, fmt.Errorf("evict oldest traces: %w", err)
	}
	evicted, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return evicted, transaction.Commit()
}

// compactOversized applies a lowered limit on launch, before serving: trim to
// the eviction target, then rewrite the file so freed space returns to the host.
func (traceStore *store) compactOversized(ctx context.Context) error {
	used, file, err := traceStore.usedBytes(ctx)
	if err != nil || (used <= traceStore.maxStorageBytes && file <= traceStore.maxStorageBytes) {
		return err
	}
	for {
		evicted, err := traceStore.evictBatchAboveTarget(ctx, launchEvictionBatchTraces)
		if err != nil {
			return err
		}
		if evicted == 0 {
			break
		}
	}
	for _, statement := range []string{"VACUUM", "PRAGMA wal_checkpoint(TRUNCATE)"} {
		if _, err := traceStore.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("compact SQLite with %q: %w", statement, err)
		}
	}
	return nil
}
