# Spaniel

`spaniel` is a local OpenTelemetry trace receiver and SQLite trace store for
deterministic end-to-end tests.

Spaniel accepts OTLP/HTTP trace exports, writes spans to SQLite through one Go
channel, and serves concurrent reads plus event-driven blocking queries. It
does not poll: committed ingestion rotates a per-trace notification channel,
waking only queries for the changed trace. WAL mode permits trace CLI reads
while the receiver writes.

## Query semantics

`POST /api/v1/traces/query` waits until every `requiredSpans` selector reaches
its `minCount` and the trace graph is valid. Required selectors guarantee only
the requested spans' presence; graph integrity is validated separately by the
Spaniel before it returns the trace.

An unresolved internal parent can arrive in a later concurrent export, so the
query remains parked until that parent arrives or the query times out. A known
remote parent must be declared explicitly with
`allowedExternalParentSpanIds`. Spaniel never treats arbitrary unresolved
parents as external.

Example request:

```json
{
  "traceId": "0123456789abcdef0123456789abcdef",
  "requiredSpans": [
    {
      "serviceName": "listenbox-api",
      "name": "POST /session/shows",
      "minCount": 1
    }
  ],
  "allowedExternalParentSpanIds": ["0123456789abcdef"],
  "timeoutMs": 30000
}
```

A successful response includes deterministic normalized `spans` for concise
assertions and canonical OTLP JSON `resourceSpans` for detailed assertions.
Tests must still assert expected operation-specific parent/child edges; generic
graph validity does not establish the intended edge.

If the required spans are present but a non-allowed parent is still missing at
timeout, the query returns a controlled error naming the missing parent and
child span IDs and records an `unresolved_parent` diagnostic.

## Validation and diagnostics

During ingestion, Spaniel validates:

- nonzero, valid trace and span IDs;
- canonical trace ownership for every stored span;
- unique span IDs within each trace, distinguishing identical duplicates from
  conflicting payloads;
- a nonempty `service.name`;
- dropped attribute, event, and link counters.

Before completing a blocking query, it rejects self-parenting, parent cycles,
and unresolved non-allowed parents. Parent IDs remain present in both normalized
spans and canonical `resourceSpans`.

`GET /api/v1/diagnostics` returns `200` when clean and `409` when any diagnostic
has been recorded. E2E harnesses check this endpoint after their tests, so
duplicate or conflicting span IDs, dropped data, invalid IDs, missing service
names, unresolved parents, self-parenting, or cycles fail the suite.

Diagnostics are durable and bounded globally and per trace. The diagnostics
endpoint returns a revision; E2E harnesses query after their startup revision
so earlier development diagnostics remain intact without affecting a suite.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/traces` | Receive OTLP/HTTP JSON or protobuf traces. |
| `POST` | `/v1/metrics` | Accept and discard OTLP/HTTP metrics. |
| `POST` | `/api/v1/traces/query` | Block until selectors and graph validation succeed. |
| `GET` | `/api/v1/traces/{traceId}` | Return the current trace snapshot without waiting. |
| `GET` | `/api/v1/diagnostics` | Return accumulated validation diagnostics. |
| `GET` | `/healthz` | Report process readiness. |

## Trace CLI

`spaniel` reads SQLite directly while `spaniel-server` runs. `gcx` returns the
raw Tempo-compatible OpenTelemetry envelope with `batches`; `jaeger` returns
the legacy query envelope for consumers expecting that schema.

```sh
go run ./cmd/spaniel \
  -db /path/to/spaniel.sqlite \
  -format gcx \
  0123456789abcdef0123456789abcdef
```

## Install

Spaniel requires Go 1.26 or newer.

```sh
go install github.com/meoyawn/spaniel/cmd/spaniel@latest
go install github.com/meoyawn/spaniel/cmd/spaniel-server@latest
```

## Run

```sh
spaniel-server -addr 127.0.0.1:4318 -db /path/to/spaniel.sqlite
```

Point an OTLP/HTTP exporter at `http://127.0.0.1:4318`. Spaniel accepts JSON
and protobuf trace payloads.

## Develop

Run the complete check from the repository root:

```sh
task check
```

`NewServer` requires an explicit SQLite path and accepts a test-specific body
size limit. A zero body-size value selects the bounded default.

## License

[MIT](LICENSE)
