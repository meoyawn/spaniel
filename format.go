// Package spaniel receives, stores, validates, and renders OpenTelemetry traces.
package spaniel

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	outputFormatGCXValue    = "gcx"
	outputFormatJaegerValue = "jaeger"
)

// OutputFormat selects the JSON representation returned by a trace lookup.
type OutputFormat struct {
	value string
}

// NewOutputFormatGCX returns the GCX trace output format.
func NewOutputFormatGCX() OutputFormat {
	return OutputFormat{value: outputFormatGCXValue}
}

// NewOutputFormatJaeger returns the Jaeger trace output format.
func NewOutputFormatJaeger() OutputFormat {
	return OutputFormat{value: outputFormatJaegerValue}
}

// NewOutputFormatFromValue parses a supported trace output format name.
func NewOutputFormatFromValue(value string) (OutputFormat, error) {
	switch value {
	case outputFormatGCXValue:
		return NewOutputFormatGCX(), nil
	case outputFormatJaegerValue:
		return NewOutputFormatJaeger(), nil
	default:
		return OutputFormat{}, fmt.Errorf("trace output format %q must be gcx or jaeger", value)
	}
}

func (format OutputFormat) String() string {
	return format.value
}

func marshalTraceJSON(traceID string, format OutputFormat, record *traceRecord) ([]byte, error) {
	switch format.value {
	case outputFormatGCXValue:
		return marshalGCXTrace(traceID, record)
	case outputFormatJaegerValue:
		return marshalJaegerTrace(traceID, record)
	default:
		return nil, fmt.Errorf("trace output format %q must be gcx or jaeger", format.value)
	}
}

func marshalGCXTrace(traceID string, record *traceRecord) ([]byte, error) {
	snapshot, err := snapshotRecord(traceID, record)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Batches json.RawMessage `json:"batches"`
	}{Batches: snapshot.ResourceSpans})
}

type jaegerTraceResponse struct {
	Data   []jaegerTrace `json:"data"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
	Errors any           `json:"errors"`
}

type jaegerTrace struct {
	TraceID   string                   `json:"traceID"`
	Spans     []jaegerSpan             `json:"spans"`
	Processes map[string]jaegerProcess `json:"processes"`
	Warnings  any                      `json:"warnings"`
}

type jaegerSpan struct {
	TraceID       string            `json:"traceID"`
	SpanID        string            `json:"spanID"`
	OperationName string            `json:"operationName"`
	References    []jaegerReference `json:"references"`
	StartTime     uint64            `json:"startTime"`
	Duration      uint64            `json:"duration"`
	Tags          []jaegerTag       `json:"tags"`
	Logs          []jaegerLog       `json:"logs"`
	ProcessID     string            `json:"processID"`
	Warnings      any               `json:"warnings"`
}

type jaegerReference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

type jaegerTag struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

type jaegerLog struct {
	Timestamp uint64      `json:"timestamp"`
	Fields    []jaegerTag `json:"fields"`
}

type jaegerProcess struct {
	ServiceName string      `json:"serviceName"`
	Tags        []jaegerTag `json:"tags"`
}

func marshalJaegerTrace(traceID string, record *traceRecord) ([]byte, error) {
	trace := jaegerTrace{TraceID: traceID, Processes: make(map[string]jaegerProcess)}
	processIDs := make(map[string]string)
	spans := make([]storedSpan, 0, len(record.spans))
	for _, span := range record.spans {
		spans = append(spans, span)
	}
	sort.Slice(spans, func(left int, right int) bool { return spans[left].normalized.SpanID < spans[right].normalized.SpanID })
	for _, stored := range spans {
		decoded, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(stored.payload)
		if err != nil {
			return nil, fmt.Errorf("decode trace %s span %s: %w", traceID, stored.normalized.SpanID, err)
		}
		resourceSpan := decoded.ResourceSpans().At(0)
		scopeSpan := resourceSpan.ScopeSpans().At(0)
		span := scopeSpan.Spans().At(0)
		processKeyBytes, err := json.Marshal(resourceSpan.Resource().Attributes().AsRaw())
		if err != nil {
			return nil, fmt.Errorf("marshal trace %s process: %w", traceID, err)
		}
		processKey := string(processKeyBytes)
		processID := processIDs[processKey]
		if processID == "" {
			processID = fmt.Sprintf("p%d", len(processIDs)+1)
			processIDs[processKey] = processID
			trace.Processes[processID] = jaegerProcess{
				ServiceName: resourceServiceName(resourceSpan.Resource()),
				Tags:        jaegerTags(resourceSpan.Resource().Attributes(), "service.name"),
			}
		}
		converted := jaegerSpan{
			TraceID:       traceID,
			SpanID:        span.SpanID().String(),
			OperationName: span.Name(),
			StartTime:     uint64(span.StartTimestamp()) / uint64(time.Microsecond),
			Duration:      uint64(span.EndTimestamp()-span.StartTimestamp()) / uint64(time.Microsecond),
			Tags:          jaegerTags(span.Attributes(), ""),
			ProcessID:     processID,
		}
		if !span.ParentSpanID().IsEmpty() {
			converted.References = append(converted.References, jaegerReference{RefType: "CHILD_OF", TraceID: traceID, SpanID: span.ParentSpanID().String()})
		}
		links := span.Links()
		for index := range links.Len() {
			link := links.At(index)
			converted.References = append(converted.References, jaegerReference{RefType: "FOLLOWS_FROM", TraceID: link.TraceID().String(), SpanID: link.SpanID().String()})
		}
		converted.Tags = append(converted.Tags,
			jaegerTag{Key: "span.kind", Type: "string", Value: span.Kind().String()},
			jaegerTag{Key: "otel.status_code", Type: "string", Value: span.Status().Code().String()},
		)
		if span.Status().Message() != "" {
			converted.Tags = append(converted.Tags, jaegerTag{Key: "otel.status_description", Type: "string", Value: span.Status().Message()})
		}
		if scopeSpan.Scope().Name() != "" {
			converted.Tags = append(converted.Tags, jaegerTag{Key: "otel.scope.name", Type: "string", Value: scopeSpan.Scope().Name()})
		}
		if scopeSpan.Scope().Version() != "" {
			converted.Tags = append(converted.Tags, jaegerTag{Key: "otel.scope.version", Type: "string", Value: scopeSpan.Scope().Version()})
		}
		events := span.Events()
		for index := range events.Len() {
			event := events.At(index)
			fields := jaegerTags(event.Attributes(), "")
			fields = append(fields, jaegerTag{Key: "event", Type: "string", Value: event.Name()})
			converted.Logs = append(converted.Logs, jaegerLog{Timestamp: uint64(event.Timestamp()) / uint64(time.Microsecond), Fields: fields})
		}
		sort.Slice(converted.Tags, func(left int, right int) bool { return converted.Tags[left].Key < converted.Tags[right].Key })
		trace.Spans = append(trace.Spans, converted)
	}
	return json.Marshal(jaegerTraceResponse{Data: []jaegerTrace{trace}, Total: 1, Limit: 0, Offset: 0})
}

func jaegerTags(attributes pcommon.Map, excluded string) []jaegerTag {
	tags := make([]jaegerTag, 0, attributes.Len())
	attributes.Range(func(key string, value pcommon.Value) bool {
		if key != excluded {
			valueType, raw := jaegerValue(value)
			tags = append(tags, jaegerTag{Key: key, Type: valueType, Value: raw})
		}
		return true
	})
	sort.Slice(tags, func(left int, right int) bool { return tags[left].Key < tags[right].Key })
	return tags
}

func jaegerValue(value pcommon.Value) (string, any) {
	switch value.Type() {
	case pcommon.ValueTypeBool:
		return "bool", value.Bool()
	case pcommon.ValueTypeInt:
		return "int64", value.Int()
	case pcommon.ValueTypeDouble:
		return "float64", value.Double()
	case pcommon.ValueTypeBytes:
		return "binary", value.Bytes().AsRaw()
	case pcommon.ValueTypeStr:
		return "string", value.Str()
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		encoded, err := json.Marshal(value.AsRaw())
		if err != nil {
			return "string", fmt.Sprintf("<invalid JSON value: %v>", err)
		}
		return "string", string(encoded)
	case pcommon.ValueTypeEmpty:
		return "string", ""
	}
	return "string", fmt.Sprint(value.AsRaw())
}
