package interpolation

import (
	"testing"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"go.uber.org/zap"
)

func TestEngine_Interpolate(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	event := &eventing.MdaiEvent{
		ID:            "test-id",
		Name:          "test-event",
		Timestamp:     timestamp,
		Payload:       `{"user":{"name":"John","age":30},"level":"info","complex":{"nested":{"value":"deep"}}}`,
		Source:        "test-source",
		SourceID:      "source-123",
		CorrelationID: "corr-456",
		HubName:       "test-hub",
	}

	tests := []struct {
		name     string
		input    string
		expected string
		event    *eventing.MdaiEvent
	}{
		{
			name:     "simple event field",
			input:    "ID: ${trigger:id}",
			expected: "ID: test-id",
			event:    event,
		},
		{
			name:     "event name field",
			input:    "Event: ${trigger:name}",
			expected: "Event: test-event",
			event:    event,
		},
		{
			name:     "timestamp field",
			input:    "Time: ${trigger:timestamp}",
			expected: "Time: 2023-01-01T12:00:00Z",
			event:    event,
		},
		{
			name:     "correlation_id field",
			input:    "Correlation_id: ${trigger:correlation_id}",
			expected: "Correlation_id: corr-456",
			event:    event,
		},
		{
			name:     "hub_name field",
			input:    "hub_name: ${trigger:hub_name}",
			expected: "hub_name: test-hub",
			event:    event,
		},
		{
			name:     "source_id field",
			input:    "Source: ${trigger:source_id}",
			expected: "Source: source-123",
			event:    event,
		},
		{
			name:     "payload field with explicit prefix",
			input:    "Level: ${trigger:payload.level}",
			expected: "Level: info",
			event:    event,
		},
		{
			name:     "nested payload field with explicit prefix",
			input:    "User: ${trigger:payload.user.name}",
			expected: "User: John",
			event:    event,
		},
		{
			name:     "deeply nested payload field with explicit prefix",
			input:    "Deep: ${trigger:payload.complex.nested.value}",
			expected: "Deep: deep",
			event:    event,
		},
		{
			name:     "with default value - payload field exists",
			input:    "User: ${trigger:payload.user.name:-Anonymous}",
			expected: "User: John",
			event:    event,
		},
		{
			name:     "with default value - payload field missing",
			input:    "Status: ${trigger:payload.status:-unknown}",
			expected: "Status: unknown",
			event:    event,
		},
		{
			name:     "invalid event field - should fail",
			input:    "Invalid: ${trigger:level:-default}",
			expected: "Invalid: default",
			event:    event,
		},
		{
			name:     "invalid nested event field - should fail",
			input:    "Invalid: ${trigger:user.name:-default}",
			expected: "Invalid: default",
			event:    event,
		},
		{
			name:     "multiple interpolations",
			input:    "Event ${trigger:name} from ${trigger:source} at ${trigger:timestamp}",
			expected: "Event test-event from test-source at 2023-01-01T12:00:00Z",
			event:    event,
		},
		{
			name:     "missing event field no default - returns original",
			input:    "Missing: ${trigger:nonexistent}",
			expected: "Missing: ${trigger:nonexistent}",
			event:    event,
		},
		{
			name:     "missing payload field no default - returns original",
			input:    "Missing: ${trigger:payload.nonexistent}",
			expected: "Missing: ${trigger:payload.nonexistent}",
			event:    event,
		},
		{
			name:     "nil event",
			input:    "Value: ${trigger:id:-default}",
			expected: "Value: default",
			event:    nil,
		},
		{
			name:     "empty payload - payload field with default",
			input:    "Value: ${trigger:payload.user.name:-default}",
			expected: "Value: default",
			event:    &eventing.MdaiEvent{ID: "test"},
		},
		{
			name:     "invalid JSON payload - payload field with default",
			input:    "Value: ${trigger:payload.field:-default}",
			expected: "Value: default",
			event:    &eventing.MdaiEvent{Payload: "{invalid json}"},
		},
		{
			name:     "unsupported scope",
			input:    "Value: ${env:PATH}",
			expected: "Value: ${env:PATH}",
			event:    event,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, tt.event)
			if result != tt.expected {
				t.Errorf("Interpolate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEngine_ComplexPayloadValues(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID: "test-id",
		Payload: `{
            "string_val": "hello",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": true,
            "object_val": {"nested": "value"},
            "array_val": [1, 2, 3]
        }`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"string value", "${trigger:payload.string_val}", "hello"},
		{"integer value", "${trigger:payload.int_val}", "42"},
		{"float value", "${trigger:payload.float_val}", "3.14"},
		{"boolean value", "${trigger:payload.bool_val}", "true"},
		{"object value (JSON marshaled)", "${trigger:payload.object_val}", `{"nested":"value"}`},
		{"array value (JSON marshaled)", "${trigger:payload.array_val}", `[1,2,3]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			if result != tt.expected {
				t.Errorf("Interpolate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEngine_EventFieldsWithEmptyValues(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:            "test-id",
		Name:          "",
		Source:        "test-source",
		SourceID:      "",
		CorrelationID: "",
		HubName:       "test-hub",
		Payload:       `{"level":"info"}`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty name field with default", "${trigger:name:-default-name}", "default-name"},
		{"empty source_id with default", "${trigger:source_id:-no-source}", "no-source"},
		{"zero timestamp with default", "${trigger:timestamp:-no-time}", "no-time"},
		{"non-empty field", "${trigger:id}", "test-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			if result != tt.expected {
				t.Errorf("Interpolate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEngine_ErrorLogging(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Payload: `{"existing":"value"}`,
	}

	tests := []struct {
		name          string
		input         string
		expectedError bool
	}{
		{"missing event field without default", "${trigger:nonexistent}", true},
		{"missing payload field without default", "${trigger:payload.nonexistent}", true},
		{"unsupported scope", "${env:PATH}", true},
		{"valid field", "${trigger:id}", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			if tt.expectedError {
				if result != tt.input {
					t.Errorf("Expected original input %v, got %v", tt.input, result)
				}
			}
		})
	}
}

func TestEngine_NestedPayloadMissingPath(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Payload: `{"user":{"name":"John"},"level":"info"}`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"missing nested payload path with default", "${trigger:payload.user.age:-unknown}", "unknown"},
		{"missing root payload path with default", "${trigger:payload.config.setting:-default}", "default"},
		{"path through non-object in payload with default", "${trigger:payload.level.subfield:-default}", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			if result != tt.expected {
				t.Errorf("Interpolate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEngine_EventFieldsOnly(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Name:    "test-event",
		Payload: `{"name":"payload-name","id":"payload-id"}`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"event name field (not payload)", "${trigger:name}", "test-event"},
		{"event id field (not payload)", "${trigger:id}", "test-id"},
		{"payload name field with prefix", "${trigger:payload.name}", "payload-name"},
		{"payload id field with prefix", "${trigger:payload.id}", "payload-id"},
		{"non-event field fails", "Value: ${trigger:custom_field:-default}", "Value: default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			if result != tt.expected {
				t.Errorf("Interpolate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEngine_PayloadRawField(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	payloadContent := `{"user":{"name":"John"},"level":"info"}`
	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Payload: payloadContent,
	}

	result := engine.Interpolate("Raw: ${trigger:payload}", event)
	expected := "Raw: " + payloadContent
	if result != expected {
		t.Errorf("Interpolate() = %v, want %v", result, expected)
	}
}

func TestEngine_TemplateScope_BasicAndDefaults(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	tpl := map[string]string{
		"service": "mdai_service",
		"url":     "http://localhost:9090/alerts",
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"basic template keys", "svc=${template:service} link=${template:url}", "svc=mdai_service link=http://localhost:9090/alerts"},
		{"missing template key uses default", "x=${template:missing:-def}", "x=def"},
		{"empty input", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.InterpolateWithValues(tt.input, nil, tpl)
			if got != tt.expected {
				t.Errorf("InterpolateWithValues() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestEngine_TemplateScope_EmptyStringFallsBackToDefault(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	tpl := map[string]string{
		"empty": "",
	}
	got := engine.InterpolateWithValues("${template:empty:-fallback}", nil, tpl)
	if got != "fallback" {
		t.Errorf("InterpolateWithValues() = %q, want %q", got, "fallback")
	}
}

func TestEngine_TriggerAndTemplate_Together(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:     "abc-123",
		Name:   "evt",
		Source: "gateway",
	}

	tpl := map[string]string{
		"service": "mdai_service",
	}

	input := "n=${trigger:name} s=${trigger:source} svc=${template:service} id=${trigger:id}"
	want := "n=evt s=gateway svc=mdai_service id=abc-123"

	got := engine.InterpolateWithValues(input, event, tpl)
	if got != want {
		t.Errorf("InterpolateWithValues() = %q, want %q", got, want)
	}
}

func TestEngine_TemplateDoesNotShadowTrigger(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		Name: "event-name",
	}
	tpl := map[string]string{
		"name": "template-name",
	}

	got := engine.InterpolateWithValues("${trigger:name} / ${template:name}", event, tpl)
	want := "event-name / template-name"
	if got != want {
		t.Errorf("InterpolateWithValues() = %q, want %q", got, want)
	}
}

func TestEngine_InterpolateMapWithSources(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:     "id-1",
		Source: "hub",
	}
	tpl := map[string]string{
		"channel": "alerts",
	}

	in := map[string]string{
		"a": "cid=${trigger:id}",
		"b": "ch=${template:channel}",
		"c": "src=${trigger:source} miss=${template:oops:-none}",
	}
	out := engine.InterpolateMapWithSources(in, TriggerSource{Event: event}, TemplateSource{Values: tpl})

	if out["a"] != "cid=id-1" {
		t.Errorf("map['a'] = %q, want %q", out["a"], "cid=id-1")
	}
	if out["b"] != "ch=alerts" {
		t.Errorf("map['b'] = %q, want %q", out["b"], "ch=alerts")
	}
	if out["c"] != "src=hub miss=none" {
		t.Errorf("map['c'] = %q, want %q", out["c"], "src=hub miss=none")
	}
}

func TestEngine_InterpolateWithSources_UnknownScope(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	input := "x=${unknown:key:-d}"
	// With default, unknown scope will not be consulted; replaceMatchWithSources will see scope missing,
	// log error, and return original match (since scope not supported). That means default is NOT applied.
	got := engine.InterpolateWithSources(input /* no sources */)
	if got != input {
		t.Errorf("InterpolateWithSources() = %q, want %q", got, input)
	}
}

func TestEngine_InterpolateWithValues_NilTemplateMap(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	input := "x=${template:key:-fallback}"
	got := engine.InterpolateWithValues(input, nil, nil)
	if got != "x=fallback" {
		t.Errorf("InterpolateWithValues() with nil map = %q, want %q", got, "x=fallback")
	}
}
