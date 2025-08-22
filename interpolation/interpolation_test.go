package interpolation

import (
	"testing"
	"time"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
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
			expected: "Invalid: default", // Falls back to default since 'level' is not an event field
			event:    event,
		},
		{
			name:     "invalid nested event field - should fail",
			input:    "Invalid: ${trigger:user.name:-default}",
			expected: "Invalid: default", // Falls back to default since 'user.name' is not an event field
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
			expected: "Value: ${env:PATH}", // Returns original on error
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
		{
			name:     "string value",
			input:    "${trigger:payload.string_val}",
			expected: "hello",
		},
		{
			name:     "integer value",
			input:    "${trigger:payload.int_val}",
			expected: "42",
		},
		{
			name:     "float value",
			input:    "${trigger:payload.float_val}",
			expected: "3.14",
		},
		{
			name:     "boolean value",
			input:    "${trigger:payload.bool_val}",
			expected: "true",
		},
		{
			name:     "object value (JSON marshaled)",
			input:    "${trigger:payload.object_val}",
			expected: `{"nested":"value"}`,
		},
		{
			name:     "array value (JSON marshaled)",
			input:    "${trigger:payload.array_val}",
			expected: `[1,2,3]`,
		},
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
		ID:   "test-id",
		Name: "", // Empty name
		// Timestamp is zero value
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
		{
			name:     "empty name field with default",
			input:    "${trigger:name:-default-name}",
			expected: "default-name",
		},
		{
			name:     "empty source_id with default",
			input:    "${trigger:source_id:-no-source}",
			expected: "no-source",
		},
		{
			name:     "zero timestamp with default",
			input:    "${trigger:timestamp:-no-time}",
			expected: "no-time",
		},
		{
			name:     "non-empty field",
			input:    "${trigger:id}",
			expected: "test-id",
		},
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
	// Use a test logger that captures logs
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
		{
			name:          "missing event field without default",
			input:         "${trigger:nonexistent}",
			expectedError: true,
		},
		{
			name:          "missing payload field without default",
			input:         "${trigger:payload.nonexistent}",
			expectedError: true,
		},
		{
			name:          "unsupported scope",
			input:         "${env:PATH}",
			expectedError: true,
		},
		{
			name:          "valid field",
			input:         "${trigger:id}",
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			if tt.expectedError {
				// Should return original input when error is logged
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
		{
			name:     "missing nested payload path with default",
			input:    "${trigger:payload.user.age:-unknown}",
			expected: "unknown",
		},
		{
			name:     "missing root payload path with default",
			input:    "${trigger:payload.config.setting:-default}",
			expected: "default",
		},
		{
			name:     "path through non-object in payload with default",
			input:    "${trigger:payload.level.subfield:-default}",
			expected: "default",
		},
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
		Payload: `{"name":"payload-name","id":"payload-id"}`, // Same keys as event fields
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "event name field (not payload)",
			input:    "${trigger:name}",
			expected: "test-event",
		},
		{
			name:     "event id field (not payload)",
			input:    "${trigger:id}",
			expected: "test-id",
		},
		{
			name:     "payload name field with prefix",
			input:    "${trigger:payload.name}",
			expected: "payload-name",
		},
		{
			name:     "payload id field with prefix",
			input:    "${trigger:payload.id}",
			expected: "payload-id",
		},
		{
			name:     "non-event field fails",
			input:    "Value: ${trigger:custom_field:-default}",
			expected: "Value: default",
		},
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
