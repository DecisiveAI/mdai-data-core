package interpolation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"go.uber.org/zap"
)

// Error represents an error during interpolation
type Error struct {
	Message string
	Scope   string
	Field   string
	Value  any
}

func (e *Error) Error() string {
	return e.Message
}

// Engine handles OTel-inspired interpolation for trigger events
type Engine struct {
	pattern *regexp.Regexp
	logger  *zap.Logger
}

func NewEngine(logger *zap.Logger) *Engine {
	pattern := regexp.MustCompile(`\$\{([^:]+):([^}:-]+)(?::-([^}]*))?}`)
	return &Engine{
		pattern: pattern,
		logger:  logger,
	}
}

// Interpolate processes a string and replaces interpolation expressions with actual values
func (e *Engine) Interpolate(input string, event *eventing.MdaiEvent) string {
	result := e.pattern.ReplaceAllStringFunc(input, func(match string) string {
		replacement := e.replaceMatch(match, event)

		return replacement
	})

	return result
}

// replaceMatch processes a single interpolation match
func (e *Engine) replaceMatch(match string, event *eventing.MdaiEvent) string {
	matches := e.pattern.FindStringSubmatch(match)
	if len(matches) < 3 {
		e.logger.Error("failed to match regex, missing required elements",
			zap.Bool("interpolation made", false),
			zap.String("match", match),
		)
		return match
	}

	scope := strings.TrimSpace(matches[1])
	field := strings.TrimSpace(matches[2])
	defaultValue := ""
	if len(matches) > 3 && matches[3] != "" {
		defaultValue = matches[3]
	}

	if scope != "trigger" {
		e.logger.Error(fmt.Sprintf("unsupported scope '%s' - only 'trigger' scope is currently supported", scope),
			zap.Bool("interpolation made", false),
			zap.String("match", match),
			zap.String("scope", scope),
			zap.String("field", field),
		)
		return match
	}

	value, found := e.getFieldValue(field, event)
	if !found {
		if defaultValue != "" {
			e.logger.Warn(fmt.Sprintf("field '%s' not found, using default value provided", field),
				zap.Bool("interpolation made", true),
				zap.String("match", match),
				zap.String("scope", scope),
				zap.String("field", field),
				zap.String("defaultValue", defaultValue),
			)
			return defaultValue
		}
		e.logger.Error(fmt.Sprintf("field '%s' not found and no default value provided", field),
			zap.Bool("interpolation made", false),
			zap.String("match", match),
			zap.String("scope", scope),
			zap.String("field", field),
		)
		return match
	}

	e.logger.Info("interpolation succeeded",
		zap.Bool("interpolation made", true),
		zap.String("match", match),
		zap.String("scope", scope),
		zap.String("field", field),
		zap.String("value", fmt.Sprintf("%v", value)),
	)
	return value
}

func (e *Engine) getFieldValue(field string, event *eventing.MdaiEvent) (string, bool) {
	if event == nil {
		return "", false
	}

	parts := strings.Split(field, ".")

	if len(parts) == 1 {
		switch field {
		case "id":
			return event.ID, event.ID != ""
		case "name":
			return event.Name, event.Name != ""
		case "timestamp":
			if !event.Timestamp.IsZero() {
				return event.Timestamp.Format(time.RFC3339), true
			}
			return "", false
		case "payload":
			return event.Payload, event.Payload != ""
		case "source":
			return event.Source, event.Source != ""
		case "source_id":
			return event.SourceID, event.SourceID != ""
		case "correlation_id":
			return event.CorrelationID, event.CorrelationID != ""
		case "hub_name":
			return event.HubName, event.HubName != ""
		}
	}

	if strings.HasPrefix(field, "payload.") {
		return e.getPayloadValue(strings.TrimPrefix(field, "payload."), event)
	}

	return "", false
}

func (e *Engine) getPayloadValue(field string, event *eventing.MdaiEvent) (string, bool) {
	if event.Payload == "" {
		return "", false
	}

	var payloadMap map[string]interface{}
	if err := json.Unmarshal([]byte(event.Payload), &payloadMap); err != nil {
		return "", false
	}

	value, found := e.getNestedValue(payloadMap, field)
	if !found {
		return "", false
	}

	return e.convertToString(value), true
}

func (e *Engine) getNestedValue(data map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		value, exists := current[part]
		if !exists {
			return nil, false
		}

		if i == len(parts)-1 {
			return value, true
		}

		if nextMap, ok := value.(map[string]interface{}); ok {
			current = nextMap
		} else {
			return nil, false
		}
	}

	return nil, false
}

func (e *Engine) convertToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int, int32, int64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case time.Time:
		return v.Format(time.RFC3339)
	case map[string]interface{}, []interface{}:
		if jsonBytes, err := json.Marshal(v); err == nil {
			return string(jsonBytes)
		}
		return fmt.Sprintf("%v", v)
	default:
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.String {
			return rv.String()
		}
		return fmt.Sprintf("%v", value)
	}
}
