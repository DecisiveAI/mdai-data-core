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
	Value   any
}

func (e *Error) Error() string {
	return e.Message
}

type ValueSource interface {
	Scope() string
	Lookup(field string) (string, bool)
}

// TriggerSource reads fields from MdaiEvent (incl. payload.* JSON path)
type TriggerSource struct {
	Event *eventing.MdaiEvent
}

func (s TriggerSource) Scope() string { return "trigger" }
func (s TriggerSource) Lookup(field string) (string, bool) {
	return getEventFieldValue(field, s.Event)
}

// TemplateSource reads from a flat map[string]string
type TemplateSource struct {
	Values map[string]string
}

func (s TemplateSource) Scope() string { return "template" }
func (s TemplateSource) Lookup(field string) (string, bool) {
	if s.Values == nil {
		return "", false
	}
	v, ok := s.Values[field]
	if !ok || v == "" {
		return "", false
	}
	return v, true
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
// Back-compat: only trigger scope via MdaiEvent
func (e *Engine) Interpolate(input string, event *eventing.MdaiEvent) string {
	return e.InterpolateWithSources(input, TriggerSource{Event: event})
}

// InterpolateWithValues trigger + template scopes
func (e *Engine) InterpolateWithValues(input string, event *eventing.MdaiEvent, templateValues map[string]string) string {
	return e.InterpolateWithSources(input,
		TriggerSource{Event: event},
		TemplateSource{Values: templateValues},
	)
}

// InterpolateWithSources provide any set of sources (each defines its Scope() name)
func (e *Engine) InterpolateWithSources(input string, sources ...ValueSource) string {
	if input == "" {
		return ""
	}
	scopeMap := make(map[string]ValueSource, len(sources))
	for _, s := range sources {
		if s == nil {
			continue
		}
		scopeMap[s.Scope()] = s
	}

	return e.pattern.ReplaceAllStringFunc(input, func(match string) string {
		return e.replaceMatchWithSources(match, scopeMap)
	})
}

// replaceMatch processes a single interpolation match
func (e *Engine) replaceMatchWithSources(match string, sources map[string]ValueSource) string {
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

	src, ok := sources[scope]
	if !ok {
		e.logger.Error(fmt.Sprintf("unsupported scope '%s'", scope),
			zap.Bool("interpolation made", false),
			zap.String("match", match),
			zap.String("scope", scope),
			zap.String("field", field),
		)
		return match
	}

	val, found := src.Lookup(field)
	if !found {
		if defaultValue != "" {
			e.logger.Warn(fmt.Sprintf("field '%s' not found, using default", field),
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
		zap.String("value", fmt.Sprintf("%v", val)),
	)

	return val
}

// InterpolateMapWithSources interpolate map with arbitrary sources (e.g., trigger+template)
func (e *Engine) InterpolateMapWithSources(in map[string]string, sources ...ValueSource) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		if v == "" {
			out[k] = v
			continue
		}
		out[k] = e.InterpolateWithSources(v, sources...)
	}
	return out
}

func convertToString(value any) string {
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
	case map[string]any, []any:
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

func getNestedValue(data map[string]interface{}, path string) (any, bool) {
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
		nextMap, ok := value.(map[string]interface{})
		if !ok {
			return nil, false
		}
		current = nextMap
	}

	return nil, false
}

func getPayloadValue(field string, event *eventing.MdaiEvent) (string, bool) {
	if event.Payload == "" {
		return "", false
	}

	var payloadMap map[string]interface{}
	if err := json.Unmarshal([]byte(event.Payload), &payloadMap); err != nil {
		return "", false
	}

	value, found := getNestedValue(payloadMap, field)
	if !found {
		return "", false
	}

	return convertToString(value), true
}

func getEventFieldValue(field string, event *eventing.MdaiEvent) (string, bool) {
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
		return getPayloadValue(strings.TrimPrefix(field, "payload."), event)
	}

	return "", false
}
