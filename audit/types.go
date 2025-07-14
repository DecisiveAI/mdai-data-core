package audit

import (
	"iter"
	"time"

	"go.uber.org/zap/zapcore"
)

// Deprecated: With the move to the MdaiEvent type from the mdai-event-hub, this type is mostly redundant.
// Suggested to use a map[string]string with audit/adapter.InsertAuditLogEventFromMap instead
type MdaiHubEvent struct {
	HubName             string `json:"hub_name"`              // name of hub event was triggered
	Event               string `json:"event"`                 // event type (evaluation/prometheus_alert)
	Type                string `json:"type"`                  // triggered event
	Name                string `json:"name"`                  // context; name of event to connect action
	Expression          string `json:"expression"`            // context; expr used to trigger event
	MetricName          string `json:"metric_name"`           // context; expr delta & metric measured by observer
	Value               string `json:"value"`                 // payload; value of metric when event triggered
	Status              string `json:"status"`                // payload; status of event (active, updated)
	RelevantLabelValues string `json:"relevant_label_values"` // payload; variable triggering event
}

func (hubEvent MdaiHubEvent) ToSequence() iter.Seq2[string, string] {
	return func(yield func(K string, V string) bool) {
		fields := map[string]string{
			"timestamp":             time.Now().UTC().Format(time.RFC3339),
			"event":                 hubEvent.Event,
			"hub_name":              hubEvent.HubName,
			"name":                  hubEvent.Name,
			"relevant_label_values": hubEvent.RelevantLabelValues,
			"type":                  hubEvent.Type,
			"metric_name":           hubEvent.MetricName,
			"expression":            hubEvent.Expression,
			"value":                 hubEvent.Value,
			"status":                hubEvent.Status,
		}

		for key, value := range fields {
			if value == "" {
				continue
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

func (hubEvent MdaiHubEvent) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("mdai-logstream", "audit")
	enc.AddString("hubName", hubEvent.HubName)
	enc.AddString("event", hubEvent.Event)
	enc.AddString("status", hubEvent.Status)
	enc.AddString("type", hubEvent.Type)
	enc.AddString("expression", hubEvent.Expression)
	enc.AddString("metricName", hubEvent.MetricName)
	enc.AddString("value", hubEvent.Value)
	enc.AddString("relevantLabelValues", hubEvent.RelevantLabelValues)
	return nil
}

type MdaiHubAction struct {
	HubName     string `json:"hub_name"`     // name of hub action was triggered
	Event       string `json:"event"`        // event type (action/update_variable)
	Status      string `json:"status"`       // status of event
	Type        string `json:"type"`         // type of action
	Operation   string `json:"operation"`    // operation to perform (add_element, remove_element)
	Target      string `json:"target"`       // target of action (ex. variable/mdaihub-sample/service_list)
	VariableRef string `json:"variable_ref"` // variable affected by action
	Variable    string `json:"variable"`     // variable value
}

func (hubAction MdaiHubAction) ToSequence() iter.Seq2[string, string] {
	return func(yield func(K string, V string) bool) {
		fields := map[string]string{
			"timestamp":    time.Now().UTC().Format(time.RFC3339),
			"event":        hubAction.Event,
			"status":       hubAction.Status,
			"hub_name":     hubAction.HubName,
			"type":         hubAction.Type,
			"operation":    hubAction.Operation,
			"target":       hubAction.Target,
			"variable_ref": hubAction.VariableRef,
			"variable":     hubAction.Variable,
		}

		for key, value := range fields {
			if value == "" {
				continue
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

func (hubAction MdaiHubAction) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("hub_name", hubAction.HubName)
	enc.AddString("event", hubAction.Event)
	enc.AddString("status", hubAction.Status)
	enc.AddString("type", hubAction.Type)
	enc.AddString("operation", hubAction.Operation)
	enc.AddString("target", hubAction.Target)
	enc.AddString("variable_ref", hubAction.VariableRef)
	enc.AddString("variable", hubAction.Variable)
	return nil
}
