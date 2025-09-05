package rule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing/triggers"
)

// Rule represents a rule that triggers a set of commands when a certain event occurs.
type Rule struct {
	Name     string           `json:"name"`
	Trigger  triggers.Trigger `json:"-"` // not part of wire shape; handled by custom (un)marshal
	Commands []Command        `json:"commands"`
}

// Command represents a single command to be executed when a rule is triggered.
type Command struct {
	Type   string          `json:"type"`   // e.g., variable.set.add, webhook.call
	Inputs json.RawMessage `json:"inputs"` // command-specific parameters
}

type ruleWireOut struct {
	Name     string          `json:"name"`
	Trigger  json.RawMessage `json:"trigger"`
	Commands []Command       `json:"commands"`
}

// UnmarshalJSON unmarshals the Rule from JSON.
func (r *Rule) UnmarshalJSON(data []byte) error {
	var wire ruleWireOut
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil {
		return err
	}

	trigger, err := triggers.BuildTrigger(wire.Trigger)
	if err != nil {
		return fmt.Errorf("trigger: %w", err)
	}

	r.Name = wire.Name
	r.Trigger = trigger
	r.Commands = wire.Commands
	return nil
}

type triggerEnvelope struct {
	Kind string `json:"kind"`
	Spec any    `json:"spec"`
}

type ruleWireIn struct {
	Name     string          `json:"name"`
	Trigger  triggerEnvelope `json:"trigger"`
	Commands []Command       `json:"commands"`
}

// MarshalJSON marshals the Rule into JSON.
func (r Rule) MarshalJSON() ([]byte, error) {
	if r.Trigger == nil {
		return nil, fmt.Errorf("rule %q: missing trigger", r.Name)
	}

	kind := r.Trigger.Kind()

	var spec any
	switch t := r.Trigger.(type) {
	case *triggers.AlertTrigger:
		if t == nil {
			return nil, fmt.Errorf("rule %q: nil alert trigger", r.Name)
		}
		spec = t
	case *triggers.VariableTrigger:
		if t == nil {
			return nil, fmt.Errorf("rule %q: nil variable trigger", r.Name)
		}
		spec = t
	default:
		return nil, fmt.Errorf("rule %q: unsupported trigger kind %q", r.Name, kind)
	}

	wire := ruleWireIn{
		Name: r.Name,
		Trigger: triggerEnvelope{
			Kind: kind,
			Spec: spec,
		},
		Commands: r.Commands,
	}
	return json.Marshal(wire)
}

// CommandEvent struct for workflow engine integration
type CommandEvent struct {
	Id              string                 `json:"id"`
	Source          string                 `json:"source"`
	Subject         string                 `json:"subject"` // variable.set.add, webhook.call
	DataContentType string                 `json:"dataContentType"`
	Time            time.Time              `json:"time"`
	HubName         string                 `json:"hubName"`
	Data            map[string]interface{} `json:"data"` // Command parameters
	CorrelationId   string                 `json:"correlationId,omitempty"`
	CausationId     string                 `json:"causationId,omitempty"`
}
