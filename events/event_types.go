package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/decisiveai/mdai-data-core/events/triggers"
)

type Rules struct {
	Rules []Rule `json:"rules"`
}

type Rule struct {
	Name     string           `json:"name"`
	Trigger  triggers.Trigger `json:"-"` // not part of wire shape; handled by custom (un)marshal
	Commands []Command        `json:"commands"`
}

type Command struct {
	Type   string                 `json:"type"`             // e.g., variable.set.add, webhook.call
	Inputs map[string]interface{} `json:"inputs,omitempty"` // command-specific parameters
}

type ruleWire struct {
	Name     string          `json:"name"`
	Trigger  json.RawMessage `json:"trigger"`
	Commands []Command       `json:"commands"`
}

func (r *Rule) UnmarshalJSON(data []byte) error {
	var wire ruleWire
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil {
		return err
	}

	trigger, err := triggers.BuildTrigger(wire.Trigger) // returns *AlertTrigger or *VariableTrigger
	if err != nil {
		return fmt.Errorf("trigger: %w", err)
	}

	r.Name = wire.Name
	r.Trigger = trigger // store pointer so only one assertion path later
	r.Commands = wire.Commands
	return nil
}

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
	// add other trigger kinds here (e.g., *triggers.WindowTrigger)
	default:
		return nil, fmt.Errorf("rule %q: unsupported trigger kind %q", r.Name, kind)
	}

	// Wire shape we want in the ConfigMap value
	type triggerEnvelope struct {
		Kind string `json:"kind"`
		Spec any    `json:"spec"`
	}
	type ruleWire struct {
		Name     string          `json:"name"`
		Trigger  triggerEnvelope `json:"trigger"`
		Commands []Command       `json:"commands"`
	}

	wire := ruleWire{
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
