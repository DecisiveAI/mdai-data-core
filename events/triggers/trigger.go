package triggers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

const (
	KindAlert    = "alert"
	KindVariable = "variable"
)

type Trigger interface {
	// Match event with trigger
	Match(ctx Context) bool
	Kind() string
}

type Context struct {
	Alert    *AlertCtx    // nil unless this was an alert
	Variable *VariableCtx // nil unless this was a variable
	Now      time.Time
}

func BuildTrigger(raw json.RawMessage) (Trigger, error) {
	var envelope struct {
		Kind string          `json:"kind"`
		Spec json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("trigger envelope: %w", err)
	}
	switch envelope.Kind {
	case KindAlert:
		var alert AlertTrigger
		dec := json.NewDecoder(bytes.NewReader(envelope.Spec))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&alert); err != nil {
			return nil, fmt.Errorf("alert spec: %w", err)
		}
		return &alert, nil
	case KindVariable:
		var variable VariableTrigger
		dec := json.NewDecoder(bytes.NewReader(envelope.Spec))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&variable); err != nil {
			return nil, fmt.Errorf("variable spec: %w", err)
		}
		return &variable, nil
	default:
		return nil, fmt.Errorf("unknown trigger kind %q", envelope.Kind)
	}
}
