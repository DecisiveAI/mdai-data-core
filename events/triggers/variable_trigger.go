package triggers

import (
	"go.uber.org/zap/zapcore"
)

type VariableCtx struct {
	Name       string `json:"name"`            // variable key
	UpdateType string `json:"update_type"`     // "added" | "removed" | "changed"
	Value      string `json:"value,omitempty"` // current value if applicable
}

type VariableTrigger struct {
	Name string `json:"name,omitempty"` // exact; "" = any
	// TODO add UpdateType to CRD
	UpdateType string `json:"update_type,omitempty"` // "added", "removed", "changed"
}

func (t *VariableTrigger) Match(ctx Context) bool {
	// Variable Name is required
	if t.Name == "" {
		return false
	}
	v := ctx.Variable
	if v == nil {
		return false
	}
	if t.Name != "" && v.Name != t.Name {
		return false
	}
	if t.UpdateType != "" && v.UpdateType != t.UpdateType {
		return false
	}
	return true
}

func (t *VariableTrigger) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("kind", "variable")
	if t.Name != "" {
		enc.AddString("name", t.Name)
	}
	if t.UpdateType != "" {
		enc.AddString("update_type", t.UpdateType)
	}
	return nil
}

func (t *VariableTrigger) Kind() string { return KindVariable }
