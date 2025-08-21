package triggers

import (
	"go.uber.org/zap/zapcore"
)

type AlertCtx struct {
	Name   string
	Status string // "firing" | "resolved"
}

type AlertTrigger struct {
	Name   string `json:"name,omitempty"`   // exact; "" = any
	Status string `json:"status,omitempty"` // exact; "" = any
}

func (t *AlertTrigger) Match(ctx Context) bool {
	// Alert Name is required; reject empty triggers
	if t.Name == "" {
		return false
	}
	a := ctx.Alert
	if a == nil {
		return false
	}
	if t.Name != "" && a.Name != t.Name {
		return false
	}
	if t.Status != "" && a.Status != t.Status {
		return false
	}
	return true
}

func (t *AlertTrigger) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("kind", "alert")
	if t.Name != "" {
		enc.AddString("name", t.Name)
	}
	if t.Status != "" {
		enc.AddString("status", t.Status)
	}
	return nil
}

func (t *AlertTrigger) Kind() string { return KindAlert }
