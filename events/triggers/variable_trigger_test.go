package triggers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestVariableTriggerMatch(t *testing.T) {
	mk := func(name, updateType, value string) Context {
		return Context{Variable: &VariableCtx{Name: name, UpdateType: updateType, Value: value}}
	}

	tests := []struct {
		name    string
		trigger VariableTrigger
		ctx     Context
		want    bool
	}{
		{"nil variable", VariableTrigger{Name: "cpu"}, Context{}, false},

		// name-only filter
		{"name match only", VariableTrigger{Name: "cpu"}, mk("cpu", "set", "x"), true},
		{"name mismatch", VariableTrigger{Name: "cpu"}, mk("mem", "set", "x"), false},

		// update_type filter (values enforced by CRD)
		{"match added", VariableTrigger{Name: "cpu", UpdateType: "added"}, mk("cpu", "added", "x"), true},
		{"match removed", VariableTrigger{Name: "cpu", UpdateType: "removed"}, mk("cpu", "removed", "x"), true},
		{"match set", VariableTrigger{Name: "cpu", UpdateType: "set"}, mk("cpu", "set", "x"), true},
		{"mismatch update_type", VariableTrigger{Name: "cpu", UpdateType: "removed"}, mk("cpu", "added", "x"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.trigger.Match(tc.ctx), "trigger=%+v ctx=%+v", tc.trigger, tc.ctx)
		})
	}
}

func TestVariableTriggerMarshalLogObject(t *testing.T) {
	enc := zapcore.NewMapObjectEncoder()
	tr := VariableTrigger{Name: "cpu", UpdateType: "set"}

	err := tr.MarshalLogObject(enc)
	assert.NoError(t, err)

	fields := enc.Fields
	assert.Equal(t, "variable", fields["kind"])
	assert.Equal(t, "cpu", fields["name"])
	assert.Equal(t, "set", fields["update_type"])
}

func TestVariableTriggerMarshalLogObject_OmitsOptional(t *testing.T) {
	enc := zapcore.NewMapObjectEncoder()
	tr := VariableTrigger{Name: "cpu"} // name required; update_type optional

	err := tr.MarshalLogObject(enc)
	assert.NoError(t, err)

	fields := enc.Fields
	assert.Equal(t, "variable", fields["kind"])
	assert.Equal(t, "cpu", fields["name"])
	assert.NotContains(t, fields, "update_type")
}

func TestVariableTriggerKind(t *testing.T) {
	assert.Equal(t, KindVariable, (&VariableTrigger{}).Kind())
}

func TestVariableTriggerMatch_EmptyName_NoMatch(t *testing.T) {
	ctx := Context{Variable: &VariableCtx{Name: "cpu", UpdateType: "set"}}
	assert.False(t, (&VariableTrigger{}).Match(ctx))
}
