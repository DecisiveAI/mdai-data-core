package triggers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestAlertTriggerMatch(t *testing.T) {
	makeCtx := func(name, status string) Context {
		return Context{Alert: &AlertCtx{Name: name, Status: status}}
	}

	tests := []struct {
		name    string
		trigger AlertTrigger
		ctx     Context
		want    bool
	}{
		{"empty name in trigger -> no match", AlertTrigger{}, makeCtx("X", "firing"), false},
		{"nil alert", AlertTrigger{Name: "X"}, Context{}, false},

		{"name match (status wildcard)", AlertTrigger{Name: "X"}, makeCtx("X", "firing"), true},
		{"name mismatch", AlertTrigger{Name: "X"}, makeCtx("Y", "firing"), false},

		{"name+status match", AlertTrigger{Name: "db_down", Status: "resolved"}, makeCtx("db_down", "resolved"), true},
		{"name matches, status mismatches", AlertTrigger{Name: "db_down", Status: "resolved"}, makeCtx("db_down", "firing"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.trigger.Match(tc.ctx), "trigger=%+v ctx=%+v", tc.trigger, tc.ctx)
		})
	}
}

func TestAlertTriggerMarshalLogObject(t *testing.T) {
	enc := zapcore.NewMapObjectEncoder()
	tr := AlertTrigger{Name: "db_down", Status: "firing"}

	err := tr.MarshalLogObject(enc)
	assert.NoError(t, err)

	fields := enc.Fields
	assert.Equal(t, "alert", fields["kind"])
	assert.Equal(t, "db_down", fields["name"])
	assert.Equal(t, "firing", fields["status"])
}

func TestAlertTriggerMarshalLogObject_OmitsEmptyStatus(t *testing.T) {
	enc := zapcore.NewMapObjectEncoder()
	tr := AlertTrigger{Name: "db_down"} // name required; status optional

	err := tr.MarshalLogObject(enc)
	assert.NoError(t, err)

	fields := enc.Fields
	assert.Equal(t, "alert", fields["kind"])
	assert.Equal(t, "db_down", fields["name"])
	assert.NotContains(t, fields, "status")
}

func TestAlertTriggerKind(t *testing.T) {
	assert.Equal(t, KindAlert, (&AlertTrigger{}).Kind())
}
