package triggers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildTrigger_AlertOK(t *testing.T) {
	raw := json.RawMessage(`{"kind":"alert","spec":{"name":"db_down","status":"firing"}}`)
	got, err := BuildTrigger(raw)
	assert.NoError(t, err)

	al, ok := got.(*AlertTrigger)
	assert.True(t, ok)
	assert.Equal(t, "db_down", al.Name)
	assert.Equal(t, "firing", al.Status)
}

func TestBuildTrigger_VariableOK(t *testing.T) {
	raw := json.RawMessage(`{"kind":"variable","spec":{"name":"cpu","update_type":"set"}}`)
	got, err := BuildTrigger(raw)
	assert.NoError(t, err)

	vt, ok := got.(*VariableTrigger)
	assert.True(t, ok)
	assert.Equal(t, "cpu", vt.Name)
	assert.Equal(t, "set", vt.UpdateType)
}

func TestBuildTrigger_UnknownKind(t *testing.T) {
	raw := json.RawMessage(`{"kind":"weird","spec":{}}`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "unknown trigger kind")
}

func TestBuildTrigger_AlertSpecUnknownField(t *testing.T) {
	raw := json.RawMessage(`{"kind":"alert","spec":{"name":"x","bogus":1}}`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "alert spec")
}

func TestBuildTrigger_VariableSpecUnknownField(t *testing.T) {
	raw := json.RawMessage(`{"kind":"variable","spec":{"name":"x","update_type":"set","bogus":1}}`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "variable spec")
}

func TestBuildTrigger_BadEnvelope(t *testing.T) {
	// not an object → envelope unmarshal error
	raw := json.RawMessage(`[]`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "trigger envelope")
}
