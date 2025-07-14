package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeAuditEntry(t *testing.T) {
	testCases := []struct {
		caseName      string
		variableKey   string
		value         string
		correlationId string
		operation     string
		expected      StoreVariableAction
	}{
		{
			caseName:      "can make an audit entry",
			variableKey:   "foobar",
			value:         "barbaz",
			correlationId: "bazfoo",
			operation:     "DO ALL THE THINGS",
			expected: StoreVariableAction{
				Operation:     "DO ALL THE THINGS",
				Target:        "foobar",
				VariableRef:   "barbaz",
				Variable:      "barbaz",
				CorrelationId: "bazfoo",
			},
		},
	}

	for _, testCase := range testCases {
		t.Parallel()
		actual := makeAuditEntry(testCase.variableKey, testCase.value, testCase.correlationId, testCase.operation)
		assert.Equal(t, testCase.expected.Operation, actual.Operation)
		assert.Equal(t, testCase.expected.Target, actual.Target)
		assert.Equal(t, testCase.expected.VariableRef, actual.VariableRef)
		assert.Equal(t, testCase.expected.Variable, actual.Variable)
		assert.Equal(t, testCase.expected.CorrelationId, actual.CorrelationId)
	}
}
