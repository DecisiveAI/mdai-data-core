package audit

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newTestLogger(buf *bytes.Buffer) *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = ""     // omit timestamps for simpler test output
	encoderCfg.EncodeTime = nil // no-op
	encoder := zapcore.NewJSONEncoder(encoderCfg)

	core := zapcore.NewCore(encoder, zapcore.AddSync(buf), zap.DebugLevel)
	return zap.New(core)
}

func TestStructuredLogging(t *testing.T) {
	tests := []struct {
		name         string
		logKey       string
		logMsg       string
		logObject    zapcore.ObjectMarshaler
		expectedData map[string]string
	}{
		{
			name:   "MdaiHubEvent",
			logKey: "hubEvent",
			logMsg: "test event",
			logObject: MdaiHubEvent{
				HubName:             "test-hub",
				Event:               "update",
				Status:              "success",
				Type:                "alert",
				Expression:          "up == 0",
				MetricName:          "up",
				Value:               "0",
				RelevantLabelValues: "instance=foo",
			},
			expectedData: map[string]string{
				"hubName":             "test-hub",
				"event":               "update",
				"status":              "success",
				"type":                "alert",
				"expression":          "up == 0",
				"metricName":          "up",
				"value":               "0",
				"relevantLabelValues": "instance=foo",
			},
		},
		{
			name:   "MdaiHubAction",
			logKey: "hubAction",
			logMsg: "test action",
			logObject: MdaiHubAction{
				HubName:     "test-hub",
				Event:       "action",
				Status:      "ok",
				Type:        "update",
				Operation:   "add_element",
				Target:      "variable/foo/service_list",
				VariableRef: "foo",
				Variable:    "bar",
			},
			expectedData: map[string]string{
				"hub_name":     "test-hub",
				"event":        "action",
				"status":       "ok",
				"type":         "update",
				"operation":    "add_element",
				"target":       "variable/foo/service_list",
				"variable_ref": "foo",
				"variable":     "bar",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := newTestLogger(&buf)
			defer logger.Sync() //nolint:errcheck

			logger.Info(tt.logMsg, zap.Object(tt.logKey, tt.logObject))

			expectedJSON, err := json.Marshal(map[string]any{
				"level":   "info",
				"msg":     tt.logMsg,
				tt.logKey: tt.expectedData,
			})
			require.NoError(t, err)

			t.Run("JSONEq", func(t *testing.T) {
				assert.JSONEq(t, string(expectedJSON), buf.String())
			})

			var logged map[string]any
			err = json.Unmarshal(buf.Bytes(), &logged)
			require.NoError(t, err, "failed to unmarshal log output")

			loggedObj, ok := logged[tt.logKey].(map[string]any)
			assert.True(t, ok, "expected '%s' to be an object", tt.logKey)

			for key, want := range tt.expectedData {
				t.Run("field:"+key, func(t *testing.T) {
					got, ok := loggedObj[key]
					assert.True(t, ok, "expected key %q in log output", key)
					assert.Equal(t, want, got)
				})
			}
		})
	}
}
