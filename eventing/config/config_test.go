package config

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSafeToken(t *testing.T) {
	cases := map[string]string{
		"":            "unknown",
		"   ":         "unknown",
		"hello.world": "hello_world",
		"a b c":       "a_b_c",
		". . .":       "_____", // FIXME: replace with a more sophisticated token sanitization
		"valid_token": "valid_token",
	}
	for input, want := range cases {
		got := SafeToken(input)
		assert.Equal(t, want, got, "safeToken(%q)", input)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	assert.Equal(t, "first", firstNonEmpty("", "first", "second"))
	assert.Equal(t, "second", firstNonEmpty("", "", "second"))
	assert.Empty(t, firstNonEmpty("", "", ""))
}

// Delay server startup to force initial connect failures.
func TestConnectRetriesUntilServerAvailable(t *testing.T) {
	lc := net.ListenConfig{}
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to pick a free port")
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("expected TCP address, got %T", l.Addr())
	}
	port := addr.Port
	_ = l.Close()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	cfg := Config{
		URL:        url,
		ClientName: "test-retry",
		Logger:     logger,
	}

	// Delay server startup to force initial connect failures
	go func() {
		time.Sleep(1 * time.Second)
		opts := &server.Options{JetStream: true, Port: port}
		srv, createServerErr := server.NewServer(opts)
		assert.NoError(t, createServerErr, "failed to create embedded NATS server")
		go srv.Start()
		assert.True(t, srv.ReadyForConnections(5*time.Second), "embedded server did not start in time")
	}()

	// Attempt to connect with retries
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn, js, err := Connect(ctx, cfg)
	require.NoError(t, err, "Connect should succeed after retries")
	assert.NotNil(t, conn, "nats.Conn should not be nil")
	assert.NotNil(t, js, "JetStream context should not be nil")

	// Cleanup
	_ = conn.Drain()
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected Config
		wantErr  bool
		desc     string
	}{
		{
			name:    "uses default values when no env vars set",
			envVars: map[string]string{},
			expected: Config{
				URL:               "nats://mdai-hub-nats.mdai.svc.cluster.local:4222",
				Subject:           "eventing",
				StreamName:        "EVENTS_STREAM",
				QueueName:         "eventing",
				InactiveThreshold: 1 * time.Minute,
				NatsPassword:      "",
			},
			wantErr: false,
			desc:    "should use all default values",
		},
		{
			name: "uses environment variables when set",
			envVars: map[string]string{
				"NATS_URL":                "nats://custom-server:4222",
				"NATS_SUBJECT":            "custom-eventing",
				"NATS_STREAM_NAME":        "CUSTOM_STREAM",
				"NATS_QUEUE_NAME":         "custom-queue",
				"NATS_INACTIVE_THRESHOLD": "5m",
				"NATS_PASSWORD":           "secret123",
			},
			expected: Config{
				URL:               "nats://custom-server:4222",
				Subject:           "custom-eventing",
				StreamName:        "CUSTOM_STREAM",
				QueueName:         "custom-queue",
				InactiveThreshold: 5 * time.Minute,
				NatsPassword:      "secret123",
			},
			wantErr: false,
			desc:    "should override defaults with env vars",
		},
		{
			name: "handles partial environment variables",
			envVars: map[string]string{
				"NATS_URL":      "nats://partial-server:4222",
				"NATS_PASSWORD": "partial-secret",
			},
			expected: Config{
				URL:               "nats://partial-server:4222",
				Subject:           "eventing",      // default
				StreamName:        "EVENTS_STREAM", // default
				QueueName:         "eventing",      // default
				InactiveThreshold: 1 * time.Minute, // default
				NatsPassword:      "partial-secret",
			},
			wantErr: false,
			desc:    "should mix env vars with defaults",
		},
		{
			name: "handles various duration formats",
			envVars: map[string]string{
				"NATS_INACTIVE_THRESHOLD": "30s",
			},
			expected: Config{
				URL:               "nats://mdai-hub-nats.mdai.svc.cluster.local:4222", // default
				Subject:           "eventing",                                         // default
				StreamName:        "EVENTS_STREAM",                                    // default
				QueueName:         "eventing",                                         // default
				InactiveThreshold: 30 * time.Second,
				NatsPassword:      "", // default
			},
			wantErr: false,
			desc:    "should parse duration correctly",
		},
		{
			name: "returns error for invalid duration",
			envVars: map[string]string{
				"NATS_INACTIVE_THRESHOLD": "invalid-duration",
			},
			expected: Config{},
			wantErr:  true,
			desc:     "should fail with invalid duration format",
		},
		{
			name: "handles empty string values",
			envVars: map[string]string{
				"NATS_URL":      "",
				"NATS_PASSWORD": "",
			},
			expected: Config{
				URL:               "",              // empty from env
				Subject:           "eventing",      // default
				StreamName:        "EVENTS_STREAM", // default
				QueueName:         "eventing",      // default
				InactiveThreshold: 1 * time.Minute, // default
				NatsPassword:      "",              // empty from env
			},
			wantErr: false,
			desc:    "should handle empty string env vars",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			cfg, err := LoadConfig()

			if tt.wantErr {
				assert.Error(t, err, tt.desc)
				return
			}

			require.NoError(t, err, tt.desc)

			assert.Equal(t, tt.expected.URL, cfg.URL, "URL mismatch")
			assert.Equal(t, tt.expected.Subject, cfg.Subject, "Subject mismatch")
			assert.Equal(t, tt.expected.StreamName, cfg.StreamName, "StreamName mismatch")
			assert.Equal(t, tt.expected.QueueName, cfg.QueueName, "QueueName mismatch")
			assert.Equal(t, tt.expected.InactiveThreshold, cfg.InactiveThreshold, "InactiveThreshold mismatch")
			assert.Equal(t, tt.expected.NatsPassword, cfg.NatsPassword, "NatsPassword mismatch")

			assert.Empty(t, cfg.ClientName, "ClientName should be empty")
		})
	}
}

func TestGetMemberIDs(t *testing.T) {
	tests := []struct {
		name     string
		podName  string
		hostname string
		expected func(string) bool
		desc     string
	}{
		{
			name:     "uses POD_NAME when available",
			podName:  "my-pod-123",
			hostname: "ignored-hostname",
			expected: func(result string) bool { return result == "my-pod-123" },
			desc:     "should use POD_NAME over HOSTNAME",
		},
		{
			name:     "uses HOSTNAME when POD_NAME empty",
			podName:  "",
			hostname: "my-hostname",
			expected: func(result string) bool { return result == "my-hostname" },
			desc:     "should fallback to HOSTNAME",
		},
		{
			name:     "uses nuid when both empty",
			podName:  "",
			hostname: "",
			expected: func(result string) bool { return result != "" },
			desc:     "should fallback to nuid",
		},
		{
			name:     "sanitizes invalid characters",
			podName:  "pod@name#with$invalid%chars",
			hostname: "",
			expected: func(result string) bool { return result == "th_invalid_chars" && len(result) == 16 },
			desc:     "should replace invalid chars with underscores and truncate",
		},
		{
			name:     "truncates long names",
			podName:  "very-long-pod-name-that-exceeds-sixteen-characters",
			hostname: "",
			expected: func(result string) bool { return result == "xteen-characters" && len(result) == 16 },
			desc:     "should truncate to 16 chars from the end",
		},
		{
			name:     "preserves valid characters",
			podName:  "valid-pod_123/=",
			hostname: "",
			expected: func(result string) bool { return result == "valid-pod_123/=" },
			desc:     "should preserve A-Z, a-z, 0-9, -_/=",
		},
		{
			name:     "handles mixed case",
			podName:  "Pod-Type-ABC",
			hostname: "",
			expected: func(result string) bool { return result == "Pod-Type-ABC" },
			desc:     "should preserve case",
		},
		{
			name:     "handles exactly 16 chars",
			podName:  "exactly16chars12",
			hostname: "",
			expected: func(result string) bool { return result == "exactly16chars12" && len(result) == 16 },
			desc:     "should not truncate when exactly 16 chars",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.podName != "" {
				t.Setenv("POD_NAME", tt.podName)
			}
			if tt.hostname != "" {
				t.Setenv("HOSTNAME", tt.hostname)
			}

			result := GetMemberIDs()

			assert.True(t, tt.expected(result), "%s: got %q", tt.desc, result)
			assert.LessOrEqual(t, len(result), 16, "result should not exceed 16 characters: %q", result)
			assert.NotEmpty(t, result, "result should not be empty")
		})
	}
}

func TestWildcardString(t *testing.T) {
	tests := []struct {
		desc          string
		subjectConfig MdaiSubjectConfig
		expected      string
	}{
		{
			desc: "two",
			subjectConfig: MdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 2,
			},
			expected: "eventing.foobar.*.*",
		},
		{
			desc: "five",
			subjectConfig: MdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 5,
			},
			expected: "eventing.foobar.*.*.*.*.*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual := tt.subjectConfig.GetWildcardString()
			assert.Equal(t, tt.expected, actual, "WildcardString mismatch")
		})
	}
}

func TestGetWildcardAndSuffixedSubjects(t *testing.T) {
	tests := []struct {
		desc          string
		suffixes      []string
		subjectConfig MdaiSubjectConfig
		expected      []string
	}{
		{
			desc:     "two",
			suffixes: []string{"dlq"},
			subjectConfig: MdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 2,
			},
			expected: []string{"eventing.foobar.*.*", "eventing.foobar.dlq"},
		},
		{
			desc:     "five",
			suffixes: []string{"dlq"},
			subjectConfig: MdaiSubjectConfig{
				Topic:         "asdf",
				ConsumerGroup: "asdf-consumer",
				WildcardCount: 5,
			},
			expected: []string{"eventing.asdf.*.*.*.*.*", "eventing.asdf.dlq"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual := tt.subjectConfig.GetWildcardAndSuffixedSubjects(tt.suffixes...)
			assert.Equal(t, tt.expected, actual, "Suffix mismatch")
		})
	}
}

func TestGetWildcardIndicesSlice(t *testing.T) {
	tests := []struct {
		desc          string
		subjectConfig MdaiSubjectConfig
		expected      []int
	}{
		{
			desc: "two",
			subjectConfig: MdaiSubjectConfig{
				Topic:         "foobar",
				ConsumerGroup: "bazfoo",
				WildcardCount: 2,
			},
			expected: []int{1, 2},
		},
		{
			desc: "five",
			subjectConfig: MdaiSubjectConfig{
				Topic:         "asdf",
				ConsumerGroup: "asdf-consumer",
				WildcardCount: 5,
			},
			expected: []int{1, 2, 3, 4, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual := tt.subjectConfig.GetWildcardIndices()
			assert.Equal(t, tt.expected, actual, "Bad wildcard indices")
		})
	}
}

func TestGetAllSubjectStringsWithAdditionalSuffixes(t *testing.T) {
	tests := []struct {
		desc     string
		suffixes []string
		expected []string
	}{
		{
			desc:     "none",
			suffixes: []string{},
			expected: []string{
				"eventing.alert.*.*",
				"eventing.var.*.*",
				"eventing.replay.*.*",
			},
		},
		{
			desc:     "dlq",
			suffixes: []string{dlqSuffix},
			expected: []string{
				"eventing.alert.*.*",
				"eventing.alert.dlq",
				"eventing.var.*.*",
				"eventing.var.dlq",
				"eventing.replay.*.*",
				"eventing.replay.dlq",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			actual := allSubjectConfigs.GetAllSubjectStringsWithAdditionalSuffixes(tt.suffixes...)
			assert.Equal(t, tt.expected, actual, "Bad subject strings")
		})
	}
}
