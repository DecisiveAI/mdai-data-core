package valkey

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	vk "github.com/valkey-io/valkey-go"
	vkmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestInit_EventualSuccessAfterRetries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	core, logObs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := vkmock.NewClient(ctrl)

	attempts := 0
	var seenOpt vk.ClientOption
	dial := func(opt vk.ClientOption) (vk.Client, error) {
		seenOpt = opt
		attempts++
		if attempts <= 2 {
			return nil, errors.New("transient connect error")
		}
		return mockClient, nil
	}

	cfg := Config{
		InitAddress:            []string{"ignored:0"},
		Password:               "pw",
		InitialBackoffInterval: 1 * time.Millisecond,
		MaxBackoffElapsedTime:  2 * time.Second,
	}

	client, err := Init(ctx, logger, cfg, WithDialer(dial))
	require.NoError(t, err)
	require.NotNil(t, client)
	require.GreaterOrEqual(t, attempts, 3, "expected at least 2 retries then success")

	require.ElementsMatch(t, cfg.InitAddress, seenOpt.InitAddress)
	require.Equal(t, cfg.Password, seenOpt.Password)

	// Logs: at least two warns (two failures) and one success
	var warnCount, infoCount int
	for _, e := range logObs.All() {
		switch {
		case e.Level == zap.WarnLevel && e.Message == "Failed to connect to Valkey. Retrying...":
			warnCount++
		case e.Level == zap.InfoLevel && e.Message == "Connected to Valkey successfully":
			infoCount++
		}
	}
	require.GreaterOrEqual(t, warnCount, 2)
	require.Equal(t, 1, infoCount)
}

func TestInit_ReturnsErrorOnExhaustion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	core, logObs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	attempts := 0
	dial := func(_ vk.ClientOption) (vk.Client, error) {
		attempts++
		return nil, errors.New("boom")
	}

	cfg := Config{
		InitAddress:            []string{"ignored:0"},
		Password:               "",
		InitialBackoffInterval: 1 * time.Millisecond,
		MaxBackoffElapsedTime:  200 * time.Millisecond, // enough for at least one retry
	}

	client, err := Init(ctx, logger, cfg, WithDialer(dial))
	require.Error(t, err)
	require.Nil(t, client)
	require.GreaterOrEqual(t, attempts, 2, "expected at least one retry before giving up")

	// warn count is timing-sensitive, we just ensure no success log.
	for _, e := range logObs.All() {
		require.NotEqual(t, "Connected to Valkey successfully", e.Message)
	}
}
