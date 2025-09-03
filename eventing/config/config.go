package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/kelseyhightower/envconfig"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

const (
	connectTimeout              = 2 * time.Second
	reconnectWait               = 2 * time.Second
	flushTimeout                = 250 * time.Millisecond
	NewSubscriberContextTimeout = 5 * time.Minute
	maxPCGroupMembers           = 5
)

type Config struct {
	URL               string        `default:"nats://mdai-hub-nats.mdai.svc.cluster.local:4222" envconfig:"NATS_URL"`
	Subject           string        `default:"eventing"                                           envconfig:"NATS_SUBJECT"`
	StreamName        string        `default:"EVENTS_STREAM"                                    envconfig:"NATS_STREAM_NAME"`
	QueueName         string        `default:"eventing"                                           envconfig:"NATS_QUEUE_NAME"`
	ClientName        string        `envconfig:"-"`
	InactiveThreshold time.Duration `default:"1m"                                               envconfig:"NATS_INACTIVE_THRESHOLD"`
	NatsPassword      string        `envconfig:"NATS_PASSWORD"`
	Logger            *zap.Logger   `envconfig:"-"`
}

const (
	DefaultAckWait       = 30 * time.Second
	DefaultMaxAckPending = 1
	defaultDuplicates    = 2 * time.Minute
	initialInterval      = 250 * time.Millisecond
	maxInterval          = 60 * time.Second
	multiplier           = 2.0
)

func LoadConfig() (Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return cfg, fmt.Errorf("processing envconfig: %w", err)
	}
	return cfg, nil
}

func SafeToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	return strings.NewReplacer(".", "_", " ", "_").Replace(s)
}

func AddPrefixToSubject(prefix string, eventSubject string) string {
	return prefix + "." + eventSubject
}

//nolint:ireturn
func Connect(ctx context.Context, cfg Config) (*nats.Conn, jetstream.JetStream, error) {
	natsOpts := []nats.Option{
		nats.UserInfo("mdai", cfg.NatsPassword),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.Timeout(connectTimeout),
		nats.ReconnectWait(reconnectWait),
		nats.Name(cfg.ClientName),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			cfg.Logger.Error("NATS disconnect", zap.Error(err))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			cfg.Logger.Error("NATS async error", zap.Error(err))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			cfg.Logger.Warn("NATS connection closed")
		}),
	}

	var conn *nats.Conn
	operation := func() (*nats.Conn, error) {
		return nats.Connect(cfg.URL, natsOpts...)
	}

	conn, err := backoff.Retry(ctx, operation)
	if err != nil {
		return nil, nil, err
	}

	// block here until we have completed an INFO/CONNECT/PONG round-trip
	waitForNATSConnection(ctx, conn, cfg)

	js, err := jetstream.New(conn) // implements pcgroups’ JetStream interface
	if err != nil {
		cfg.Logger.Error("NATS JetStream setup failed", zap.Error(err))
		_ = conn.Drain()
		return nil, nil, err
	}

	cfg.Logger.Info("NATS setup completed")
	return conn, js, nil
}

func waitForNATSConnection(ctx context.Context, conn *nats.Conn, cfg Config) {
	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = initialInterval
	exp.MaxInterval = maxInterval
	exp.Multiplier = multiplier

	notify := func(err error, next time.Duration) {
		cfg.Logger.Error(
			"NATS connection not ready, backing off",
			zap.Error(err),
			zap.Duration("next_retry_in", next),
			zap.String("nats_url", cfg.URL),
		)
	}

	operation := func() (bool, error) {
		// RetryFlush returns nil as soon as FlushTimeout succeeds.
		if err := conn.FlushTimeout(flushTimeout); err != nil {
			return false, err
		}
		cfg.Logger.Info("NATS connection verified")
		return true, nil
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithBackOff(exp),
		backoff.WithNotify(notify),
	)
	if err != nil {
		cfg.Logger.Fatal("Unable to establish NATS connection", zap.Error(err))
	}
	cfg.Logger.Info("NATS connection ready")
}

func GetMemberIDs() string {
	raw := firstNonEmpty(
		os.Getenv("POD_NAME"),
		os.Getenv("HOSTNAME"),
		nuid.Next(), // fallback for local testing
	)
	// Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters
	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '/', r == '=':
			return r
		default:
			return '_'
		}
	}, raw)

	const maxLen = 16
	if len(clean) > maxLen {
		clean = clean[len(clean)-maxLen:]
	}
	return clean
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func EnsurePCGroup(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	if err := ensureElasticGroup(ctx, js, cfg.StreamName, eventing.AlertConsumerGroupName, "eventing.alert.*.*", []int{1, 2}, cfg); err != nil {
		return err
	}
	return ensureElasticGroup(ctx, js, cfg.StreamName, eventing.VarsConsumerGroupName, "eventing.var.*.*", []int{1, 2}, cfg)
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	_, err := js.Stream(ctx, cfg.StreamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		cfg.Logger.Info("Creating new NATS JetStream stream", zap.String("stream_name", cfg.StreamName))
		_, err = js.CreateStream(ctx,
			jetstream.StreamConfig{
				Name: cfg.StreamName,
				// TODO create a separate stream for DLQ since it could have different retention settings
				Subjects:   []string{"eventing.alert.*.*", "eventing.alert.dlq", "eventing.var.*.*", "eventing.var.dlq"},
				Storage:    jetstream.FileStorage,
				Retention:  jetstream.WorkQueuePolicy, // assume no replay needed
				MaxMsgs:    -1,
				MaxBytes:   -1,
				Discard:    jetstream.DiscardOld,
				Duplicates: defaultDuplicates,
			})
	}
	if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return err // otherwise someone else just created it
	}

	return nil
}

func ensureElasticGroup(ctx context.Context, js jetstream.JetStream, streamName, groupName, pattern string, hashWildcards []int, cfg Config) error {
	ec, _ := pcgroups.GetElasticConsumerGroupConfig(ctx, js, streamName, groupName)
	if ec == nil {
		cfg.Logger.Info("NATS Elastic Consumer Group does not exist, creating", zap.String("group_name", groupName), zap.String("pattern", pattern))
		_, err := pcgroups.CreateElastic(
			ctx,
			js,
			streamName,
			groupName,
			maxPCGroupMembers, // works for 1-3 replicas, TODO make it configurable: partitions = replicas * 3  (rounded to something tidy, e.g. 10, 12, 16)
			pattern,
			hashWildcards,
			-1,
			-1,
		)
		if err != nil {
			cfg.Logger.Error("NATS Elastic Consumer Group creation failed", zap.Error(err))
			return err
		}
		cfg.Logger.Info("NATS Elastic Consumer Group created", zap.String("group_name", groupName), zap.String("pattern", pattern))
	}
	return nil
}
