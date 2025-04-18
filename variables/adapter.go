package ValkeyAdapter

import (
	"context"
	"errors"

	"github.com/go-logr/logr"
	"github.com/valkey-io/valkey-go"
	"gopkg.in/yaml.v3"
)

const (
	VariableKeyPrefix = "variable/"
)

var (
	ErrValkeyNilMessage = errors.New("valkey nil message")
)

type ValkeyAdapter struct {
	client valkey.Client
	logger logr.Logger
}

func NewValkeyAdapter(client valkey.Client, logger logr.Logger) *ValkeyAdapter {
	return &ValkeyAdapter{
		client: client,
		logger: logger,
	}
}

func ComposeValkeyKey(mdaiCRName string, variableStorageKey string) string {
	return VariableKeyPrefix + mdaiCRName + "/" + variableStorageKey
}

func (r *ValkeyAdapter) GetSetAsStringSlice(ctx context.Context, key string) ([]string, error) {
	valueAsSlice, err := r.client.Do(ctx, r.client.B().Smembers().Key(key).Build()).AsStrSlice()
	if err != nil {
		r.logger.Error(err, "Failed to get set value from Valkey", "key", key)
		return nil, err
	}
	r.logger.Info("Valkey data received", "key", key, "valueAsSlice", valueAsSlice)
	return valueAsSlice, nil
}

// GetString retrieves the string, int and boolean variable.
// It returns the value, a boolean indicating whether the key was found, and any error encountered.
func (r *ValkeyAdapter) GetString(ctx context.Context, key string) (string, bool, error) {
	value, err := r.client.Do(ctx, r.client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if errors.Is(err, ErrValkeyNilMessage) || err.Error() == ErrValkeyNilMessage.Error() {
			r.logger.Info("No value found in Valkey", "key", key)
			return "", false, nil
		}
		r.logger.Error(err, "Failed to get String value from Valkey", "key", key)
		return "", false, err
	}
	return value, true, nil
}

func (r *ValkeyAdapter) GetMapAsString(ctx context.Context, key string) (string, error) {
	valueAsMap, err := r.client.Do(ctx, r.client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		r.logger.Error(err, "Failed to get Map value from Valkey", "key", key)
		return "", err
	}
	yamlData, err := yaml.Marshal(valueAsMap)
	if err != nil {
		r.logger.Error(err, "Failed to marshal Map to YAML", "key", key)
		return "", err
	}
	return string(yamlData), nil
}

func (r *ValkeyAdapter) AddElementToSet(key string, value string) valkey.Completed {
	return r.client.B().Sadd().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) RemoveElementFromSet(key string, value string) valkey.Completed {
	return r.client.B().Srem().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) SetString(key string, value string) valkey.Completed {
	return r.client.B().Set().Key(key).Value(value).Build()
}

func (r *ValkeyAdapter) DeleteString(key string) valkey.Completed {
	return r.client.B().Del().Key(key).Build()
}

func (r *ValkeyAdapter) IntIncrBy(key string, value int64) valkey.Completed {
	return r.client.B().Incrby().Key(key).Increment(value).Build()
}

func (r *ValkeyAdapter) IntDecrBy(key string, value int64) valkey.Completed {
	return r.client.B().Decrby().Key(key).Decrement(value).Build()
}

func (r *ValkeyAdapter) SetMapEntry(key string, field string, value string) valkey.Completed {
	return r.client.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build()
}

func (r *ValkeyAdapter) RemoveMapEntry(key string, field string) valkey.Completed {
	return r.client.B().Hdel().Key(key).Field(field).Build()
}
