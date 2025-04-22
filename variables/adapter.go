package ValkeyAdapter

import (
	"context"
	"errors"

	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/go-logr/logr"
	"github.com/valkey-io/valkey-go"
	"gopkg.in/yaml.v3"
)

const (
	VariableKeyPrefix = "variable/"

	VariableUpdateSetAddElement    mdaiv1.VariableUpdateOperation = "mdai/add_element"
	VariableUpdateSetRemoveElement mdaiv1.VariableUpdateOperation = "mdai/remove_element"
	VariableUpdateSet              mdaiv1.VariableUpdateOperation = "mdai/set"
	VariableUpdateDelete           mdaiv1.VariableUpdateOperation = "mdai/delete"
	VariableUpdateIntIncrBy        mdaiv1.VariableUpdateOperation = "mdai/incr_by"
	VariableUpdateIntDecrBy        mdaiv1.VariableUpdateOperation = "mdai/decr_by"
	VariableUpdateSetMapEntry      mdaiv1.VariableUpdateOperation = "mdai/map_set_entry"
	VariableUpdateRemoveMapEntry   mdaiv1.VariableUpdateOperation = "mdai/map_remove_entry"
	VariableUpdateBulkSetKeyValue  mdaiv1.VariableUpdateOperation = "mdai/bulk_set_key_value"
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
		if errors.Is(err, ErrValkeyNilMessage) {
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

func (r *ValkeyAdapter) BulkSetMap(key string, values map[string]string) valkey.Completed {
	hsetFieldValue := r.client.B().Hset().Key(key).FieldValue()
	for field, val := range values {
		hsetFieldValue = hsetFieldValue.FieldValue(field, val)
	}
	return hsetFieldValue.Build()
}

func (r *ValkeyAdapter) RemoveMapEntry(key string, field string) valkey.Completed {
	return r.client.B().Hdel().Key(key).Field(field).Build()
}

type OperationArgs struct {
	Label    string
	Value    string
	IntValue int64
	MapValue map[string]string
}

type OperationDef struct {
	LoopOverAllLabels bool
	BuildVariableCmd  func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed
}

// operationDefs holds the definition for each VariableUpdateOperation.
var operationDefs = map[mdaiv1.VariableUpdateOperation]OperationDef{
	VariableUpdateSetAddElement: {
		LoopOverAllLabels: true,
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.AddElementToSet(key, args.Value)
		},
	},
	VariableUpdateSetRemoveElement: {
		LoopOverAllLabels: true,
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.RemoveElementFromSet(key, args.Value)
		},
	},
	// --- single‐label operations ---
	VariableUpdateBulkSetKeyValue: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.BulkSetMap(key, args.MapValue)
		},
	},
	VariableUpdateSet: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.SetString(key, args.Value)
		},
	},
	VariableUpdateDelete: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.DeleteString(key)
		},
	},
	VariableUpdateIntIncrBy: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.IntIncrBy(key, args.IntValue)
		},
	},
	VariableUpdateIntDecrBy: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.IntDecrBy(key, args.IntValue)
		},
	},
	VariableUpdateSetMapEntry: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.SetMapEntry(key, args.Label, args.Value)
		},
	},
	VariableUpdateRemoveMapEntry: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, args OperationArgs) valkey.Completed {
			return a.RemoveMapEntry(key, args.Label)
		},
	},
}

func (r *ValkeyAdapter) GetOperationDef(operation mdaiv1.VariableUpdateOperation) (OperationDef, bool) {
	def, found := operationDefs[operation]
	return def, found
}
