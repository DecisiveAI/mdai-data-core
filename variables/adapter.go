package ValkeyAdapter

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"

	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/valkey-io/valkey-go"
	"gopkg.in/yaml.v3"
)

const (
	variableKeyPrefix = "variable/"

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

type ValkeyAdapter struct {
	client  valkey.Client
	Logger  *zap.Logger
	hubName string
}

func NewValkeyAdapter(client valkey.Client, logger *zap.Logger, hubName string) *ValkeyAdapter {
	return &ValkeyAdapter{
		client:  client,
		Logger:  logger,
		hubName: hubName,
	}
}

func (r *ValkeyAdapter) composeStorageKey(variableStorageKey string) string {
	return variableKeyPrefix + r.hubName + "/" + variableStorageKey
}

func (r *ValkeyAdapter) prefixedRefs(refs []string) []string {
	out := make([]string, len(refs))
	for i, ref := range refs {
		out[i] = r.composeStorageKey(ref)
	}
	return out
}

func (r *ValkeyAdapter) GetOrCreateMetaPriorityList(ctx context.Context, variableKey string, variableRefs []string) ([]string, bool, error) {
	key := r.composeStorageKey(variableKey)
	refs := r.prefixedRefs(variableRefs)
	list, err := r.client.Do(ctx, r.client.B().Arbitrary("PRIORITYLIST.GETORCREATE").Keys(key).Args(refs...).Build()).AsStrSlice()
	if err == nil {
		r.Logger.Info(fmt.Sprintf("Data received for %s: %s", key, list))
		return list, true, nil
	}

	if valkey.IsValkeyNil(err) {
		r.Logger.Info("No value found for references", zap.String("key", key))
		return nil, false, nil
	}
	return nil, false, err
}

func (r *ValkeyAdapter) GetOrCreateMetaHashSet(ctx context.Context, variableKey string, variableStringKey string, variableSetKey string) (string, bool, error) {
	key := r.composeStorageKey(variableKey)
	stringKey := r.composeStorageKey(variableStringKey)
	setKey := r.composeStorageKey(variableSetKey)
	value, err := r.client.Do(ctx, r.client.B().Arbitrary("HASHSET.GETORCREATE").Keys(key).Args(stringKey, setKey).Build()).ToString()
	if err == nil {
		r.Logger.Info(fmt.Sprintf("Data received for %s: %s", key, value))
		return value, true, nil
	}

	if valkey.IsValkeyNil(err) {
		r.Logger.Info("No value found for references", zap.Error(err), zap.String("key", key))
		return "", false, nil
	}
	return "", false, err
}

func (r *ValkeyAdapter) GetSetAsStringSlice(ctx context.Context, variableKey string) ([]string, error) {
	key := r.composeStorageKey(variableKey)
	valueAsSlice, err := r.client.Do(ctx, r.client.B().Smembers().Key(key).Build()).AsStrSlice()
	if err != nil {
		r.Logger.Error("failed to get a Set value from storage", zap.Error(err), zap.String("key", key))
		return nil, err
	}

	r.Logger.Info(fmt.Sprintf("Data received for %s: %s", key, valueAsSlice))
	return valueAsSlice, nil
}

// GetString retrieves the string, int and boolean variable.
// It returns the value, a boolean indicating whether the key was found, and any error encountered.
func (r *ValkeyAdapter) GetString(ctx context.Context, variableKey string) (string, bool, error) {
	key := r.composeStorageKey(variableKey)
	value, err := r.client.Do(ctx, r.client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			r.Logger.Info("No value found in storage", zap.String("key", key))
			return "", false, nil
		}
		r.Logger.Error("failed to get String value from storage", zap.Error(err), zap.String("key", key))
		return "", false, err
	}
	r.Logger.Info(fmt.Sprintf("Data received for %s: %s", key, value))
	return value, true, nil
}

func (r *ValkeyAdapter) GetMapAsString(ctx context.Context, variableKey string) (string, error) {
	key := r.composeStorageKey(variableKey)
	raw, err := r.client.Do(ctx, r.client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		r.Logger.Error("failed to get Map value from storage", zap.Error(err), zap.String("key", key))
		return "", err
	}

	data := make(map[string]any, len(raw))
	for k, v := range raw {
		if i, err := strconv.Atoi(v); err == nil {
			data[k] = i // store as int
		} else if f, err := strconv.ParseFloat(v, 64); err == nil {
			data[k] = f // or float
		} else {
			data[k] = v // leave as string
		}
	}

	yamlData, err := yaml.Marshal(data)
	if err != nil {
		r.Logger.Error("failed to marshal Map to YAML", zap.Error(err), zap.String("key", key))
		return "", err
	}

	r.Logger.Info(fmt.Sprintf("Data received for %s: %s", key, string(yamlData)))
	return string(yamlData), nil
}

func (r *ValkeyAdapter) AddElementToSet(variableKey string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	r.Logger.Info("Adding element to set", zap.String("variableKey", variableKey), zap.String("value", value), zap.String("key", key))

	return r.client.B().Sadd().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) RemoveElementFromSet(variableKey string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	return r.client.B().Srem().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) SetString(variableKey string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	return r.client.B().Set().Key(key).Value(value).Build()
}

func (r *ValkeyAdapter) DeleteString(variableKey string) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	return r.client.B().Del().Key(key).Build()
}

func (r *ValkeyAdapter) IntIncrBy(variableKey string, value int64) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	return r.client.B().Incrby().Key(key).Increment(value).Build()
}

func (r *ValkeyAdapter) IntDecrBy(variableKey string, value int64) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	return r.client.B().Decrby().Key(key).Decrement(value).Build()
}

func (r *ValkeyAdapter) SetMapEntry(variableKey string, field string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	return r.client.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build()
}

func (r *ValkeyAdapter) BulkSetMap(variableKey string, values map[string]string) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	hsetFieldValue := r.client.B().Hset().Key(key).FieldValue()
	for field, val := range values {
		hsetFieldValue = hsetFieldValue.FieldValue(field, val)
	}
	return hsetFieldValue.Build()
}

func (r *ValkeyAdapter) RemoveMapEntry(variableKey string, field string) valkey.Completed {
	key := r.composeStorageKey(variableKey)
	return r.client.B().Hdel().Key(key).Field(field).Build()
}

func (r *ValkeyAdapter) DeleteKeysWithPrefixUsingScan(ctx context.Context, keep map[string]struct{}) error {
	prefix := variableKeyPrefix + r.hubName + "/"
	keyPattern := prefix + "*"

	var cursor uint64
	for {
		scanResult, err := r.client.Do(ctx, r.client.B().Scan().Cursor(cursor).Match(keyPattern).Count(100).Build()).AsScanEntry()
		if err != nil {
			return fmt.Errorf("failed to scan with prefix %s: %w", prefix, err)
		}
		for _, k := range scanResult.Elements {
			res, found := strings.CutPrefix(k, prefix)
			if !found {
				return fmt.Errorf("failed to parse prefix for key %s: %w", k, err)
			}
			if _, exists := keep[res]; exists {
				continue
			}
			if _, err := r.client.Do(ctx, r.client.B().Del().Key(k).Build()).AsInt64(); err != nil {
				return fmt.Errorf("failed to delete key %s: %w", k, err)
			}
		}
		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	return nil
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
