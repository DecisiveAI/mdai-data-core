package ValkeyAdapter

import (
	"context"
	"errors"
	"fmt"

	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/decisiveai/mdai-data-core/audit"
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
	client                  valkey.Client
	logger                  logr.Logger
	valkeyAuditStreamExpiry time.Duration
}

type ValkeyAdapterOption func(*ValkeyAdapter)

func WithValkeyAuditStreamExpiry(expiry time.Duration) ValkeyAdapterOption {
	return func(va *ValkeyAdapter) {
		va.valkeyAuditStreamExpiry = expiry
	}
}

func (r *ValkeyAdapter) AuditStreamExpiry() time.Duration {
	return r.valkeyAuditStreamExpiry
}

func NewValkeyAdapter(client valkey.Client, logger logr.Logger, opts ...ValkeyAdapterOption) *ValkeyAdapter {
	va := &ValkeyAdapter{
		client:                  client,
		logger:                  logger,
		valkeyAuditStreamExpiry: 30 * 24 * time.Hour,
	}

	for _, opt := range opts {
		opt(va)
	}

	return va
}

func (r *ValkeyAdapter) composeStorageKey(variableStorageKey string, hubName string) string {
	return variableKeyPrefix + hubName + "/" + variableStorageKey
}

func (r *ValkeyAdapter) prefixedRefs(refs []string, hubName string) []string {
	out := make([]string, len(refs))
	for i, ref := range refs {
		out[i] = r.composeStorageKey(ref, hubName)
	}
	return out
}

func (r *ValkeyAdapter) GetOrCreateMetaPriorityList(ctx context.Context, variableKey string, hubName string, variableRefs []string) ([]string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	refs := r.prefixedRefs(variableRefs, hubName)
	list, err := r.client.Do(ctx, r.client.B().Arbitrary("PRIORITYLIST.GETORCREATE").Keys(key).Args(refs...).Build()).AsStrSlice()
	if err == nil {
		r.logger.Info(fmt.Sprintf("Data received for %s: %s", key, list))
		return list, true, nil
	}

	if valkey.IsValkeyNil(err) {
		r.logger.Info("No value found for references", "key", key)
		return nil, false, nil
	}
	return nil, false, err
}

func (r *ValkeyAdapter) GetOrCreateMetaHashSet(ctx context.Context, variableKey string, hubName string, variableStringKey string, variableSetKey string) (string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	stringKey := r.composeStorageKey(variableStringKey, hubName)
	setKey := r.composeStorageKey(variableSetKey, hubName)
	value, err := r.client.Do(ctx, r.client.B().Arbitrary("HASHSET.GETORCREATE").Keys(key).Args(stringKey, setKey).Build()).ToString()
	if err == nil {
		r.logger.Info(fmt.Sprintf("Data received for %s: %s", key, value))
		return value, true, nil
	}

	if valkey.IsValkeyNil(err) {
		r.logger.Info("No value found for references", "key", key)
		return "", false, nil
	}
	return "", false, err
}

func (r *ValkeyAdapter) GetSetAsStringSlice(ctx context.Context, variableKey string, hubName string) ([]string, error) {
	key := r.composeStorageKey(variableKey, hubName)
	valueAsSlice, err := r.client.Do(ctx, r.client.B().Smembers().Key(key).Build()).AsStrSlice()
	if err != nil {
		r.logger.Error(err, "failed to get a Set value from storage", "key", key)
		return nil, err
	}

	r.logger.Info(fmt.Sprintf("Data received for %s: %s", key, valueAsSlice))
	return valueAsSlice, nil
}

// GetString retrieves the string, int and boolean variable.
// It returns the value, a boolean indicating whether the key was found, and any error encountered.
func (r *ValkeyAdapter) GetString(ctx context.Context, variableKey string, hubName string) (string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	value, err := r.client.Do(ctx, r.client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			r.logger.Info("No value found in storage", "key", key)
			return "", false, nil
		}
		r.logger.Error(err, "failed to get String value from storage", "key", key)
		return "", false, err
	}
	r.logger.Info(fmt.Sprintf("Data received for %s: %s", key, value))
	return value, true, nil
}

func (r *ValkeyAdapter) GetMapAsString(ctx context.Context, variableKey string, hubName string) (string, error) {
	key := r.composeStorageKey(variableKey, hubName)
	raw, err := r.client.Do(ctx, r.client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		r.logger.Error(err, "failed to get Map value from storage", "key", key)
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
		r.logger.Error(err, "failed to marshal Map to YAML", "key", key)
		return "", err
	}

	r.logger.Info(fmt.Sprintf("Data received for %s: %s", key, string(yamlData)))
	return string(yamlData), nil
}

func (r *ValkeyAdapter) AddElementToSet(variableKey string, hubName string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	r.logger.Info("Adding element to set", "variableKey", variableKey, "value", value, "key", key)
	return r.client.B().Sadd().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) RemoveElementFromSet(variableKey string, hubName string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Srem().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) SetString(variableKey string, hubName string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Set().Key(key).Value(value).Build()
}

func (r *ValkeyAdapter) DeleteString(variableKey string, hubName string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Del().Key(key).Build()
}

func (r *ValkeyAdapter) IntIncrBy(variableKey string, hubName string, value int64) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Incrby().Key(key).Increment(value).Build()
}

func (r *ValkeyAdapter) IntDecrBy(variableKey string, hubName string, value int64) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Decrby().Key(key).Decrement(value).Build()
}

func (r *ValkeyAdapter) SetMapEntry(variableKey string, hubName string, field string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build()
}

func (r *ValkeyAdapter) BulkSetMap(variableKey string, hubName string, values map[string]string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	hsetFieldValue := r.client.B().Hset().Key(key).FieldValue()
	for field, val := range values {
		hsetFieldValue = hsetFieldValue.FieldValue(field, val)
	}
	return hsetFieldValue.Build()
}

func (r *ValkeyAdapter) RemoveMapEntry(variableKey string, hubName string, field string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Hdel().Key(key).Field(field).Build()
}

func (r *ValkeyAdapter) DoVariableUpdateAndLog(ctx context.Context, variableUpdate *mdaiv1.VariableUpdate, mdaiHubAction audit.MdaiHubAction, valkeyKey string, hubName string, mapKey string, value string, intValue int64) (bool, error) {
	r.logger.Info(fmt.Sprintf("Performing %s operation", mdaiHubAction.Operation), "variable", valkeyKey, "mdaiHubAction", mdaiHubAction)
	auditLogCommand := r.makeAuditLogActionCommand(mdaiHubAction)

	def, found := r.GetOperationDef(variableUpdate.Operation)
	if !found {
		return false, nil
	}

	variableUpdateCommand := def.BuildVariableCmd(r, valkeyKey, hubName, OperationArgs{
		MapKey:   mapKey,
		Value:    value,
		IntValue: intValue, // TODO this is a placeholder for int value
	})
	results := r.client.DoMulti(ctx,
		variableUpdateCommand,
		auditLogCommand,
	)
	valkeyMultiErr := r.accumulateValkeyErrors(results, valkeyKey) // TODO we should probably retry here
	return true, valkeyMultiErr
}

func (r *ValkeyAdapter) accumulateValkeyErrors(results []valkey.ValkeyResult, key string) error {
	var errs []error
	for _, result := range results {
		if err := result.Error(); err != nil {
			wrapped := fmt.Errorf("operation failed on key %s: %w", key, err)
			r.logger.Error(wrapped, "Valkey error")
			errs = append(errs, wrapped)
		}
	}
	return errors.Join(errs...)
}

func (r *ValkeyAdapter) makeAuditLogActionCommand(mdaiHubAction audit.MdaiHubAction) valkey.Completed {
	return r.client.B().Xadd().Key(audit.MdaiHubEventHistoryStreamName).Minid().
		Threshold(audit.GetAuditLogTTLMinId(r.valkeyAuditStreamExpiry)).
		Id("*").FieldValue().FieldValueIter(mdaiHubAction.ToSequence()).
		Build()
}

func (r *ValkeyAdapter) DeleteKeysWithPrefixUsingScan(ctx context.Context, keep map[string]struct{}, hubName string) error {
	prefix := variableKeyPrefix + hubName + "/"
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
	MapKey   string
	HubName  string
	Value    string
	IntValue int64
	MapValue map[string]string
}

type OperationDef struct {
	BuildVariableCmd func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed
}

// operationDefs holds the definition for each VariableUpdateOperation.
var operationDefs = map[mdaiv1.VariableUpdateOperation]OperationDef{
	VariableUpdateSetAddElement: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.AddElementToSet(key, hubName, args.Value)
		},
	},
	VariableUpdateSetRemoveElement: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.RemoveElementFromSet(key, hubName, args.Value)
		},
	},
	// --- single‐label operations ---
	VariableUpdateBulkSetKeyValue: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.BulkSetMap(key, hubName, args.MapValue)
		},
	},
	VariableUpdateSet: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.SetString(key, hubName, args.Value)
		},
	},
	VariableUpdateDelete: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.DeleteString(key, hubName)
		},
	},
	VariableUpdateIntIncrBy: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.IntIncrBy(key, hubName, args.IntValue)
		},
	},
	VariableUpdateIntDecrBy: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.IntDecrBy(key, hubName, args.IntValue)
		},
	},
	VariableUpdateSetMapEntry: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.SetMapEntry(key, hubName, args.MapKey, args.Value)
		},
	},
	VariableUpdateRemoveMapEntry: {
		BuildVariableCmd: func(a *ValkeyAdapter, key string, hubName string, args OperationArgs) valkey.Completed {
			return a.RemoveMapEntry(key, hubName, args.MapKey)
		},
	},
}

func (r *ValkeyAdapter) GetOperationDef(operation mdaiv1.VariableUpdateOperation) (OperationDef, bool) {
	def, found := operationDefs[operation]
	return def, found
}
