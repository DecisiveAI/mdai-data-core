package handlers

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/decisiveai/mdai-data-core/audit"
	variables "github.com/decisiveai/mdai-data-core/variables"
	"github.com/valkey-io/valkey-go"
)

// HandlerAdapter is a wrapper for handling variable operations.
// Functions of HandlerAdapter execute the provided valkey commands and logs audit entries.
// Common functions arguments:
// ctx is the context for the request.
// variableKey is the key for the Valkey data type.
// hubName is the name of the associated hub, used for namespacing.
// value is the element to be added to the data type.
// correlationId is used for tracking and auditing purposes.
type HandlerAdapter struct {
	client        valkey.Client
	logger        *zap.Logger
	valkeyAdapter *variables.ValkeyAdapter
}

// NewHandlerAdapter creates a new wrapper for handling variable operations.
func NewHandlerAdapter(client valkey.Client, logger *zap.Logger, opts ...variables.ValkeyAdapterOption) *HandlerAdapter {
	va := variables.NewValkeyAdapter(client, logger, opts...)
	ha := &HandlerAdapter{
		client:        client,
		logger:        logger,
		valkeyAdapter: va,
	}

	return ha
}

// AddElementToSet adds an element to a Set data type and logs an audit entry.
func (r *HandlerAdapter) AddElementToSet(ctx context.Context, variableKey string, hubName string, value string, correlationId string) error {
	variableUpdateCommand := r.valkeyAdapter.AddElementToSet(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Add element to set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand)
}

// RemoveElementFromSet removes an element from a Set data type and logs an audit entry.
func (r *HandlerAdapter) RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string, correlationId string) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveElementFromSet(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Remove element from set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand)
}

// SetMapEntry sets a field-value pair in a map data type and logs an audit entry.
// It is using Valkey's HSET command on a Hash data type under the hood.
// This command overwrites the values of specified fields that exist in the hash.
// If key doesn't exist, a new key holding a hash is created.
func (r *HandlerAdapter) SetMapEntry(ctx context.Context, variableKey string, hubName string, field string, value string, correlationId string) error {
	variableUpdateCommand := r.valkeyAdapter.SetMapEntry(variableKey, hubName, field, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Set map entry")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand)
}

// RemoveMapEntry removes a field from a map data type and logs an audit entry.
// It uses Valkey's HDEL command on a Hash data type under the hood.
func (r *HandlerAdapter) RemoveMapEntry(ctx context.Context, variableKey string, hubName string, field string, correlationId string) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveMapEntry(variableKey, hubName, field)

	auditEntry := makeAuditEntry(variableKey, field, correlationId, "Remove element from set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand)
}

// SetStringValue sets a string value and logs an audit entry.
func (r *HandlerAdapter) SetStringValue(ctx context.Context, variableKey string, hubName string, value string, correlationId string) error {
	variableUpdateCommand := r.valkeyAdapter.SetString(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Set string value")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand)
}

func (r *HandlerAdapter) executeAuditedUpdateCommand(ctx context.Context, variableKey string, variableUpdateCommand valkey.Completed, auditLogCommand valkey.Completed) error {
	results := r.client.DoMulti(
		ctx,
		variableUpdateCommand,
		auditLogCommand,
	)

	return r.accumulateErrors(results, variableKey)
}

func (r *HandlerAdapter) accumulateErrors(results []valkey.ValkeyResult, key string) error {
	var errs []string
	for _, result := range results {
		if result.Error() != nil {
			errs = append(errs, fmt.Sprintf("operation failed on key %s: %s", key, result.Error()))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func makeAuditEntry(variableKey string, value string, correlationId string, operation string) StoreVariableAction {
	auditAction := StoreVariableAction{
		EventId:       time.Now().String(),
		Operation:     operation,
		Target:        variableKey,
		VariableRef:   value,
		Variable:      value,
		CorrelationId: correlationId,
	}
	return auditAction
}

func (r *HandlerAdapter) makeVariableAuditLogActionCommand(action StoreVariableAction) valkey.Completed {
	return r.client.B().Xadd().Key(audit.MdaiHubEventHistoryStreamName).Minid().
		Threshold(audit.GetAuditLogTTLMinId(r.valkeyAdapter.AuditStreamExpiry())).
		Id("*").FieldValue().FieldValueIter(action.ToSequence()).
		Build()
}

type StoreVariableAction struct {
	HubName       string `json:"hub_name"`
	EventId       string `json:"event_id"`
	Operation     string `json:"operation"`
	Target        string `json:"target"`
	VariableRef   string `json:"variable_ref"`
	Variable      string `json:"variable"`
	CorrelationId string `json:"correlation_id"`
}

func (action StoreVariableAction) ToSequence() iter.Seq2[string, string] {
	return func(yield func(K string, V string) bool) {
		fields := map[string]string{
			"timestamp":      time.Now().UTC().Format(time.RFC3339),
			"hub_name":       action.HubName,
			"event_id":       action.EventId,
			"operation":      action.Operation,
			"target":         action.Target,
			"variable_ref":   action.VariableRef,
			"variable":       action.Variable,
			"correlation_id": action.CorrelationId,
		}

		for key, value := range fields {
			if value == "" {
				continue
			}
			if !yield(key, value) {
				return
			}
		}
	}
}
