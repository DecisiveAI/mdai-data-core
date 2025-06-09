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

type HandlerAdapter struct {
	client        valkey.Client
	logger        *zap.Logger
	valkeyAdapter *variables.ValkeyAdapter
}

func NewHandlerAdapter(client valkey.Client, logger *zap.Logger, opts ...variables.ValkeyAdapterOption) *HandlerAdapter {
	va := variables.NewValkeyAdapter(client, logger, opts...)
	ha := &HandlerAdapter{
		client:        client,
		logger:        logger,
		valkeyAdapter: va,
	}

	return ha
}

func (r *HandlerAdapter) AddElementToSet(ctx context.Context, variableKey string, hubName string, value string) error {
	variableUpdateCommand := r.valkeyAdapter.AddElementToSet(variableKey, hubName, value)

	auditAction := StoreVariableAction{
		EventId:     time.Now().String(),
		Operation:   "Add element to set",
		Target:      variableKey,
		VariableRef: value,
		Variable:    value,
	}
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditAction)

	results := r.client.DoMulti(
		ctx,
		variableUpdateCommand,
		auditLogCommand,
	)

	return r.accumulateErrors(results, variableKey) // TODO we should retry here
}

func (r *HandlerAdapter) RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveElementFromSet(variableKey, hubName, value)
	auditAction := StoreVariableAction{
		HubName:     hubName,
		EventId:     time.Now().String(),
		Operation:   "Remove element from set",
		Target:      variableKey,
		VariableRef: value,
		Variable:    value,
	}
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditAction)

	results := r.client.DoMulti(
		ctx,
		variableUpdateCommand,
		auditLogCommand,
	)

	return r.accumulateErrors(results, variableKey) // TODO we should retry here
}

func (r *HandlerAdapter) AddSetMapElement(ctx context.Context, variableKey string, hubName string, field string, value string) error {
	variableUpdateCommand := r.valkeyAdapter.SetMapEntry(variableKey, hubName, field, value)

	auditAction := StoreVariableAction{
		HubName:     hubName,
		EventId:     time.Now().String(),
		Operation:   "Set map entry",
		Target:      variableKey,
		VariableRef: field,
		Variable:    value,
	}
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditAction)

	results := r.client.DoMulti(
		ctx,
		variableUpdateCommand,
		auditLogCommand,
	)

	return r.accumulateErrors(results, variableKey) // TODO we should retry here
}

func (r *HandlerAdapter) RemoveElementFromMap(ctx context.Context, variableKey string, hubName string, field string) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveMapEntry(variableKey, hubName, field)
	auditAction := StoreVariableAction{
		HubName:     hubName,
		EventId:     time.Now().String(),
		Operation:   "Remove element from set",
		Target:      variableKey,
		VariableRef: field,
		Variable:    field,
	}
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditAction)

	results := r.client.DoMulti(
		ctx,
		variableUpdateCommand,
		auditLogCommand,
	)

	return r.accumulateErrors(results, variableKey) // TODO we should retry here
}

func (r *HandlerAdapter) SetStringValue(ctx context.Context, variableKey string, hubName string, value string) error {
	variableUpdateCommand := r.valkeyAdapter.SetString(variableKey, hubName, value)
	auditAction := StoreVariableAction{
		HubName:     hubName,
		EventId:     time.Now().String(),
		Operation:   "Set string value",
		Target:      variableKey,
		VariableRef: value,
		Variable:    value,
	}
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditAction)

	results := r.client.DoMulti(
		ctx,
		variableUpdateCommand,
		auditLogCommand,
	)

	return r.accumulateErrors(results, variableKey) // TODO we should retry here
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

func (r *HandlerAdapter) makeVariableAuditLogActionCommand(action StoreVariableAction) valkey.Completed {
	return r.client.B().Xadd().Key(audit.MdaiHubEventHistoryStreamName).Minid().
		Threshold(audit.GetAuditLogTTLMinId(r.valkeyAdapter.AuditStreamExpiry())).
		Id("*").FieldValue().FieldValueIter(action.ToSequence()).
		Build()
}

type StoreVariableAction struct {
	HubName     string `json:"hub_name"`
	EventId     string `json:"event_id"`
	Operation   string `json:"operation"`
	Target      string `json:"target"`
	VariableRef string `json:"variable_ref"`
	Variable    string `json:"variable"`
}

func (action StoreVariableAction) ToSequence() iter.Seq2[string, string] {
	return func(yield func(K string, V string) bool) {
		fields := map[string]string{
			"timestamp":    time.Now().UTC().Format(time.RFC3339),
			"hub_name":     action.HubName,
			"event_id":     action.EventId,
			"operation":    action.Operation,
			"target":       action.Target,
			"variable_ref": action.VariableRef,
			"variable":     action.Variable,
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
