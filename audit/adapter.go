package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/alertmanager/template"
	"go.uber.org/multierr"

	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"

	"github.com/valkey-io/valkey-go"
)

var (
	metricRegex = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\{`)
)

const (
	HubName          = "hub_name"
	Expression       = "expression"
	CurrentValue     = "current_value"
	AlertName        = "alert_name"
	EventTriggered   = "event_triggered"
	VariableUpdated  = "variable_updated"
	CollectorRestart = "collector_restart"
	ValkeyUpdate     = "valkey_update"
	Evaluation       = "evaluation"

	mdaiHubEventHistoryStreamName = "mdai_hub_event_history"
)

type AuditAdapter struct {
	logger                  logr.Logger
	valkeyClient            valkey.Client
	valkeyAuditStreamExpiry time.Duration
}

func NewAuditAdapter(
	logger logr.Logger,
	valkeyClient valkey.Client,
	valkeyAuditStreamExpiry time.Duration,
) *AuditAdapter {
	return &AuditAdapter{
		logger:                  logger,
		valkeyClient:            valkeyClient,
		valkeyAuditStreamExpiry: valkeyAuditStreamExpiry,
	}
}

func (c AuditAdapter) HandleEventsGet(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := c.valkeyClient.Do(ctx, c.valkeyClient.B().Xrevrange().Key(mdaiHubEventHistoryStreamName).End("+").Start("-").Build())
		if err := result.Error(); err != nil {
			c.logger.Error(err, "valkey error")
			http.Error(w, "Unable to fetch history from Valkey", http.StatusInternalServerError)
			return
		}

		resultList, err := result.ToArray()
		if err != nil {
			c.logger.Error(err, "failed to get valkey variable as map")
			http.Error(w, "Unable to fetch history from Valkey", http.StatusInternalServerError)
			return
		}

		entries := make([]map[string]any, 0)
		for _, entry := range resultList {
			entryMap, err := entry.AsXRangeEntry()
			if err != nil {
				c.logger.Error(err, "failed to convert entry to map")
				continue
			}

			if processedEntry := processEntry(entryMap); processedEntry != nil {
				entries = append(entries, processedEntry)
			}
		}

		resultMapJson, err := json.Marshal(entries)
		if err != nil {
			c.logger.Error(err, "failed to marshal events map to json")
			http.Error(w, "Unable to fetch history from Valkey", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(resultMapJson); err != nil {
			c.logger.Error(err, "Failed to write response body (y tho)")
		}
	}
}

func processEntry(entryMap valkey.XRangeEntry) map[string]any {
	timestamp := entryMap.FieldValues["timestamp"]
	hubName := entryMap.FieldValues["hub_name"]
	eventType := entryMap.FieldValues["type"]

	switch eventType {
	case CollectorRestart:
		storedVars := showHubCollectorRestartVariables(entryMap.FieldValues)
		return map[string]any{
			"timestamp": timestamp,
			"hubName":   hubName,
			"event":     "mdai/" + CollectorRestart,
			"trigger":   "mdai/" + ValkeyUpdate,
			"context": map[string]any{
				"storedVariables": storedVars,
			},
		}
	case VariableUpdated:
		return map[string]any{
			"timestamp": timestamp,
			"hubName":   hubName,
			"event":     "action/" + VariableUpdated,
			"trigger":   entryMap.FieldValues["event"] + "/" + entryMap.FieldValues["status"],
			"context": map[string]any{
				"variableRef": entryMap.FieldValues["variable_ref"],
				"operation":   entryMap.FieldValues["operation"],
				"target":      entryMap.FieldValues["target"],
			},
			"payload": map[string]any{
				"variable": entryMap.FieldValues["variable"],
			},
		}
	case EventTriggered:
		return map[string]any{
			"timestamp": timestamp,
			"hubName":   hubName,
			"event":     Evaluation + "/prometheus_alert",
			"trigger":   Evaluation,
			"context": map[string]any{
				"name":       entryMap.FieldValues["name"],
				"expression": entryMap.FieldValues["expression"],
				"metric":     entryMap.FieldValues["metric_name"],
			},
			"payload": map[string]any{
				"status":              entryMap.FieldValues["status"],
				"relevantLabelValues": entryMap.FieldValues["relevant_label_values"],
				"value":               entryMap.FieldValues["value"],
			},
		}
	default:
		transformedEntry := make(map[string]any)
		for k, v := range entryMap.FieldValues {
			transformedEntry[k] = v
		}
		return transformedEntry
	}
}

func showHubCollectorRestartVariables(fields map[string]string) string {
	var storedVars []string
	ignoreKeys := map[string]bool{
		"timestamp": true,
		"type":      true,
	}
	for key, value := range fields {
		if strings.HasSuffix(key, "_CSV") && !ignoreKeys[key] && value != "" && value != "n/a" {
			storedVars = append(storedVars, value)
		}
	}
	return strings.Join(storedVars, ",")
}

func (c AuditAdapter) DoVariableUpdateAndLog(ctx context.Context, variableUpdateCommand valkey.Completed, mdaiHubAction MdaiHubAction, valkeyKey string) error {
	c.logger.Info(fmt.Sprintf("Performing %s operation", mdaiHubAction.Operation), "variable", valkeyKey, "mdaiHubAction", mdaiHubAction)
	auditLogCommand := c.makeAuditLogActionCommand(mdaiHubAction)
	results := c.valkeyClient.DoMulti(ctx,
		variableUpdateCommand,
		auditLogCommand,
	)
	valkeyMultiErr := c.accumulateValkeyErrors(results)
	return valkeyMultiErr
}

func (c AuditAdapter) makeAuditLogActionCommand(mdaiHubAction MdaiHubAction) valkey.Completed {
	return c.valkeyClient.B().Xadd().Key(mdaiHubEventHistoryStreamName).Minid().Threshold(c.getAuditLogTTLMinId()).Id("*").FieldValue().FieldValueIter(mdaiHubAction.ToSequence()).Build()
}

func (c AuditAdapter) insertAuditLogEvent(ctx context.Context, mdaiHubEventIter iter.Seq2[string, string]) error {
	result := c.valkeyClient.Do(ctx, c.valkeyClient.B().Xadd().Key(mdaiHubEventHistoryStreamName).Minid().Threshold(c.getAuditLogTTLMinId()).Id("*").FieldValue().FieldValueIter(mdaiHubEventIter).Build())
	if err := result.Error(); err != nil {
		c.logger.Error(err, "failed to append event to history stream", "stream", mdaiHubEventHistoryStreamName)
		return err
	}
	return nil
}

func (c AuditAdapter) InsertAuditLogEventFromMap(ctx context.Context, mdaiHubEventMap map[string]string) error {
	return c.insertAuditLogEvent(ctx, composeValkeyStreamIterFromMap(mdaiHubEventMap))
}

func (c AuditAdapter) InsertAuditLogEventFromEvent(ctx context.Context, mdaiHubEvent MdaiHubEvent) error {
	return c.insertAuditLogEvent(ctx, mdaiHubEvent.ToSequence())
}

func composeValkeyStreamIterFromMap(mapToIter map[string]string) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for k, v := range mapToIter {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (c AuditAdapter) CreateHubEvent(relevantLabels []string, alert template.Alert) MdaiHubEvent {
	metricMatch := metricRegex.FindStringSubmatch(alert.Annotations[Expression])
	metricName := ""
	if len(metricMatch) > 1 {
		metricName = metricMatch[1]
	}

	relevantLabelValues := make([]string, len(relevantLabels))
	for idx, relevantLabel := range relevantLabels {
		relevantLabelValues[idx] = alert.Labels[relevantLabel]
	}

	mdaiHubEvent := MdaiHubEvent{
		HubName:             alert.Annotations[HubName],
		Name:                alert.Annotations[AlertName],
		RelevantLabelValues: strings.Join(relevantLabelValues, ","),
		Type:                EventTriggered,
		MetricName:          metricName,
		Expression:          alert.Annotations[Expression],
		Value:               alert.Annotations[CurrentValue],
		Status:              alert.Status,
	}
	return mdaiHubEvent
}

func (c AuditAdapter) CreateRestartEvent(mdaiCRName string, envMap map[string]string) map[string]string {
	mdaiHubEvent := map[string]string{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"hub_name":  mdaiCRName,
		"type":      "collector_restart",
	}
	for key, value := range envMap {
		mdaiHubEvent[key] = value
	}
	return mdaiHubEvent
}

func (c AuditAdapter) CreateHubAction(relevantLabels []string, variableUpdate *mdaiv1.VariableUpdate, valkeyKey string, alert template.Alert) MdaiHubAction {
	mdaiHubAction := MdaiHubAction{
		HubName:     alert.Annotations[HubName],
		Event:       alert.Annotations[AlertName],
		Status:      alert.Status,
		Type:        VariableUpdated,
		Operation:   string(variableUpdate.Operation),
		Target:      valkeyKey,
		VariableRef: variableUpdate.VariableRef,
		Variable:    alert.Labels[relevantLabels[0]],
	}
	return mdaiHubAction
}

func (c AuditAdapter) getAuditLogTTLMinId() string {
	minid := strconv.FormatInt(time.Now().Add(-c.valkeyAuditStreamExpiry).UnixMilli(), 10)
	return minid
}

func (c AuditAdapter) accumulateValkeyErrors(results []valkey.ValkeyResult) error {
	var valkeyMultiErr error
	for _, result := range results {
		err := result.Error()
		if err != nil {
			valkeyMultiErr = multierr.Append(valkeyMultiErr, err)
			c.logger.Error(err, "Valkey error")
		}
	}
	return valkeyMultiErr
}
