package ValkeyAdapter

import (
	"context"
	"errors"
	"testing"

	"github.com/decisiveai/mdai-data-core/audit"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/valkey-io/valkey-go"
	vmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"gopkg.in/yaml.v3"
)

func newAdapterWithMock(t *testing.T) (*ValkeyAdapter, *vmock.Client, context.Context, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	client := vmock.NewClient(ctrl)
	adapter := NewValkeyAdapter(client, logr.Discard(), "hub")
	return adapter, client, context.Background(), ctrl
}

func TestComposeStorageKey(t *testing.T) {
	adapter := NewValkeyAdapter(nil, logr.Discard(), "hub-1")
	assert.Equal(t, "variable/hub-1/myvar", adapter.composeStorageKey("myvar"))
}

func TestGetString(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/foo"

	client.EXPECT().
		Do(ctx, vmock.Match("GET", key)).
		Return(vmock.Result(vmock.ValkeyString("bar")))
	val, found, err := adapter.GetString(ctx, "foo")

	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "bar", val)

	client.EXPECT().
		Do(ctx, vmock.Match("GET", key)).
		Return(vmock.Result(vmock.ValkeyNil()))
	_, found, err = adapter.GetString(ctx, "foo")

	assert.NoError(t, err)
	assert.False(t, found)
}

func TestGetSetAsStringSlice(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/myset"

	client.EXPECT().
		Do(ctx, vmock.Match("SMEMBERS", key)).
		Return(vmock.Result(
			vmock.ValkeyArray(vmock.ValkeyBlobString("first"), vmock.ValkeyBlobString("second")),
		))

	got, err := adapter.GetSetAsStringSlice(ctx, "myset")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"first", "second"}, got)
}

func TestGetMapAsString_YAMLConversion(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/myhash"

	client.EXPECT().
		Do(ctx, vmock.Match("HGETALL", key)).
		Return(vmock.Result(vmock.ValkeyMap(map[string]valkey.ValkeyMessage{
			"a": vmock.ValkeyBlobString("1"),
			"b": vmock.ValkeyBlobString("two"),
			"c": vmock.ValkeyBlobString("3.14"),
		})))

	out, err := adapter.GetMapAsString(ctx, "myhash")
	assert.NoError(t, err)
	var got map[string]any
	assert.NoError(t, yaml.Unmarshal([]byte(out), &got))

	expected := map[string]any{
		"a": 1,     // int
		"b": "two", // string
		"c": 3.14,  // float64
	}

	assert.Equal(t, expected, got)
}

func TestGetOperationDef(t *testing.T) {
	adapter := NewValkeyAdapter(nil, logr.Discard(), "hub")

	cases := []struct {
		op   mdaiv1.VariableUpdateOperation
		want bool
	}{
		{VariableUpdateSet, true},
		{VariableUpdateIntIncrBy, true},
		{mdaiv1.VariableUpdateOperation("unknown"), false},
	}
	for _, c := range cases {
		_, ok := adapter.GetOperationDef(c.op)
		assert.Equal(t, c.want, ok, "operation %q", c.op)
	}
}

func TestDeleteKeysWithPrefixUsingScan(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	prefix := "variable/hub/"
	scanPattern := prefix + "*"
	keyToDelete := prefix + "killme"
	keyToKeep := prefix + "keepme"

	scanReply := vmock.ValkeyArray(
		vmock.ValkeyBlobString("0"),
		vmock.ValkeyArray(
			vmock.ValkeyBlobString(keyToDelete),
			vmock.ValkeyBlobString(keyToKeep),
		),
	)

	gomock.InOrder(
		client.EXPECT().
			Do(ctx, vmock.Match(
				"SCAN", "0",
				"MATCH", scanPattern,
				"COUNT", "100",
			)).
			Return(vmock.Result(scanReply)),

		client.EXPECT().
			Do(ctx, vmock.Match("DEL", keyToDelete)).
			Return(vmock.Result(vmock.ValkeyInt64(1))),
	)

	keep := map[string]struct{}{"keepme": {}}
	err := adapter.DeleteKeysWithPrefixUsingScan(ctx, keep)
	assert.NoError(t, err)
}

func TestDoVariableUpdateAndLog_Success(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	vu := &mdaiv1.VariableUpdate{Operation: VariableUpdateSet}
	action := audit.MdaiHubAction{Operation: string(VariableUpdateSet)}

	client.
		EXPECT().
		DoMulti(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, cmds ...valkey.Completed) []valkey.ValkeyResult {
			assert.Len(t, cmds, 2, "variable-update cmd + audit-log cmd")
			return []valkey.ValkeyResult{
				vmock.Result(vmock.ValkeyString("OK")),
				vmock.Result(vmock.ValkeyString("OK")),
			}
		})

	ok, err := adapter.DoVariableUpdateAndLog(ctx, vu, action,
		"foo",
		"",
		"bar",
		0,
	)

	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestDoVariableUpdateAndLog_UnknownOp_NoCall(t *testing.T) {
	adapter, _, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	vu := &mdaiv1.VariableUpdate{Operation: mdaiv1.VariableUpdateOperation("bogus")}
	action := audit.MdaiHubAction{Operation: "bogus"}

	ok, err := adapter.DoVariableUpdateAndLog(ctx, vu, action,
		"foo", "", "", 0)

	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestDoVariableUpdateAndLog_ErrorAggregated(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	vu := &mdaiv1.VariableUpdate{Operation: VariableUpdateSet}
	action := audit.MdaiHubAction{Operation: string(VariableUpdateSet)}

	client.
		EXPECT().
		DoMulti(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, cmds ...valkey.Completed) []valkey.ValkeyResult {
			return []valkey.ValkeyResult{
				vmock.Result(vmock.ValkeyString("OK")),
				vmock.ErrorResult(errors.New("boom")),
			}
		})

	ok, err := adapter.DoVariableUpdateAndLog(ctx, vu, action,
		"foo", "", "bar", 0)

	assert.True(t, ok)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}
