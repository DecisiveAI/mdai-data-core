package triggers

import (
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
)

// TODO this is generated untested example of how it may look like, rewrite once command logic is implemented

type VariableCtx struct {
	Name       string `json:"name"`            // variable key
	UpdateType string `json:"update_type"`     // "added" | "removed" | "changed"
	Value      string `json:"value,omitempty"` // current value if applicable
}

type VariableTrigger struct {
	Name string `json:"name,omitempty"` // exact; "" = any
	// TODO add UpdateType to CRD
	UpdateType string `json:"update_type,omitempty"` // "added", "removed", "changed"
	Condition  string `json:"condition,omitempty"`   // e.g. "variable_value == 'critical'"
}

func (t *VariableTrigger) Match(ctx Context) bool {
	v := ctx.Variable
	if v == nil {
		return false
	}
	if t.Name != "" && v.Name != t.Name {
		return false
	}
	if t.UpdateType != "" && v.UpdateType != t.UpdateType {
		return false
	}
	ok, _ := evalVarCondition(t.Condition, *v) // invalid expr → no match
	return ok
}

// evalVarCondition supports: <ident> (==|!=) 'literal'
// idents: variable_value | name | hub_name | update_type
func evalVarCondition(expr string, v VariableCtx) (bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true, nil
	}

	op := "=="
	i := strings.Index(expr, "==")
	if i < 0 {
		i = strings.Index(expr, "!=")
		if i < 0 {
			return false, fmt.Errorf("unsupported operator (use == or !=)")
		}
		op = "!="
	}

	left := strings.TrimSpace(expr[:i])
	right := strings.TrimSpace(expr[i+len(op):])

	if len(right) < 2 || right[0] != '\'' || right[len(right)-1] != '\'' {
		return false, fmt.Errorf("right side must be single-quoted string")
	}
	lit := right[1 : len(right)-1]

	var lhs string
	switch left {
	case "variable_value":
		lhs = v.Value
	case "name":
		lhs = v.Name
	case "update_type":
		lhs = v.UpdateType
	default:
		return false, fmt.Errorf("unknown identifier %q", left)
	}

	eq := lhs == lit
	if op == "==" {
		return eq, nil
	}
	return !eq, nil
}

func (t *VariableTrigger) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("kind", "variable")
	if t.Name != "" {
		enc.AddString("name", t.Name)
	}
	if t.UpdateType != "" {
		enc.AddString("update_type", t.UpdateType)
	}
	if t.Condition != "" {
		enc.AddString("condition", t.Condition)
	}
	return nil
}

func (t *VariableTrigger) Kind() string { return KindVariable }
