package main

import (
	"encoding/json"
	"time"

	"github.com/araddon/qlbridge/value"
	"github.com/tidwall/gjson"
)

type WorkflowExpr struct {
	*Workflow
}

func (e WorkflowExpr) Get(key string) (outV value.Value, ok bool) {
	d, err := json.Marshal(e) // TODO: benchrmark / optimize this
	if err != nil {
		panic(err)
	}
	return GetGJson(d, key)
}

func GetGJson(data []byte, expr string) (outV value.Value, ok bool) {
	v := gjson.GetBytes(data, expr)
	switch v.Type {
	case gjson.Null:
		return value.NilValue{}, true
	case gjson.False:
		return value.BoolValueFalse, true
	case gjson.True:
		return value.BoolValueTrue, true
	case gjson.String:
		return value.NewStringValue(v.Str), true
	case gjson.Number:
		return value.NewIntValue(int64(v.Num)), true
	case gjson.JSON:
		return value.NewJsonValue([]byte(v.Raw)), true
	default:
		return value.NilValue{}, false
	}
}

func (w WorkflowExpr) Row() map[string]value.Value {
	return nil
}

func (w WorkflowExpr) Ts() time.Time {
	return time.Now()
}
