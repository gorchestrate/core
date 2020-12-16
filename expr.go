package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/araddon/qlbridge/value"
	"github.com/tidwall/gjson"
)

type WorkflowExpr struct {
	*Workflow
}

func (e WorkflowExpr) Get(key string) (outV value.Value, ok bool) {
	defer func() {
		log.Printf("Get %v %v", key, outV)
	}()
	d, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return GetGJson(d, key)
}

func GetGJson(data []byte, expr string) (outV value.Value, ok bool) {
	defer func() {
		log.Printf("GetJson %v %v %v", expr, outV, string(data))
	}()
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
	log.Print("Row()")
	return nil
}
func (w WorkflowExpr) Ts() time.Time {
	log.Print("TS()")
	return time.Now()
}
