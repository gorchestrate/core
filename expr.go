package main

import (
	"log"
	"strings"
	"time"

	"github.com/araddon/qlbridge/value"
	"github.com/tidwall/gjson"
)

type JsonExpr []byte

type WorkflowExpr struct {
	*Workflow
}

func (e WorkflowExpr) Get(key string) (outV value.Value, ok bool) {
	defer func() {
		log.Printf("Get %v %v", key, outV)
	}()
	w := e.Workflow
	if w == nil {
		return value.NilValue{}, false
	}
	switch key {
	case "id":
		return value.NewStringValue(w.Id), true
	case "name":
		return value.NewStringValue(w.Name), true
	case "service":
		return value.NewStringValue(w.Service), true
	case "status":
		return value.NewStringValue(w.Status.String()), true
	case "version":
		return value.NewIntValue(int64(w.Version)), true
	case "updatedat":
		return value.NewIntValue(int64(w.UpdatedAt)), true
		//case "threads": // TODO: manual filtering logic here
		//return w.Threads, true
	}

	if strings.HasPrefix(key, "input.") {
		log.Print(w.Input)
		key = strings.TrimPrefix(key, "input.")
		return JsonExpr(w.Input).Get(key)
	}
	if strings.HasPrefix(key, "output.") {
		log.Print(w.Input)
		key = strings.TrimPrefix(key, "output.")
		return JsonExpr(w.Output).Get(key)
	}
	if strings.HasPrefix(key, "state.") {
		log.Print(w.Input)
		key = strings.TrimPrefix(key, "state.")
		return JsonExpr(w.State).Get(key)
	}
	return value.NilValue{}, false
}

func (j JsonExpr) Get(expr string) (outV value.Value, ok bool) {
	defer func() {
		log.Printf("GetJson %v %v %v", expr, outV, string(j))
	}()
	v := gjson.GetBytes(j, expr)
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
