package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gorchestrate/cmd/gorocksdb"
)

func IndexStrInt(s string, i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return bytes.Join([][]byte{[]byte(s), b}, []byte{0})
}

func IndexIntInt(i uint64, j uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, uint64(i))
	binary.BigEndian.PutUint64(b[8:], uint64(j))
	return b
}

func IndexInt(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func Unmarshal(data []byte, m proto.Message) {
	err := proto.Unmarshal(data, m)
	if err != nil {
		panic(err)
	}
}

func Marshal(m proto.Message) []byte {
	data, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

type HLC struct {
	TS    uint64
	Count uint64
}

func (h *HLC) Now() uint64 {
	now := uint64(time.Now().Unix())
	if now < h.TS {
		panic("HLC time went backward")
	}
	if now > h.TS {
		h.TS = now
		h.Count = 0
	} else {
		h.Count++
	}
	return h.TS*1000000 + h.Count
}

func (h *HLC) Data() []byte {
	d, err := json.Marshal(h)
	if err != nil {
		panic(err)
	}
	return d
}

func NewThreads(new, old *Workflow, resumed *Thread) ([]*Thread, error) {
	if old == nil {
		return new.Threads, nil
	}
	for _, t := range old.Threads {
		if resumed != nil && resumed.ID == t.ID { // it's ok to delete resumed thread
			continue
		}
		if !new.HasThread(t.ID) {
			return nil, fmt.Errorf("Thread %v was removed", t.ID)
		}
	}
	var ss []*Thread
	for _, sel := range new.Threads {
		if !old.HasThread(sel.ID) || (resumed != nil && resumed.ID == sel.ID) {
			ss = append(ss, sel)
		}
	}
	return ss, nil
}

func (r *SelectRuntime) getFirst(cfh *gorocksdb.ColumnFamilyHandle, prefix []byte) *ChanSelect {
	baseIt := r.db.NewIteratorCF(r.ro, cfh)
	it := r.wb.NewBaseIterator(baseIt)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item, err := r.wb.GetCF(cfh, it.Key().Data())
		if err != nil {
			panic(err)
		}
		if !item.Exists() { // tombstone shit. WBI iterator can read deleted records...
			it.Key().Free()
			it.Value().Free()
			continue
		}
		var cs ChanSelect
		err = proto.Unmarshal(it.Value().Data(), &cs)
		if err != nil {
			panic(err)
		}
		it.Key().Free()
		it.Value().Free()
		return &cs
	}
	if it.Err() != nil {
		panic(it.Err())
	}
	return nil
}

func (r *SelectRuntime) getFirstBuf(prefix []byte) *BufData {
	baseIt := r.db.NewIteratorCF(r.ro, r.cfhBuffers)
	it := r.wb.NewBaseIterator(baseIt)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item, err := r.wb.GetCF(r.cfhBuffers, it.Key().Data())
		if err != nil {
			panic(err)
		}
		if !item.Exists() { // tombstone shit. WBI iterator can read deleted records...
			it.Key().Free()
			it.Value().Free()
			continue
		}
		var cs BufData
		err = proto.Unmarshal(it.Value().Data(), &cs)
		if err != nil {
			panic(err)
		}
		it.Key().Free()
		it.Value().Free()
		return &cs
	}
	if it.Err() != nil {
		panic(it.Err())
	}
	return nil
}

func (r *SelectRuntime) all(cfh *gorocksdb.ColumnFamilyHandle, prefix []byte) []*ChanSelect {
	baseIt := r.db.NewIteratorCF(r.ro, cfh)
	it := r.wb.NewBaseIterator(baseIt)
	var out []*ChanSelect
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item, err := r.wb.GetCF(cfh, it.Key().Data())
		if err != nil {
			panic(err)
		}
		if !item.Exists() { // tombstone shit. WBI iterator can read deleted records...
			it.Key().Free()
			it.Value().Free()
			continue
		}
		var cs ChanSelect
		err = proto.Unmarshal(it.Value().Data(), &cs)
		if err != nil {
			panic(err)
		}
		out = append(out, &cs)
		it.Key().Free()
		it.Value().Free()
	}
	if it.Err() != nil {
		panic(it.Err())
	}
	return out
}

func (r *SelectRuntime) mustGetWorkflow(id string) Workflow {
	var s Workflow
	item, err := r.db.GetCF(r.ro, r.cfhWorkflows, []byte(id))
	if err != nil {
		panic(err)
	}
	defer item.Free()
	err = proto.Unmarshal(item.Data(), &s)
	if err != nil {
		panic(err)
	}
	return s
}

func (r *SelectRuntime) getWorkflowAPI(id string) *WorkflowAPI {
	var s WorkflowAPI
	item, err := r.db.GetCF(r.ro, r.cfhAPIs, []byte(id))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		return nil
	}
	defer item.Free()
	err = proto.Unmarshal(item.Data(), &s)
	if err != nil {
		panic(err)
	}
	return &s
}

func (r *SelectRuntime) getWorkflow(id string) *Workflow {
	var s Workflow
	item, err := r.db.GetCF(r.ro, r.cfhWorkflows, []byte(id))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		return nil
	}
	defer item.Free()
	err = proto.Unmarshal(item.Data(), &s)
	if err != nil {
		panic(err)
	}
	return &s
}

func (r *SelectRuntime) getType(id string) *Type {
	if id == "async.None" || id == "async.JSON" { // builtin types
		return &Type{
			ID:      id,
			Version: 1,
		}
	}
	var t Type
	item, err := r.db.GetCF(r.ro, r.cfhTypes, []byte(id))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		return nil
	}
	defer item.Free()
	err = proto.Unmarshal(item.Data(), &t)
	if err != nil {
		panic(err)
	}
	return &t
}

func (r *SelectRuntime) getCallSelect(id string) *ChanSelect {
	var s ChanSelect
	item, err := r.db.GetCF(r.ro, r.cfhCalls, []byte(id))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		return nil
	}
	defer item.Free()
	err = proto.Unmarshal(item.Data(), &s)
	if err != nil {
		panic(err)
	}
	return &s
}

func validateString(s, name string) error {
	if s == "" {
		return fmt.Errorf("%v is empty", name)
	}
	if len(s) > 1024 {
		return fmt.Errorf("%v is > 1024 bytes", name)
	}
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return fmt.Errorf("%v contains byte{0}", name)
		}
	}
	return nil
}

func validateAndFillThread(p *Workflow, t *Thread) error {
	if t.Workflow == "" {
		t.Workflow = p.ID
	}
	if t.Service == "" {
		t.Service = p.Service
	}
	if t.Workflow != p.ID {
		return fmt.Errorf("thread workflow id mismatch")
	}
	if t.Service != p.Service {
		return fmt.Errorf("thread service name mismatch")
	}
	if t.Status != Thread_Blocked &&
		t.Status != Thread_Unblocked {
		return fmt.Errorf("unexpected thread status: %v", t.Status)
	}
	if t.Call == nil && t.Select == nil {
		return fmt.Errorf("thread should do either 'Call' or 'Select' ")
	}
	if t.Call != nil {
		if t.Select != nil {
			return fmt.Errorf("you can only do only 'Call' or 'Select'at the same time")
		}
		err := validateString(t.Call.ID, "thread 'Call' id")
		if err != nil {
			return err
		}
		err = validateString(t.Call.API, "thread 'Call' API")
		if err != nil {
			return err
		}
		if t.Call.InputType != "async.None" && len(t.Call.Input) == 0 {
			return fmt.Errorf("Call input is empty")
		}
	}
	if t.Select != nil {
		if t.Call != nil {
			return fmt.Errorf("you can only do only 'Call' or 'Select'at the same time")
		}
		if len(t.Select.Cases) == 0 {
			return fmt.Errorf("select should have at least 1 case")
		}
		if t.Select.Closed {
			return fmt.Errorf("predefined 'Closed' select result is not allowed")
		}
		if len(t.Select.RecvData) != 0 {
			return fmt.Errorf("recv data is filled by server")
		}
		for i, c := range t.Select.Cases {
			msg := fmt.Sprintf("in case %v", i)
			if c.Callback != "" {
				err := validateString(c.Callback, "select to status "+msg)
				if err != nil {
					return err
				}
			}
			switch c.Op {
			case Case_Recv:
				err := validateString(c.Chan, "select channel name "+msg)
				if err != nil {
					return err
				}
				err = validateString(c.DataType, "select data type "+msg)
				if err != nil {
					return err
				}
				if c.Time != 0 {
					return fmt.Errorf("unexpected 'Time' " + msg)
				}
				if len(c.Data) != 0 {
					return fmt.Errorf("unexpected 'Data' in Recv op " + msg)
				}
			case Case_Send:
				err := validateString(c.Chan, "select channel name "+msg)
				if err != nil {
					return err
				}
				err = validateString(c.DataType, "select data type "+msg)
				if err != nil {
					return err
				}
				if c.Time != 0 {
					return fmt.Errorf("unexpected 'Time' " + msg)
				}
				if c.DataType != "async.None" && len(c.Data) == 0 {
					return fmt.Errorf("send operation data is empty " + msg)
				}
			case Case_Time:
				if c.Time == 0 {
					return fmt.Errorf("'Time' op should have unix timestamp supplied " + msg)
				}
				if len(c.Data) != 0 {
					return fmt.Errorf("unexpected 'Data' in Time op " + msg)
				}
				if c.DataType != "" {
					return fmt.Errorf("unexpected 'DataType' in Time op " + msg)
				}
				if c.Chan != "" {
					return fmt.Errorf("unexpected 'Chan' in Time op " + msg)
				}
			case Case_Default:
				if c.Time != 0 || len(c.Data) != 0 || c.DataType != "" || c.Chan != "" {
					return fmt.Errorf("'Default' case can only have 'Callback' set")
				}
			}
		}
	}
	return nil
}

func (s *Workflow) HasThread(id string) bool {
	for _, sel := range s.Threads {
		if sel.ID == id {
			return true
		}
	}
	return false
}

func (s *Workflow) SetThread(new *Thread) (created bool) {
	for i, sel := range s.Threads {
		if sel.ID == new.ID {
			s.Threads[i] = new
			return false
		}
	}
	s.Threads = append(s.Threads, new)
	return true
}

func (w *Call) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID         string
		API       string
		Input      json.RawMessage
		InputType  string
		OutputType string
	}{
		ID:         w.ID,
		API:       w.API,
		Input:      w.Input,
		InputType:  w.InputType,
		OutputType: w.OutputType,
	})
}

func (w *Case) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Callback string
		Op       Case_Op
		Chan     string
		Time     uint64
		Data     json.RawMessage
		DataType string
	}{
		Callback: w.Callback,
		Op:       w.Op,
		Chan:     w.Chan,
		Time:     w.Time,
		Data:     w.Data,
		DataType: w.DataType,
	})
}

func (w *Select) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Cases         []*Case
		UnblockedCase uint64
		RecvData      json.RawMessage
		Closed        bool
	}{
		Cases:         w.Cases,
		UnblockedCase: w.UnblockedCase,
		RecvData:      w.RecvData,
		Closed:        w.Closed,
	})
}

func (w *Workflow) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID        string
		API       string
		Service   string
		Status    Workflow_Status
		Threads   []*Thread
		State     json.RawMessage
		Input     json.RawMessage
		Output    json.RawMessage
		Version   uint64
		UpdatedAt uint64
	}{
		ID:        w.ID,
		API:       w.API,
		Service:   w.Service,
		Status:    w.Status,
		Threads:   w.Threads,
		State:     w.State,
		Input:     w.Input,
		Output:    w.Output,
		Version:   w.Version,
		UpdatedAt: w.UpdatedAt,
	})
}
