package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gorchestrate/cmd/gorocksdb"
	"github.com/golang/protobuf/proto"
	"github.com/qri-io/jsonschema"
)

func IndexStrStr(s, s2 string) []byte {
	return bytes.Join([][]byte{[]byte(s), []byte(s2)}, []byte{0})
}

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

func NewThreads(new, old *Process, resumed *Thread) ([]*Thread, error) {
	if old == nil {
		return new.Threads, nil
	}
	for _, t := range old.Threads {
		if resumed != nil && resumed.Id == t.Id { // it's ok to delete resumed thread
			continue
		}
		if !new.HasThread(t.Id) {
			return nil, fmt.Errorf("Thread %v was removed", t.Id)
		}
	}
	var ss []*Thread
	for _, sel := range new.Threads {
		if !old.HasThread(sel.Id) || (resumed != nil && resumed.Id == sel.Id) {
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

func (r *SelectRuntime) mustGetProcess(id string) Process {
	var s Process
	item, err := r.db.GetCF(r.ro, r.cfhProcesses, []byte(id))
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

func (r *SelectRuntime) getProcessAPI(id string) *ProcessAPI {
	var s ProcessAPI
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

func (r *SelectRuntime) getProcess(id string) *Process {
	var s Process
	item, err := r.db.GetCF(r.ro, r.cfhProcesses, []byte(id))
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

func isCompatible(old, new *jsonschema.Schema, path ...string) error {
	if old.ID != new.ID {
		return fmt.Errorf("id is different at : %v", strings.Join(path, "."))
	}
	if old.Format != new.Format {
		return fmt.Errorf("format is different at : %v", strings.Join(path, "."))
	}
	if old.Ref != new.Ref {
		return fmt.Errorf("ref is different at : %v", strings.Join(path, "."))
	}
	if old.Ref != new.Ref {
		return fmt.Errorf("ref is different at : %v", strings.Join(path, "."))
	}
	// make sure all new validators are same as in old schema.
	// if old validators were deleted - that's ok
	for k, v := range new.Validators {
		if !reflect.DeepEqual(old.Validators[k], v) {
			pNew, okNew := v.(*jsonschema.AdditionalProperties)
			pOld, okOld := old.Validators[k].(*jsonschema.AdditionalProperties)
			if okNew && okOld {
				err := isCompatible(pOld.Schema, pNew.Schema, append(path, "."+k)...)
				if err != nil {
					return err
				}
				for k, v := range *pOld.Properties {
					err := isCompatible(v, (*pNew.Properties)[k], append(path, "."+k)...)
					if err != nil {
						return err
					}
				}
			}

			return fmt.Errorf("validator %v is different at : %v", k, strings.Join(path, "."))
		}
	}

	// check old definitions are compatible with new ones
	// if new definitions were added - that's ok
	for k, v := range old.Definitions {
		err := isCompatible(v, new.Definitions[k], append(path, "."+k)...)
		if err != nil {
			return err
		}
	}
	return nil
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

func validateAndFillThread(p *Process, t *Thread) error {
	if t.Process == "" {
		t.Process = p.Id
	}
	if t.Service == "" {
		t.Service = p.Service
	}
	if t.Process != p.Id {
		return fmt.Errorf("thread process id mismatch")
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
		err := validateString(t.Call.Id, "thread 'Call' id")
		if err != nil {
			return err
		}
		err = validateString(t.Call.Name, "thread 'Call' name")
		if err != nil {
			return err
		}
		if len(t.Call.Input) == 0 {
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
		if t.Select.Result == Select_Closed {
			return fmt.Errorf("predefined 'Closed' select result is not allowed")
		}
		if len(t.Select.RecvData) != 0 {
			return fmt.Errorf("recv data is filled by server")
		}
		for i, c := range t.Select.Cases {
			msg := fmt.Sprintf("in case %v", i)
			if c.ToStatus != "" {
				err := validateString(c.ToStatus, "select to status "+msg)
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
				if c.DataType != "" {
					err = validateString(c.DataType, "select data type "+msg)
					if err != nil {
						return err
					}
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
				if len(c.Data) == 0 {
					return fmt.Errorf("send operation can't send empty data " + msg)
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
					return fmt.Errorf("'Default' case can only have 'ToStatus' set")
				}
			}
		}
	}
	return nil
}


func (s *Process) HasThread(id string) bool {
	for _, sel := range s.Threads {
		if sel.Id == id {
			return true
		}
	}
	return false
}

func (s *Process) SetThread(new *Thread) (created bool) {
	for i, sel := range s.Threads {
		if sel.Id == new.Id {
			s.Threads[i] = new
			return false
		}
	}
	s.Threads = append(s.Threads, new)
	return true
}
