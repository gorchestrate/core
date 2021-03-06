package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	"github.com/golang/protobuf/proto"
	"github.com/gorchestrate/cmd/gorocksdb"
	"github.com/qri-io/jsonschema"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Server struct {
	r     *SelectRuntime
	mu    sync.Mutex
	procs map[string]*Proc
}

func (srv *Server) GetWorkflow(ctx context.Context, req *GetWorkflowReq) (*Workflow, error) {
	p := srv.r.getWorkflow(req.ID)
	if p == nil {
		return nil, fmt.Errorf("not found")
	}
	return p, nil
}

func (srv *Server) FindWorkflows(ctx context.Context, req *FindWorkflowsReq) (*FindWorkflowsResp, error) {
	var ex expr.Node
	if req.Filter != "" {
		var err error
		ex, err = expr.ParseExpression(req.Filter)
		if err != nil {
			return nil, fmt.Errorf("filter parse: %v", err)
		}
	}

	opts := gorocksdb.NewDefaultReadOptions()
	it := srv.r.db.NewIteratorCF(opts, srv.r.cfhWorkflowEvents)
	defer it.Close()

	ret := &FindWorkflowsResp{
		Workflows: []*Workflow{},
	}
	scanned := uint64(0)
	for it.Seek(IndexInt(req.From + 1)); it.Valid(); it.Next() {
		select {
		case <-ctx.Done():
			break
		default:
		}
		scanned++
		if req.Scanlimit != 0 && scanned >= req.Scanlimit {
			break
		}
		var evt WorkflowEvent
		err := proto.Unmarshal(it.Value().Data(), &evt)
		if err != nil {
			panic(err)
		}
		if req.To != 0 && evt.Workflow.UpdatedAt > req.To {
			break
		}
		// filter
		if ex != nil {
			val, ok := vm.Eval(WorkflowExpr{Workflow: evt.Workflow}, ex)
			if !ok || val == nil || val.Nil() {
				continue
			}
			v, ok := val.(value.BoolValue)
			if !ok || !v.Val() {
				continue
			}
		}
		ret.Workflows = append(ret.Workflows, evt.Workflow)
	}
	return ret, nil
}

func (srv *Server) ListenWorkflowsUpdates(req *ListenWorkflowsUpdatesReq, stream Runtime_ListenWorkflowsUpdatesServer) error {
	opts := gorocksdb.NewDefaultReadOptions()
	cur := req.From
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		it := srv.r.db.NewIteratorCF(opts, srv.r.cfhWorkflowEvents)
		from := IndexInt(cur + 1) // do not include event at time == "cur"
		for it.Seek(from); it.Valid(); it.Next() {
			var evt WorkflowEvent
			err := proto.Unmarshal(it.Value().Data(), &evt)
			if err != nil {
				panic(err)
			}
			// if evt.Workflow.Service != req.Service { // TODO: better filters?
			// 	it.Value().Free()
			// 	it.Key().Free()
			// 	continue
			// }
			err = stream.Send(&evt)
			it.Value().Free()
			it.Key().Free()
			if err != nil {
				it.Close()
				return err
			}
			cur = evt.Workflow.UpdatedAt // update
			select {
			case <-stream.Context().Done():
				return nil
			default:
			}
		}
		it.Close()
		time.Sleep(time.Millisecond * 100) // TODO: dynamic wait time?, i.e. based on number of searches with no success.
	}
}

func (srv *Server) DeleteChan(ctx context.Context, req *DeleteChanReq) (*Empty, error) {
	return nil, nil
}

func (srv *Server) DeleteType(ctx context.Context, req *Type) (*Empty, error) {
	return nil, nil
}

// TODO: also make MakeChan a deferred operation?
func (srv *Server) MakeChan(ctx context.Context, c *Channel) (*Empty, error) {
	if c.ID == "" {
		return nil, fmt.Errorf("channel id is emptry")
	}
	if c.Closed {
		return nil, fmt.Errorf("can't make closed channel")
	}
	if c.BufSize != 0 {
		return nil, fmt.Errorf("buf size is set by server")
	}
	pType := srv.r.getType(c.DataType)
	if pType == nil {
		return nil, fmt.Errorf("can't find type '%v' for channel", c.DataType)
	}
	for {
		srv.r.batchMu.Lock()
		if len(srv.r.updates) >= 1000 {
			srv.r.batchMu.Unlock()
			continue
		}
		break
	}
	srv.r.newChannels = append(srv.r.newChannels, &Channel{
		DataType:   c.DataType,
		ID:         c.ID,
		BufMaxSize: c.BufMaxSize,
	})
	cb := srv.r.done
	srv.r.batchMu.Unlock()
	<-cb
	return &Empty{}, nil
}

func (srv *Server) GetWorkflowAPI(ctx context.Context, req *GetWorkflowAPIReq) (*WorkflowAPI, error) {
	return nil, nil
}

func (srv *Server) ListWorkflowAPIs(ctx context.Context, req *ListWorkflowAPIsReq) (*ListWorkflowAPIsResp, error) {
	// if req.ID != "" {
	// 	item, err := srv.r.db.GetCF(srv.r.ro, srv.r.cfhAPIs, []byte(req.ID))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	defer item.Free()
	// 	if !item.Exists() {
	// 		return &ListWorkflowAPIsResp{}, nil
	// 	}
	// 	var p WorkflowAPI
	// 	Unmarshal(item.Data(), &p)
	// 	return &ListWorkflowAPIsResp{Apis: []*WorkflowAPI{&p}}, nil
	// }
	ret := []*WorkflowAPI{}
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	it := srv.r.db.NewIteratorCF(opts, srv.r.cfhAPIs)
	for it.Seek([]byte(req.From)); it.Valid(); it.Next() {
		if string(it.Key().Data()) == req.From {
			continue
		}
		var p WorkflowAPI
		Unmarshal(it.Value().Data(), &p)
		ret = append(ret, &p)
		it.Key().Free()
		it.Value().Free()
	}
	return &ListWorkflowAPIsResp{
		APIs: ret,
	}, nil
}

func (srv *Server) PutWorkflowAPI(ctx context.Context, req *WorkflowAPI) (*Empty, error) {
	if srv.r.getType(req.Input) == nil {
		return nil, fmt.Errorf("Input type not found")
	}
	if srv.r.getType(req.Output) == nil {
		return nil, fmt.Errorf("Output type not found")
	}
	if srv.r.getType(req.State) == nil {
		return nil, fmt.Errorf("State type not found")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("API name is empty")
	}
	if req.Service == "" {
		return nil, fmt.Errorf("Service is emptry")
	}
	oldAPI := srv.r.getWorkflowAPI(req.Name)
	if oldAPI != nil && (oldAPI.Input != req.Input ||
		oldAPI.Output != req.Output ||
		oldAPI.State != req.State) {
		return nil, fmt.Errorf("API's are idempotent, create new or try to change types instead")
	}
	wb := gorocksdb.NewWriteBatch()
	wb.PutCF(srv.r.cfhAPIs, []byte(req.Name), Marshal(req))
	return &Empty{}, srv.r.db.Write(srv.r.wo, wb)
}

func (srv *Server) DeleteWorkflowAPI(ctx context.Context, req *WorkflowAPI) (*Empty, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("API name is empty")
	}
	wb := gorocksdb.NewWriteBatch()
	wb.DeleteCF(srv.r.cfhAPIs, []byte(req.Name))
	return &Empty{}, srv.r.db.Write(srv.r.wo, wb)
}

func (srv *Server) PutType(ctx context.Context, req *Type) (*Empty, error) {
	if req.ID == "" {
		return nil, fmt.Errorf("ID is empty")
	}
	new := jsonschema.Schema{}
	err := json.Unmarshal(req.JsonSchema, &new)
	if err != nil {
		return nil, err
	}

	// TODO: you cannot put type that is not backward-compatible with the old one.

	wb := gorocksdb.NewWriteBatch()
	wb.PutCF(srv.r.cfhTypes, []byte(req.ID), Marshal(req))
	return &Empty{}, srv.r.db.Write(srv.r.wo, wb)
}

func (srv *Server) GetType(ctx context.Context, req *GetTypeReq) (*Type, error) {
	return nil, nil
}

func (srv *Server) ListTypes(ctx context.Context, req *ListTypesReq) (*ListTypesResp, error) {
	// if req.ID != "" {
	// 	item, err := srv.r.db.GetCF(srv.r.ro, srv.r.cfhTypes, []byte(req.ID))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	defer item.Free()
	// 	if !item.Exists() {
	// 		return &ListTypesResp{}, nil
	// 	}
	// 	var p Type
	// 	Unmarshal(item.Data(), &p)
	// 	return &ListTypesResp{Types: []*Type{&p}}, nil
	// }
	ret := []*Type{}
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	it := srv.r.db.NewIteratorCF(opts, srv.r.cfhTypes)
	for it.Seek([]byte(req.From)); it.Valid(); it.Next() {
		if string(it.Key().Data()) == req.From {
			continue
		}
		var p Type
		Unmarshal(it.Value().Data(), &p)
		ret = append(ret, &p)
		it.Key().Free()
		it.Value().Free()
	}
	return &ListTypesResp{
		Types: ret,
	}, nil
}

func (srv *Server) GetChan(ctx context.Context, req *GetChanReq) (*Channel, error) {
	return nil, nil
}

func (srv *Server) ListChans(ctx context.Context, req *ListChansReq) (*ListChansResp, error) {
	// if req.ID != "" {
	// 	item, err := srv.r.db.GetCF(srv.r.ro, srv.r.cfhChannels, []byte(req.ID))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	defer item.Free()
	// 	if !item.Exists() {
	// 		return &ListChansResp{}, nil
	// 	}
	// 	var p Channel
	// 	Unmarshal(item.Data(), &p)
	// 	return &ListChansResp{Chans: []*Channel{&p}}, nil
	// }
	ret := []*Channel{}
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	it := srv.r.db.NewIteratorCF(opts, srv.r.cfhChannels)
	for it.Seek([]byte(req.From)); it.Valid(); it.Next() {
		if string(it.Key().Data()) == req.From {
			continue
		}
		var p Channel
		Unmarshal(it.Value().Data(), &p)
		ret = append(ret, &p)
		it.Key().Free()
		it.Value().Free()
	}
	return &ListChansResp{
		Chans: ret,
	}, nil
}

func (srv *Server) ValidateType(id string, data []byte) error {
	if id == "async.None" && string(data) != "" {
		return fmt.Errorf("schema validation: %v is not empty value for type async.None", string(data))
	}
	stype := srv.r.getType(id)
	pType := jsonschema.RootSchema{}
	err := json.Unmarshal(stype.JsonSchema, &pType)
	if err != nil {
		panic("unmarshal schema: " + err.Error())
	}

	errs, err := pType.ValidateBytes(data)
	if err != nil || len(errs) > 0 {
		return fmt.Errorf("schema validation: schema: %v json:%v %v %v", string(stype.JsonSchema), string(data), errs, err)
	}
	return nil
}

func (srv *Server) ExtendLock(ctx context.Context, req *ExtendLockReq) (*Empty, error) {
	if req.Seconds == 0 {
		req.Seconds = 30
	}
	if !srv.r.extendWorkflowLock(req.ID, req.LockID, time.Second*time.Duration(req.Seconds)) {
		return nil, fmt.Errorf("lock expired")
	}
	return &Empty{}, nil
}

// When locking workflow we have to remember that there may be unblocked selects pending for it
// We allow clients to lock and modify workflow inflight, even if it can be incorrectly handled
// by unblocked selects.
// This is needed for clients to avoid deadlocks/ infinite loops or other kind of issues that has
// to be handled in unsafe manner.
func (srv *Server) LockWorkflow(ctx context.Context, req *LockWorkflowReq) (*LockedWorkflow, error) {
	if req.Seconds == 0 {
		req.Seconds = 30
	}
	err := validateString(req.ID, "workflow id")
	if err != nil {
		return nil, err
	}
	l := srv.r.lockWorkflow(ctx, req.ID, time.Second*time.Duration(req.Seconds))
	if l == nil {
		return nil, fmt.Errorf("can't lock workflow")
	}
	old := srv.r.getWorkflow(req.ID)
	if old == nil {
		srv.r.unlockWorkflow(req.ID, l.id)
		return nil, fmt.Errorf("workflow not found")
	}
	return &LockedWorkflow{
		Workflow: old,
		LockID:   l.id,
	}, nil
}

func (srv *Server) UpdateWorkflow(ctx context.Context, req *UpdateWorkflowReq) (out *Empty, err error) {
	err = validateString(req.Workflow.ID, "workflow id")
	if err != nil {
		return nil, err
	}
	err = validateString(req.Workflow.API, "workflow api")
	if err != nil {
		return nil, err
	}
	err = validateString(req.Workflow.Service, "workflow service")
	if err != nil {
		return nil, err
	}
	if req.Workflow.Status != Workflow_Running &&
		req.Workflow.Status != Workflow_Finished {
		return nil, fmt.Errorf("unexpected workflow status: %v", req.Workflow.Status)
	}

	for _, t := range req.Workflow.Threads {
		err := validateAndFillThread(req.Workflow, t)
		if err != nil {
			return nil, fmt.Errorf("thread %v: %v", t.ID, err)
		}
	}

	api := srv.r.getWorkflowAPI(req.Workflow.API)
	if api == nil {
		return nil, fmt.Errorf("workflow api %v not found", req.Workflow.API)
	}
	if req.Workflow.Status == Workflow_Finished {
		err = srv.ValidateType(api.Output, req.Workflow.Output)
		if err != nil {
			return nil, fmt.Errorf("workflow output is invalid: %v", err)
		}
	} else if req.Workflow.Status != Workflow_Running {
		return nil, fmt.Errorf("workflow status should be either finished or running: %v", req.Workflow.Status)
	}
	err = srv.ValidateType(api.State, req.Workflow.State)
	if err != nil {
		return nil, fmt.Errorf("workflow state is invalid: %v", err)
	}

	if req.LockID == 0 {
		slock := srv.r.lockWorkflow(ctx, req.Workflow.ID, time.Second*30)
		req.LockID = slock.id
	} else if !srv.r.extendWorkflowLock(req.Workflow.ID, req.LockID, time.Second*30) {
		return nil, fmt.Errorf("lock expired")
	}
	defer func() {
		if err == nil { // unlock only successful operations
			srv.r.unlockWorkflow(req.Workflow.ID, req.LockID)
		}
	}()

	var oldSel *Thread
	if req.UnblockedAt != 0 { // mark select as workflowed
		// find select that triggered this update
		srv.mu.Lock()
		g, ok := srv.procs[req.Workflow.Service]
		srv.mu.Unlock()
		if !ok {
			return &Empty{}, nil
		}
		g.mu.Lock()
		q := g.stateQueues[req.Workflow.ID]
		g.mu.Unlock()
		if len(q.Queue) == 0 {
			g.mu.Unlock()
			return nil, fmt.Errorf("state queue %v is empty", req.Workflow.ID)
		}
		if q.Lock.id != req.LockID {
			g.mu.Unlock()
			return nil, fmt.Errorf("state queue %v lock id mismatch %v %v", req.Workflow.ID, req.LockID, q.Lock.id)
		}
		oldSel = q.Queue[0]
	}

	old := srv.r.getWorkflow(req.Workflow.ID)
	if old != nil && old.Version != req.Workflow.Version-1 {
		return nil, fmt.Errorf("version mismatch")
	}

	toCreate, err := NewThreads(req.Workflow, old, oldSel)
	if err != nil {
		return nil, fmt.Errorf("state queue %v is empty", req.Workflow.ID)
	}

	// If thread called new workflow inside - create this workflow.
	var toCall []*Workflow
	for _, t := range toCreate {
		if t.Call != nil {
			p, err := srv.prepareWorkflow(t.Call)
			if err != nil {
				return nil, fmt.Errorf("calling workflow: %v", err)
			}
			toCall = append(toCall, p)
		}
		if t.Select != nil {
			for _, c := range t.Select.Cases {
				if c.Chan == "" {
					continue
				}
				ch := srv.r.dbGetChan(c.Chan)
				if ch == nil {
					return nil, fmt.Errorf("channel %v not found", c.Chan)
				}
				if c.DataType != "" && c.DataType != ch.DataType {
					return nil, fmt.Errorf("Channel dataType mismatch: want %v, has %v", c.DataType, ch.DataType)
				}
				if c.Op == Case_Send {
					err := srv.ValidateType(ch.DataType, c.Data)
					if err != nil {
						return nil, fmt.Errorf("Channel %v data validation failed: %v", c.Chan, err)
					}
				}
			}
		}
	}

	// write to DB
	for {
		srv.r.batchMu.Lock()
		if len(srv.r.updates) >= 1000 {
			srv.r.batchMu.Unlock()
			continue
		}
		break
	}
	srv.r.updates = append(srv.r.updates, workflowUpdate{
		Workflow:        req.Workflow,
		ThreadsToBlock:  toCreate,
		UnblockedThread: oldSel,
	})

	for _, p := range toCall { // new workflows created
		srv.r.updates = append(srv.r.updates, workflowUpdate{
			Workflow:       p,
			ThreadsToBlock: p.Threads,
		})
	}
	cb := srv.r.done
	srv.r.batchMu.Unlock()
	<-cb

	if req.UnblockedAt != 0 { // mark select as processed
		srv.mu.Lock()
		g, ok := srv.procs[req.Workflow.Service]
		srv.mu.Unlock()
		if !ok {
			return &Empty{}, nil
		}
		g.mu.Lock()
		q := g.stateQueues[req.Workflow.ID]
		q.Queue = q.Queue[1:]
		g.mu.Unlock()
	}
	return &Empty{}, nil
}

func (srv *Server) prepareWorkflow(c *Call) (*Workflow, error) {
	err := validateString(c.ID, "workflow id")
	if err != nil {
		return nil, err
	}
	err = validateString(c.API, "workflow API")
	if err != nil {
		return nil, err
	}
	api := srv.r.getWorkflowAPI(c.API)
	if api == nil {
		return nil, fmt.Errorf("workflow API not found: %v", c.API)
	}
	if c.InputType != "" && c.InputType != api.Input {
		return nil, fmt.Errorf("workflow API input type mismatch")
	}
	if c.OutputType != "" && c.OutputType != api.Output {
		return nil, fmt.Errorf("workflow API output type mismatch")
	}
	err = srv.ValidateType(api.Input, c.Input)
	if err != nil {
		return nil, fmt.Errorf("workflow input is invalid: %v", err)
	}
	return &Workflow{
		Status:  Workflow_Started,
		Input:   c.Input,
		ID:      c.ID,
		API:     api.Name,
		Service: api.Service,
		Version: 1,
		Threads: []*Thread{{ // unblock workflow on main thread
			Status:   Thread_Unblocked,
			ID:       "_main_",
			Workflow: c.ID,
			Service:  api.Service,
			Callback: "_start_",
		}},
	}, nil
}

func (srv *Server) NewWorkflow(ctx context.Context, c *Call) (*Empty, error) {
	p, err := srv.prepareWorkflow(c)
	if err != nil {
		return nil, err
	}
	slock := srv.r.trylockWorkflow(c.ID, time.Second*30)
	if slock == nil {
		return nil, fmt.Errorf("can't lock new state")
	}
	defer srv.r.unlockWorkflow(c.ID, slock.id)

	old := srv.r.getWorkflow(c.ID)
	if old != nil {
		return nil, fmt.Errorf("already exists")
	}

	// write to DB
	for {
		srv.r.batchMu.Lock()
		if len(srv.r.updates) >= 1000 { // optimal batch size for WBI
			srv.r.batchMu.Unlock()
			continue
		}
		break
	}
	srv.r.updates = append(srv.r.updates, workflowUpdate{
		Workflow:       p,
		ThreadsToBlock: p.Threads,
	})
	cb := srv.r.done
	srv.r.batchMu.Unlock()
	<-cb

	return &Empty{}, nil
}

func (srv *Server) CloseChan(ctx context.Context, req *CloseChanReq) (*Empty, error) {
	for i, id := range req.IDs {
		err := validateString(id, fmt.Sprintf("channel id %v", i))
		if err != nil {
			return nil, err
		}
	}
	// write to DB
	for {
		srv.r.batchMu.Lock()
		if len(srv.r.updates) >= 1000 { // optimal batch size for WBI
			srv.r.batchMu.Unlock()
			continue
		}
		break
	}
	srv.r.updates = append(srv.r.updates, workflowUpdate{
		ChannelsToClose: req.IDs,
	})
	cb := srv.r.done
	srv.r.batchMu.Unlock()
	<-cb
	return &Empty{}, nil
}

func (srv *Server) RegisterWorkflowHandler(r *RegisterWorkflowHandlerReq, stream Runtime_RegisterWorkflowHandlerServer) error {
	if r.Pool == 0 {
		r.Pool = viper.GetInt64("DefaultServicePool")
	}
	if r.PollIntervalMs == 0 {
		r.PollIntervalMs = 100
	}

	// create group iterator if not exist
	srv.mu.Lock()
	g, ok := srv.procs[r.Service]
	if !ok {
		cctx, ccancel := context.WithCancel(context.Background())
		g = &Proc{
			group:       r.Service,
			pool:        r.Pool,
			interval:    r.PollIntervalMs,
			count:       1,
			cancel:      ccancel,
			out:         make(chan LockedWorkflow),
			r:           srv.r,
			stateQueues: map[string]*WorkflowQueue{},
			ctx:         cctx,
		}
		srv.procs[r.Service] = g
		go g.workflowUpdates()
	} else {
		g.count++
	}
	srv.mu.Unlock()

Loop:
	for {
		select {
		case <-stream.Context().Done():
			break Loop
		case req := <-g.out:
			err := stream.Send(&req)
			if err != nil {
				logrus.Errorf("error sending: %v", err)
				break Loop
			}
		}
	}

	srv.mu.Lock()
	g.count--
	if g.count == 0 {
		g.cancel()
		delete(srv.procs, r.Service)
	}
	srv.mu.Unlock()
	return nil
}

type WorkflowQueue struct {
	Queue []*Thread
	Lock  *sLock
}

type Proc struct {
	group       string
	pool        int64
	count       int64
	interval    int64
	out         chan LockedWorkflow
	mu          sync.Mutex
	r           *SelectRuntime
	stateQueues map[string]*WorkflowQueue

	ctx    context.Context
	cancel func()
}

func (p *Proc) workflowUpdates() {
	logrus.Debugf("start workflowing group %v", p.group)
	defer logrus.Debugf("stop workflowing group %v", p.group)

	go p.readUpdates()
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			var toSend []LockedWorkflow
			p.mu.Lock()
			for id, ss := range p.stateQueues {
				if len(ss.Queue) == 0 {
					delete(p.stateQueues, id)
					continue
				}
				lid := p.r.trylockWorkflow(ss.Queue[0].Workflow, time.Second*30)
				if lid != nil {
					toSend = append(toSend, LockedWorkflow{
						Thread: ss.Queue[0],
						LockID: lid.id,
					})
					ss.Lock = lid
				}
			}
			p.mu.Unlock()

			for _, req := range toSend { // send locked states
				s := p.r.mustGetWorkflow(req.Thread.Workflow)
				if s.Status == Workflow_Finished {
					go func(req LockedWorkflow) {
						// If workflow was finished - don't send stuff to client for workflowing any more. just mark it as workflowed
						p.r.batchMu.Lock()
						p.r.updates = append(p.r.updates, workflowUpdate{
							UnblockedThread: req.Thread,
						})
						cb := p.r.done
						p.r.batchMu.Unlock()
						<-cb
						p.mu.Lock()
						q := p.stateQueues[req.Thread.Workflow]
						q.Queue = q.Queue[1:]
						p.mu.Unlock()
						p.r.unlockWorkflow(req.Thread.Workflow, req.LockID)
					}(req)
					continue
				}
				req.Workflow = &s
				select {
				case p.out <- req:
				case <-p.ctx.Done(): // canceled while sending, will wait for lock to expire
					return
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// seek database and populate p.stateQueues.
// backoff if p.statesQueues size exceeds pool limit
func (p *Proc) readUpdates() {
	prefix := append([]byte(p.group), byte(0))
	lastKey := append([]byte(p.group), byte(0))
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	opts.SetTailing(true) // make sure we don't hold snapshot of db, while
	for {
		it := p.r.db.NewIteratorCF(opts, p.r.cfhUnblockedThreads)
		for it.Seek(lastKey); it.ValidForPrefix(prefix); it.Next() {
			lastKey = p.nextItem(it, lastKey)
			if lastKey == nil { // ctx closed
				it.Close()
				return
			}
		}
		if it.Err() != nil {
			panic(it.Err())
		}
		it.Close()
		time.Sleep(time.Millisecond * time.Duration(p.interval)) // wait for new data to arrive
	}
}

func (p *Proc) nextItem(it *gorocksdb.Iterator, lastKey []byte) []byte {
	defer func() {
		it.Value().Free()
		it.Key().Free()
	}()

	select {
	case <-p.ctx.Done():
		return nil
	default:
	}

	if bytes.Equal(it.Key().Data(), lastKey) { // do nothing, since we start from last key
		return lastKey
	}

	var sel Thread
	err := proto.Unmarshal(it.Value().Data(), &sel)
	if err != nil {
		panic(err)
	}

	for {
		p.mu.Lock()
		if len(p.stateQueues) < int(p.pool) {
			break // locked successfully
		}
		p.mu.Unlock()
		select {
		case <-p.ctx.Done():
			return nil
		default:
		}
		time.Sleep(time.Millisecond * 100) // wait for worker pool to clear out
	}
	if p.stateQueues[sel.Workflow] == nil {
		p.stateQueues[sel.Workflow] = &WorkflowQueue{}
	}
	p.stateQueues[sel.Workflow].Queue = append(p.stateQueues[sel.Workflow].Queue, &sel) // add tasks to state queue
	p.mu.Unlock()
	if len(lastKey) != it.Key().Size() {
		lastKey = make([]byte, it.Key().Size())
	}
	copy(lastKey, it.Key().Data()) // we should copy data, as we will free this later
	return lastKey
}
