package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gorchestrate/cmd/gorocksdb"
	"github.com/spf13/viper"
)

// states are locked to make sure multiple clients can't process same state at the same time
// locks expiration time should be bigger than maximum workflow execution time
// otherwise one client can start processing workflow that was locked, but not processed in time previously.
type sLock struct {
	id        uint64
	expiresAt time.Time
}

type SelectRuntime struct {
	db                    *gorocksdb.DB
	wo                    *gorocksdb.WriteOptions
	ro                    *gorocksdb.ReadOptions
	wb                    *gorocksdb.WriteBatchWithIndex
	cfhDefault            *gorocksdb.ColumnFamilyHandle
	cfhChannels           *gorocksdb.ColumnFamilyHandle
	cfhChanSend           *gorocksdb.ColumnFamilyHandle
	cfhChanRecv           *gorocksdb.ColumnFamilyHandle
	cfhChanTime           *gorocksdb.ColumnFamilyHandle
	cfhChanWorkflowFinish *gorocksdb.ColumnFamilyHandle
	cfhChanBuffer         *gorocksdb.ColumnFamilyHandle
	cfhBlockedThreads     *gorocksdb.ColumnFamilyHandle
	cfhUnblockedThreads   *gorocksdb.ColumnFamilyHandle
	cfhWorkflows          *gorocksdb.ColumnFamilyHandle
	cfhTypes              *gorocksdb.ColumnFamilyHandle
	cfhAPIs               *gorocksdb.ColumnFamilyHandle
	cfhWorkflowEvents     *gorocksdb.ColumnFamilyHandle
	cfhBuffers            *gorocksdb.ColumnFamilyHandle
	cfhCalls              *gorocksdb.ColumnFamilyHandle

	stateMu sync.Mutex
	states  map[string]sLock

	// hybrid logical clock used to order records in time
	clock HLC

	// We do not immediately flush updates into SSD - we buffer all updates in memory, keeping clients blocked
	// Then we process buffered updates and write them in DB as a single WriteBatch and close "done" channel to unblock them
	// This allows us to maintain performance and return response to clients only after result has been flushed to SSD.
	batchMu     sync.Mutex
	updates     []workflowUpdate // workflows to update
	newChannels []*Channel       // channels to create
	done        chan struct{}    // callback for current batch, this will be closed when batch is flushed to DB
}

// workflowUpdate is applied atomically on the server. It's either success or fail operation.
type workflowUpdate struct {
	Workflow        *Workflow // updated workflow state
	UnblockedThread *Thread   // if update is a result of unblock event - we should mark this event as processed (delete it)
	ThreadsToBlock  []*Thread // here are *new* threads that has to be blocked.
	ChannelsToClose []string  // channels that has to be closed.
}

// Creates(or restores) runtime stored at 'dbpath'
func NewSelectRuntime(dbpath string) (*SelectRuntime, error) {
	dbOpts := gorocksdb.NewDefaultOptions()
	dbOpts.IncreaseParallelism(viper.GetInt("RocksDBThreads"))
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCreateIfMissingColumnFamilies(true)
	dbOpts.SetCompression(gorocksdb.LZ4Compression)
	dbOpts.SetWALRecoveryMode(gorocksdb.PointInTimeRecovery) // ensure consistency of WAL recovery

	chanOpts := gorocksdb.NewDefaultOptions()
	chanOpts.SetCompression(gorocksdb.LZ4Compression)

	chanSendOpts := gorocksdb.NewDefaultOptions()
	chanSendOpts.SetCompression(gorocksdb.LZ4Compression)

	chanRecvOpts := gorocksdb.NewDefaultOptions()
	chanRecvOpts.SetCompression(gorocksdb.LZ4Compression)

	chanTimeOpts := gorocksdb.NewDefaultOptions()
	chanTimeOpts.SetCompression(gorocksdb.LZ4Compression)

	chanProcEndOpts := gorocksdb.NewDefaultOptions()
	chanProcEndOpts.SetCompression(gorocksdb.LZ4Compression)

	chanBufferOpts := gorocksdb.NewDefaultOptions()
	chanBufferOpts.SetCompression(gorocksdb.LZ4Compression)

	bselOpts := gorocksdb.NewDefaultOptions()
	bselOpts.SetCompression(gorocksdb.LZ4Compression)

	uselOpts := gorocksdb.NewDefaultOptions()
	uselOpts.SetCompression(gorocksdb.LZ4Compression)

	stateOpts := gorocksdb.NewDefaultOptions()
	stateOpts.SetCompression(gorocksdb.LZ4Compression)

	stateLogOpts := gorocksdb.NewDefaultOptions()
	stateLogOpts.SetCompression(gorocksdb.LZ4Compression)

	buffersOpts := gorocksdb.NewDefaultOptions()
	buffersOpts.SetCompression(gorocksdb.LZ4Compression)

	callOpts := gorocksdb.NewDefaultOptions()
	callOpts.SetCompression(gorocksdb.LZ4Compression)

	typesOpts := gorocksdb.NewDefaultOptions()
	apisOpts := gorocksdb.NewDefaultOptions()

	db, cfh, err := gorocksdb.OpenDbColumnFamilies(dbOpts, dbpath, []string{
		"default",
		"channels",
		"chan_send",
		"chan_recv",
		"chan_time",
		"chan_proc_end",
		"chan_buffer",
		"blocked_selects",
		"unblocked_selects",
		"states",
		"state_updates",
		"buffers",
		"types",
		"apis",
		"callOpts",
	}, []*gorocksdb.Options{
		gorocksdb.NewDefaultOptions(),
		chanOpts,
		chanSendOpts,
		chanRecvOpts,
		chanTimeOpts,
		chanProcEndOpts,
		chanBufferOpts,
		bselOpts,
		uselOpts,
		stateOpts,
		stateLogOpts,
		buffersOpts,
		typesOpts,
		apisOpts,
		callOpts,
	})
	if err != nil {
		return nil, err
	}
	if len(cfh) != 15 {
		return nil, fmt.Errorf("wrong number of chfs")
	}
	r := &SelectRuntime{
		db:                    db,
		ro:                    gorocksdb.NewDefaultReadOptions(),
		wo:                    gorocksdb.NewDefaultWriteOptions(),
		cfhDefault:            cfh[0],
		cfhChannels:           cfh[1],
		cfhChanSend:           cfh[2],
		cfhChanRecv:           cfh[3],
		cfhChanTime:           cfh[4],
		cfhChanWorkflowFinish: cfh[5],
		cfhChanBuffer:         cfh[6],
		cfhBlockedThreads:     cfh[7],
		cfhUnblockedThreads:   cfh[8],
		cfhWorkflows:          cfh[9],
		cfhWorkflowEvents:     cfh[10],
		cfhBuffers:            cfh[11],
		cfhTypes:              cfh[12],
		cfhAPIs:               cfh[13],
		cfhCalls:              cfh[14],
		states:                map[string]sLock{},
		done:                  make(chan struct{}),
	}

	r.wo.SetSync(true) // since we manage batches ourselves - there's no need in async writes
	r.ro.SetVerifyChecksums(true)

	// restore last HLC clock time
	d, err := r.db.Get(r.ro, []byte("clock"))
	if err != nil {
		return nil, err
	}
	if d.Size() > 0 {
		err := json.Unmarshal(d.Data(), &r.clock)
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *SelectRuntime) Run(ctx context.Context) error {
	// make batches, process them and schedule for flushing to SSD
	// main bottlenect is the RocksdDB write speed, so we have a room to execute all operations in 1 thread
	// what we have to do is to make sure updates are written in batches. This increases write performance dramatically.
	for {
		select {
		case <-ctx.Done():
			r.cfhDefault.Destroy()
			r.cfhChannels.Destroy()
			r.cfhChanSend.Destroy()
			r.cfhChanRecv.Destroy()
			r.cfhChanBuffer.Destroy()
			r.cfhChanTime.Destroy()
			r.cfhChanWorkflowFinish.Destroy()
			r.cfhBlockedThreads.Destroy()
			r.cfhUnblockedThreads.Destroy()
			r.cfhWorkflows.Destroy()
			r.cfhWorkflowEvents.Destroy()
			r.cfhBuffers.Destroy()
			r.cfhTypes.Destroy()
			r.cfhAPIs.Destroy()
			r.cfhCalls.Destroy()
			r.db.Close()
			return nil
		default:
			// fetch accumulated batch of updates
			r.batchMu.Lock()
			done := r.done
			r.done = make(chan struct{})
			updates := r.updates
			r.updates = nil
			newChans := r.newChannels
			r.newChannels = nil // TODO: create new channels?
			r.batchMu.Unlock()

			// https://rocksdb.org/blog/2015/02/27/write-batch-with-index.html
			r.wb = gorocksdb.NewWriteBatchWithIndex(r.db, r.ro, r.wo)

			for _, c := range newChans { // upsert channels
				oldChan := r.getOrCreateChan(c.ID)
				if oldChan.ID == "" {
					r.wb.PutCF(r.cfhChannels, []byte(c.ID), Marshal(c))
				}
			}

			for _, u := range updates {
				// handle closed channels
				for _, cID := range u.ChannelsToClose {
					ch := r.getOrCreateChan(cID)
					if ch.Closed {
						continue // no need to close channel twice
					}
					// if channel was closed - we need to unblock all selects that were sending/receiving on this channel
					toUnblock := append(r.all(r.cfhChanRecv, append([]byte(cID), 0)), r.all(r.cfhChanSend, append([]byte(cID), 0))...)
					for _, sel := range toUnblock {
						t2 := r.mustGetBlockedThread(sel.BlockedAt)
						t2.Select.Closed = true
						t2.Callback = t2.Select.Cases[sel.Case].Callback
						t2.Select.UnblockedCase = sel.Case
						r.unblockThread(t2)
					}
					ch.Closed = true
					r.wb.PutCF(r.cfhChannels, []byte(cID), Marshal(&ch))
				}

				// clean up unblocked events that were already processes
				if u.UnblockedThread != nil {
					r.wb.DeleteCF(r.cfhUnblockedThreads, IndexStrInt(u.Workflow.Service, u.UnblockedThread.UnblockedAt))
				}

				// process workflow update
				if u.Workflow != nil {
					now := r.clock.Now()
					u.Workflow.UpdatedAt = now
					r.wb.PutCF(r.cfhWorkflowEvents, IndexInt(now), Marshal(&WorkflowEvent{
						Workflow: u.Workflow,
						Thread:   u.UnblockedThread,
					}))
					r.wb.PutCF(r.cfhWorkflows, []byte(u.Workflow.ID), Marshal(u.Workflow))

					if u.Workflow.Status == Workflow_Finished {
						// notify workflow that was blocked
						s := r.getCallSelect(u.Workflow.ID)
						if s != nil {
							t := r.mustGetBlockedThread(s.BlockedAt)
							t.Call.Output = u.Workflow.Output
							r.unblockThread(t)
						}

						// unblock all threads of this workflow to avoid zombie selects/calls
						for _, t := range u.Workflow.Threads {
							bt := r.getBlockedThread(t.BlockedAt)
							if bt != nil {
								r.unblockThread(t)
								continue
							}
						}
					}

					for _, sel := range u.ThreadsToBlock {
						r.tryBlockThread(sel)
					}
				}
			}

			// unblock threads waiting for <-time.After()
			r.handleTick()

			r.wb.Put([]byte("clock"), r.clock.Data()) // save time to be able to check for clock skew on startup.
			err := r.wb.Write()
			if err != nil {
				panic(err)
			}
			r.wb.Destroy()
			close(done) // callback that operations were written

			if len(r.updates) == 0 { // reduce idle load. If there were no new updates buffered - we should simply wait for them to arrive
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (r *SelectRuntime) lockWorkflow(ctx context.Context, id string, duration time.Duration) *sLock {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			lid := r.trylockWorkflow(id, duration)
			if lid != nil {
				return lid
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

var lockSeq uint64 = 1 // lockIDs generator for locking state

func (r *SelectRuntime) trylockWorkflow(id string, duration time.Duration) *sLock {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	old := r.states[id]
	now := time.Now()
	if now.After(old.expiresAt) {
		lid := atomic.AddUint64(&lockSeq, 1)
		slock := sLock{id: lid, expiresAt: now.Add(duration)}
		r.states[id] = slock
		return &slock
	}
	return nil
}

func (r *SelectRuntime) extendWorkflowLock(id string, lid uint64, duration time.Duration) bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	old := r.states[id]
	now := time.Now()
	if old.id != lid {
		return false
	}
	if !now.After(old.expiresAt) {
		r.states[id] = sLock{id: lid, expiresAt: now.Add(duration)}
		return true
	}
	delete(r.states, id) // delete expired lock
	return false
}

func (r *SelectRuntime) unlockWorkflow(id string, lid uint64) bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	old := r.states[id]
	now := time.Now()

	// wrong lockID, but expired
	if now.After(old.expiresAt) {
		delete(r.states, id)
		return false
	}

	// wrong lockID, but not expired
	if old.id != lid {
		return false
	}

	// lockID is correct, lock not expired
	delete(r.states, id)
	return true
}

func (r *SelectRuntime) getOrCreateChan(id string) Channel {
	item, err := r.wb.GetCF(r.cfhChannels, []byte(id))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		return Channel{}
	}
	var s Channel
	err = proto.Unmarshal(item.Data(), &s)
	if err != nil {
		panic(err)
	}
	item.Free()
	return s
}

func (r *SelectRuntime) dbGetChan(id string) *Channel {
	item, err := r.db.GetCF(r.ro, r.cfhChannels, []byte(id))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		return nil
	}
	var s Channel
	err = proto.Unmarshal(item.Data(), &s)
	if err != nil {
		panic(err)
	}
	item.Free()
	return &s
}

func (r *SelectRuntime) mustGetBlockedThread(blockedAt uint64) *Thread {
	item, err := r.wb.GetCF(r.cfhBlockedThreads, IndexInt(blockedAt))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		panic("no select for send/recv/time")
	}
	var t Thread
	err = proto.Unmarshal(item.Data(), &t)
	if err != nil {
		panic(err)
	}
	item.Free()
	return &t
}

func (r *SelectRuntime) getBlockedThread(blockedAt uint64) *Thread {
	item, err := r.wb.GetCF(r.cfhBlockedThreads, IndexInt(blockedAt))
	if err != nil {
		panic(err)
	}
	if !item.Exists() {
		return nil
	}
	var t Thread
	err = proto.Unmarshal(item.Data(), &t)
	if err != nil {
		panic(err)
	}
	item.Free()
	return &t
}

// remove select from index lookup
func (r *SelectRuntime) unblockThread(t *Thread) {
	switch {
	case t.Call != nil:
		r.wb.DeleteCF(r.cfhCalls, []byte(t.Call.ID))
	case t.Select != nil:
		for _, c := range t.Select.Cases {
			switch c.Op {
			case Case_Time:
				r.wb.DeleteCF(r.cfhChanTime, IndexIntInt(c.Time, t.BlockedAt))
			case Case_Send:
				r.wb.DeleteCF(r.cfhChanSend, IndexStrInt(c.Chan, t.BlockedAt))
			case Case_Recv:
				r.wb.DeleteCF(r.cfhChanRecv, IndexStrInt(c.Chan, t.BlockedAt))
			}
		}
	default:
		panic("unexpected thread state. request validation broken")
	}
	r.wb.DeleteCF(r.cfhBlockedThreads, IndexInt(t.BlockedAt))
	t.UnblockedAt = r.clock.Now()
	r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
}

// add select to index lookup
func (r *SelectRuntime) blockThread(t *Thread) {
	t.BlockedAt = r.clock.Now()
	switch {
	case t.Call != nil:
		r.wb.PutCF(r.cfhCalls, []byte(t.Call.ID), Marshal(&ChanSelect{
			BlockedAt: t.BlockedAt,
		}))
	case t.Select != nil:
		for i, c := range t.Select.Cases {
			switch c.Op {
			case Case_Send:
				r.wb.PutCF(r.cfhChanSend, IndexStrInt(c.Chan, t.BlockedAt), Marshal(&ChanSelect{
					BlockedAt: t.BlockedAt,
					Case:      uint64(i),
				}))
			case Case_Recv:
				r.wb.PutCF(r.cfhChanRecv, IndexStrInt(c.Chan, t.BlockedAt), Marshal(&ChanSelect{
					BlockedAt: t.BlockedAt,
					Case:      uint64(i),
				}))
			case Case_Time:
				r.wb.PutCF(r.cfhChanTime, IndexIntInt(c.Time, t.BlockedAt), Marshal(&ChanSelect{
					BlockedAt: t.BlockedAt,
					Case:      uint64(i),
				}))
			}
		}
	default:
		panic("unexpected thread state. request validation broken")
	}
	r.wb.PutCF(r.cfhBlockedThreads, IndexInt(t.BlockedAt), Marshal(t))
}

// unblock selects waiting for time condition (i.e. <-time.After)
func (r *SelectRuntime) handleTick() {
	baseIt := r.db.NewIteratorCF(r.ro, r.cfhChanTime)
	it := r.wb.NewBaseIterator(baseIt)
	defer it.Close()
	endTime := IndexInt(uint64(time.Now().Unix()))
	var toUnblock []*Thread
	for it.SeekToFirst(); it.ValidForPrefix(endTime); it.Next() {
		item, err := r.wb.GetCF(r.cfhChanTime, it.Key().Data())
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
		s2 := r.mustGetBlockedThread(cs.BlockedAt)
		s2.Select.UnblockedCase = cs.Case
		s2.Callback = s2.Select.Cases[cs.Case].Callback
		toUnblock = append(toUnblock, s2)
		it.Key().Free()
		it.Value().Free()
	}
	if it.Err() != nil {
		panic(it.Err())
	}
	for _, s := range toUnblock { // don't modify while iterating
		r.unblockThread(s) // will also remove time record
	}
}

// workflow select and write results to lookup index and wb
func (r *SelectRuntime) tryBlockThread(t *Thread) {
	switch {
	default:
		panic(fmt.Sprintf("unexpected thread: %v", t))

	// This is a use-case when thread is unblocked manually by caller
	case t.Status == Thread_Unblocked:
		t.UnblockedAt = r.clock.Now()
		r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
		return

	// Thread is blocked by call operation
	// It will be unblocked when the process that we are calling is finished
	case t.Call != nil:
		r.blockThread(t)
		return

	// Try to unblock thread immediately
	case t.Select != nil:
		for i, c := range t.Select.Cases {
			switch c.Op {

			// Default case - unblock thread immediately
			case Case_Default:
				if i != len(t.Select.Cases)-1 {
					panic("default case is not the last one. request validation is broken")
				}
				t.Callback = c.Callback
				t.Select.UnblockedCase = uint64(i)
				t.UnblockedAt = r.clock.Now()
				r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))

			case Case_Time:
				// unblock only if unblock time has already passed
				if c.Time <= uint64(time.Now().Unix()) {
					t.Callback = c.Callback
					t.Select.UnblockedCase = uint64(i)
					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
				}

			case Case_Send:
				// unblock if channel is closed
				ch := r.getOrCreateChan(c.Chan)
				if ch.Closed {
					t.Select.Closed = true
					t.Callback = c.Callback
					t.Select.UnblockedCase = uint64(i)
					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					return
				}

				// unblock if channel is buffered and buffer is not full
				if ch.BufMaxSize != 0 && ch.BufSize < ch.BufMaxSize {
					t.Callback = c.Callback
					t.Select.UnblockedCase = uint64(i)
					t.UnblockedAt = r.clock.Now()
					bufData := &BufData{
						Chan:  c.Chan,
						Data:  c.Data,
						Clock: t.UnblockedAt,
					}
					r.wb.PutCF(r.cfhBuffers, IndexInt(t.UnblockedAt), Marshal(bufData)) // save buffer to disk
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					ch.BufSize++
					r.wb.PutCF(r.cfhChannels, []byte(c.Chan), Marshal(&ch))
					return
				}

				// unblock if we have someone receiving on this channel
				p := r.getFirst(r.cfhChanRecv, append([]byte(c.Chan), 0))
				if p != nil {
					s2 := r.mustGetBlockedThread(p.BlockedAt)

					t.Callback = c.Callback
					t.Select.UnblockedCase = uint64(i)

					s2.Callback = s2.Select.Cases[p.Case].Callback
					s2.Select.RecvData = c.Data
					s2.Select.UnblockedCase = p.Case

					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					r.unblockThread(s2)
					return
				}

			case Case_Recv:
				// unblock if channel is closed
				ch := r.getOrCreateChan(c.Chan)
				if ch.Closed && ch.BufSize == 0 {
					t.Select.Closed = true
					t.Callback = c.Callback
					t.Select.UnblockedCase = uint64(i)
					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					return
				}

				// unblock if channel is buffered and buffer is not empty
				if ch.BufMaxSize != 0 && ch.BufSize != 0 {
					t.Callback = c.Callback
					t.Select.UnblockedCase = uint64(i)
					bufData := r.getFirstBuf(append([]byte(c.Chan), 0))
					t.Select.RecvData = bufData.Data
					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					r.wb.DeleteCF(r.cfhBuffers, IndexInt(bufData.Clock))
					ch.BufSize--

					// if buffer was full we should also unblock select that was blocked sending on this full buffered channel
					if ch.BufSize == ch.BufMaxSize {
						p := r.getFirst(r.cfhChanSend, append([]byte(c.Chan), 0))
						if p != nil {
							s2 := r.mustGetBlockedThread(p.BlockedAt)
							s2.Callback = c.Callback
							s2.Select.UnblockedCase = uint64(i)
							s2.UnblockedAt = r.clock.Now()
							bufData := &BufData{
								Chan:  c.Chan,
								Data:  c.Data,
								Clock: s2.UnblockedAt,
							}
							r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(s2.Service, s2.UnblockedAt), Marshal(t))
							r.wb.PutCF(r.cfhBuffers, IndexInt(s2.UnblockedAt), Marshal(bufData)) // save buffer to disk
							ch.BufSize++
						}
					}
					r.wb.PutCF(r.cfhChannels, []byte(c.Chan), Marshal(&ch))
					return
				}

				// unblock if we have someone sending on this channel
				p := r.getFirst(r.cfhChanSend, append([]byte(c.Chan), 0))
				if p != nil { // unblock if we have someone to send on this channel
					s2 := r.mustGetBlockedThread(p.BlockedAt)
					// recv from other select
					t.Callback = c.Callback
					t.Select.UnblockedCase = uint64(i)
					t.Select.RecvData = s2.Select.Cases[p.Case].Data

					s2.Callback = s2.Select.Cases[p.Case].Callback
					s2.Select.UnblockedCase = p.Case

					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))

					r.unblockThread(s2)
					return
				}
			default:
				panic("unexpected case op. request validation is broken")
			}
		}
		// if none of the cases fired - we have to block this thread
		r.blockThread(t)
	}
}
