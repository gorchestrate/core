package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/gorchestrate/cmd/gorocksdb"
	"github.com/spf13/viper"
)

type sLock struct {
	id        uint64
	expiresAt time.Time
}

type SelectRuntime struct {
	// rocksdb stuff
	db                   *gorocksdb.DB
	wo                   *gorocksdb.WriteOptions
	ro                   *gorocksdb.ReadOptions
	wb                   *gorocksdb.WriteBatchWithIndex
	cfhDefault           *gorocksdb.ColumnFamilyHandle
	cfhChannels          *gorocksdb.ColumnFamilyHandle
	cfhChanSend          *gorocksdb.ColumnFamilyHandle
	cfhChanRecv          *gorocksdb.ColumnFamilyHandle
	cfhChanTime          *gorocksdb.ColumnFamilyHandle
	cfhChanProcessFinish *gorocksdb.ColumnFamilyHandle
	cfhChanBuffer        *gorocksdb.ColumnFamilyHandle
	cfhBlockedThreads    *gorocksdb.ColumnFamilyHandle
	cfhUnblockedThreads  *gorocksdb.ColumnFamilyHandle
	cfhProcesses         *gorocksdb.ColumnFamilyHandle
	cfhTypes             *gorocksdb.ColumnFamilyHandle
	cfhAPIs              *gorocksdb.ColumnFamilyHandle
	cfhProcessEvents     *gorocksdb.ColumnFamilyHandle
	cfhBuffers           *gorocksdb.ColumnFamilyHandle
	cfhCalls             *gorocksdb.ColumnFamilyHandle

	// lock states while processing
	stateMu sync.Mutex
	states  map[string]sLock

	// hybrid logical clock used to order records in time, and be able to join states and selects by this time
	clock HLC

	batchMu     sync.Mutex
	done        chan struct{}   // callback for current batch, this will be closed when batch is flushed to DB
	updates     []processUpdate // selects to add
	newChannels []*Channel      // selects to add
}

type processUpdate struct {
	Process         *Process
	UnblockedThread *Thread
	ThreadsToBlock  []*Thread
	ChannelsToClose []string
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
		db:                   db,
		ro:                   gorocksdb.NewDefaultReadOptions(),
		wo:                   gorocksdb.NewDefaultWriteOptions(),
		cfhDefault:           cfh[0],
		cfhChannels:          cfh[1],
		cfhChanSend:          cfh[2],
		cfhChanRecv:          cfh[3],
		cfhChanTime:          cfh[4],
		cfhChanProcessFinish: cfh[5],
		cfhChanBuffer:        cfh[6],
		cfhBlockedThreads:    cfh[7],
		cfhUnblockedThreads:  cfh[8],
		cfhProcesses:         cfh[9],
		cfhProcessEvents:     cfh[10],
		cfhBuffers:           cfh[11],
		cfhTypes:             cfh[12],
		cfhAPIs:              cfh[13],
		cfhCalls:             cfh[14],
		states:               map[string]sLock{},
		done:                 make(chan struct{}),
	}

	r.wo.SetSync(true)
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
	// make batches, process them and schedule to flush
	// since the bottleneck is the RocksdDB write speed we have a room to execute all operations
	// in 1 thread, ensuring linearized consistency
	for {
		select {
		case <-ctx.Done():
			r.cfhDefault.Destroy()
			r.cfhChannels.Destroy()
			r.cfhChanSend.Destroy()
			r.cfhChanRecv.Destroy()
			r.cfhChanBuffer.Destroy()
			r.cfhChanTime.Destroy()
			r.cfhChanProcessFinish.Destroy()
			r.cfhBlockedThreads.Destroy()
			r.cfhUnblockedThreads.Destroy()
			r.cfhProcesses.Destroy()
			r.cfhProcessEvents.Destroy()
			r.cfhBuffers.Destroy()
			r.cfhTypes.Destroy()
			r.cfhAPIs.Destroy()
			r.cfhCalls.Destroy()
			r.db.Close()
			return nil
		default:
			r.batchMu.Lock()
			done := r.done
			r.done = make(chan struct{})
			updates := r.updates
			r.updates = nil
			newChans := r.newChannels
			r.newChannels = nil // TODO: create new channels?
			r.batchMu.Unlock()

			if len(r.updates) == 0 { // reduce idle load
				time.Sleep(time.Millisecond)
			}

			r.wb = gorocksdb.NewWriteBatchWithIndex(r.db, r.ro, r.wo)

			for _, c := range newChans { // Upsert channels
				oldChan := r.getOrCreateChan(c.Id)
				if oldChan.Id == "" {
					r.wb.PutCF(r.cfhChannels, []byte(c.Id), Marshal(c))
				}
			}
			for _, u := range updates {
				for _, cID := range u.ChannelsToClose {
					ch := r.getOrCreateChan(cID)
					if ch.Closed {
						continue // no need to close channel twice
					}
					toUnblock := append(r.all(r.cfhChanRecv, append([]byte(cID), 0)), r.all(r.cfhChanSend, append([]byte(cID), 0))...)
					for _, sel := range toUnblock {
						t2 := r.mustGetBlockedThread(sel.BlockedAt)
						t2.Select.Result = Select_Closed
						t2.ToStatus = t2.Select.Cases[sel.Case].ToStatus
						t2.Select.UnblockedCase = sel.Case
						r.unblockThread(t2)
					}
					ch.Closed = true
					r.wb.PutCF(r.cfhChannels, []byte(cID), Marshal(&ch))
				}
				now := r.clock.Now()
				if u.UnblockedThread != nil {
					r.wb.DeleteCF(r.cfhUnblockedThreads, IndexStrInt(u.UnblockedThread.Service, u.UnblockedThread.UnblockedAt))
				}
				if u.Process == nil {
					continue
				}
				u.Process.UpdatedAt = now
				r.wb.PutCF(r.cfhProcessEvents, IndexInt(now), Marshal(&ProcessEvent{
					Process: u.Process,
					Thread:  u.UnblockedThread,
				}))
				r.wb.PutCF(r.cfhProcesses, []byte(u.Process.Id), Marshal(u.Process))
				if u.Process.Status == Process_Finished {
					s := r.getCallSelect(u.Process.Id)
					if s != nil {
						t := r.mustGetBlockedThread(s.BlockedAt)
						t.Call.Output = u.Process.Output
						r.unblockThread(t)
					}
					for _, t := range u.Process.Threads {
						bt := r.getBlockedThread(t.BlockedAt)
						if bt != nil {
							r.unblockThread(t)
							continue
						}
					}
				}
				for _, sel := range u.ThreadsToBlock {
					r.handleThread(sel)
				}
			}
			r.handleTick()

			r.wb.Put([]byte("clock"), r.clock.Data()) // save time to be able to check for clock skew on startup.
			err := r.wb.Write()
			if err != nil {
				panic(err)
			}
			r.wb.Destroy()
			close(done) // callback that operations were written
		}
	}
}

type channel struct {
	Channel
	Recv   *btree.BTree
	Send   *btree.BTree
	Buffer *btree.BTree
}

type timeSelect struct {
	Process  string
	Select   string
	Service  string
	ToStatus string
	Clock    uint64
	Time     uint64
}

func (c timeSelect) Less(than btree.Item) bool {
	c2 := than.(timeSelect)
	if c.Time != c2.Time {
		return c.Time < c2.Time
	}
	return c.Clock < c2.Clock
}

func (r *SelectRuntime) lockProcess(ctx context.Context, id string, duration time.Duration) *sLock {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			lid := r.trylockProcess(id, duration)
			if lid != nil {
				return lid
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

var lockSeq uint64 = 1 // used to generate lockIDs for locking state

func (r *SelectRuntime) trylockProcess(id string, duration time.Duration) *sLock {
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

func (r *SelectRuntime) extendProcessLock(id string, lid uint64, duration time.Duration) bool {
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

func (r *SelectRuntime) unlockProcess(id string, lid uint64) bool {
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
		r.wb.DeleteCF(r.cfhCalls, []byte(t.Call.Id))
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
func (r *SelectRuntime) addThread(t *Thread) {
	t.BlockedAt = r.clock.Now()
	switch {
	case t.Call != nil:
		r.wb.PutCF(r.cfhCalls, []byte(t.Call.Id), Marshal(&ChanSelect{
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
		s2.Select.Result = Select_OK
		s2.Select.UnblockedCase = cs.Case
		s2.ToStatus = s2.Select.Cases[cs.Case].ToStatus
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

// process select and write results to lookup index and wb
func (r *SelectRuntime) handleThread(t *Thread) {
	switch {
	default:
		panic(fmt.Sprintf("unexpected thread: %v", t))
	case t.Status == Thread_Unblocked:
		t.UnblockedAt = r.clock.Now()
		r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
		return
	case t.Call != nil:
		// If this is call operation - it's atomic in nature and happens at the same time process starts
		// so we can just do nothing and save this thread as blocked
		r.addThread(t)
		return
	case t.Select != nil:
		for i, c := range t.Select.Cases {
			switch c.Op {
			case Case_Default:
				// reached default statement - unblock select
				t.Select.Result = Select_OK
				t.ToStatus = c.ToStatus
				t.Select.UnblockedCase = uint64(i)
				t.UnblockedAt = r.clock.Now()
				r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))

			case Case_Time:
				if c.Time > uint64(time.Now().Unix()) {
					continue
				}
				// select on time in the past - unblock select
				t.Select.Result = Select_OK
				t.ToStatus = c.ToStatus
				t.Select.UnblockedCase = uint64(i)
				t.UnblockedAt = r.clock.Now()
				r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))

			case Case_Send:
				ch := r.getOrCreateChan(c.Chan)
				if ch.Closed { // send on closed channel
					t.Select.Result = Select_Closed
					t.ToStatus = c.ToStatus
					t.Select.UnblockedCase = uint64(i)
					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					return
				}
				p := r.getFirst(r.cfhChanRecv, append([]byte(c.Chan), 0))
				if p == nil {
					if ch.BufSize >= ch.BufMaxSize {
						continue // no one to receive an buffer is full
					}
					t.Select.Result = Select_OK
					t.ToStatus = c.ToStatus
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
					continue
				}
				s2 := r.mustGetBlockedThread(p.BlockedAt)
				// send to other select
				t.Select.Result = Select_OK
				t.ToStatus = c.ToStatus
				t.Select.UnblockedCase = uint64(i)

				s2.Select.Result = Select_OK
				s2.ToStatus = s2.Select.Cases[p.Case].ToStatus
				s2.Select.RecvData = c.Data
				s2.Select.UnblockedCase = p.Case

				t.UnblockedAt = r.clock.Now()
				r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
				r.unblockThread(s2)

			case Case_Recv:
				ch := r.getOrCreateChan(c.Chan)
				if ch.Closed && ch.BufSize == 0 { // all messages received and channel was closed
					t.Select.Result = Select_Closed
					t.ToStatus = c.ToStatus
					t.Select.UnblockedCase = uint64(i)
					t.UnblockedAt = r.clock.Now()
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					return
				}
				p := r.getFirst(r.cfhChanSend, append([]byte(c.Chan), 0))
				if p == nil {
					if ch.BufSize == 0 {
						continue // no one to send an buffer is empty
					}
					t.Select.Result = Select_OK
					t.ToStatus = c.ToStatus
					t.Select.UnblockedCase = uint64(i)
					bufData := r.getFirstBuf(append([]byte(c.Chan), 0))
					t.Select.RecvData = bufData.Data
					t.UnblockedAt = r.clock.Now()
					r.wb.DeleteCF(r.cfhBuffers, IndexInt(bufData.Clock))
					r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))
					ch.BufSize--
					r.wb.PutCF(r.cfhChannels, []byte(c.Chan), Marshal(&ch))
					continue
				}
				s2 := r.mustGetBlockedThread(p.BlockedAt)
				// recv from other select
				t.Select.Result = Select_OK
				t.ToStatus = c.ToStatus
				t.Select.UnblockedCase = uint64(i)
				t.Select.RecvData = s2.Select.Cases[p.Case].Data

				s2.Select.Result = Select_OK
				s2.ToStatus = s2.Select.Cases[p.Case].ToStatus
				s2.Select.UnblockedCase = p.Case

				t.UnblockedAt = r.clock.Now()
				r.wb.PutCF(r.cfhUnblockedThreads, IndexStrInt(t.Service, t.UnblockedAt), Marshal(t))

				r.unblockThread(s2)
			default:
				panic("unexpected case op. request validation broken")
			}
		}
		r.addThread(t)
	}
}
