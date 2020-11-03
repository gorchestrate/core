package gorocksdb

// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// WriteBatch is a batching of Puts, Merges and Deletes.
type WriteBatchWithIndex struct {
	c  *C.rocksdb_writebatch_wi_t
	db *DB
	ro *ReadOptions
	wo *WriteOptions
}

// NewWriteBatch create a WriteBatch object.
func NewWriteBatchWithIndex(db *DB, ro *ReadOptions, wo *WriteOptions) *WriteBatchWithIndex {
	return &WriteBatchWithIndex{
		c:  C.rocksdb_writebatch_wi_create(C.size_t(0), boolToChar(true)),
		db: db,
		ro: ro,
		wo: wo,
	}
}

// Put queues a key-value pair.
func (wb *WriteBatchWithIndex) Write() error {
	var cErr *C.char
	C.rocksdb_write_writebatch_wi(wb.db.c, wb.wo.c, wb.c, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Put queues a key-value pair.
func (wb *WriteBatchWithIndex) Put(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_wi_put(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// // PutCF queues a key-value pair in a column family.
func (wb *WriteBatchWithIndex) PutCF(cf *ColumnFamilyHandle, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_wi_put_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Delete queues a deletion of the data at key.
func (wb *WriteBatchWithIndex) Delete(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_wi_delete(wb.c, cKey, C.size_t(len(key)))
}

// // DeleteCF queues a deletion of the data at key in a column family.
func (wb *WriteBatchWithIndex) DeleteCF(cf *ColumnFamilyHandle, key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_wi_delete_cf(wb.c, cf.c, cKey, C.size_t(len(key)))
}

// Put queues a key-value pair.
func (wb *WriteBatchWithIndex) Get(db DB, opts ReadOptions, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	cValue := C.rocksdb_writebatch_wi_get_from_batch_and_db(wb.c, db.c, opts.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

// Put queues a key-value pair.
func (wb *WriteBatchWithIndex) GetCF(cf *ColumnFamilyHandle, key []byte) (*Slice, error) {
	var (
		cErr    *C.char
		cValLen C.size_t
		cKey    = byteToChar(key)
	)
	cValue := C.rocksdb_writebatch_wi_get_from_batch_and_db_cf(wb.c, wb.db.c, wb.ro.c, cf.c, cKey, C.size_t(len(key)), &cValLen, &cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewSlice(cValue, cValLen), nil
}

// // Append a blob of arbitrary size to the records in this batch.
// func (wb *WriteBatch) PutLogData(blob []byte) {
// 	cBlob := byteToChar(blob)
// 	C.rocksdb_writebatch_put_log_data(wb.c, cBlob, C.size_t(len(blob)))
// }

// // Merge queues a merge of "value" with the existing value of "key".
// func (wb *WriteBatch) Merge(key, value []byte) {
// 	cKey := byteToChar(key)
// 	cValue := byteToChar(value)
// 	C.rocksdb_writebatch_merge(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
// }

// // MergeCF queues a merge of "value" with the existing value of "key" in a
// // column family.
// func (wb *WriteBatch) MergeCF(cf *ColumnFamilyHandle, key, value []byte) {
// 	cKey := byteToChar(key)
// 	cValue := byteToChar(value)
// 	C.rocksdb_writebatch_merge_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
// }

// // DeleteRange deletes keys that are between [startKey, endKey)
// func (wb *WriteBatch) DeleteRange(startKey []byte, endKey []byte) {
// 	cStartKey := byteToChar(startKey)
// 	cEndKey := byteToChar(endKey)
// 	C.rocksdb_writebatch_delete_range(wb.c, cStartKey, C.size_t(len(startKey)), cEndKey, C.size_t(len(endKey)))
// }

// // DeleteRangeCF deletes keys that are between [startKey, endKey) and
// // belong to a given column family
// func (wb *WriteBatch) DeleteRangeCF(cf *ColumnFamilyHandle, startKey []byte, endKey []byte) {
// 	cStartKey := byteToChar(startKey)
// 	cEndKey := byteToChar(endKey)
// 	C.rocksdb_writebatch_delete_range_cf(wb.c, cf.c, cStartKey, C.size_t(len(startKey)), cEndKey, C.size_t(len(endKey)))
// }

// // Data returns the serialized version of this batch.
// func (wb *WriteBatch) Data() []byte {
// 	var cSize C.size_t
// 	cValue := C.rocksdb_writebatch_data(wb.c, &cSize)
// 	return charToByte(cValue, cSize)
// }

// // Count returns the number of updates in the batch.
// func (wb *WriteBatch) Count() int {
// 	return int(C.rocksdb_writebatch_count(wb.c))
// }

// // NewIterator returns a iterator to iterate over the records in the batch.
// func (wb *WriteBatch) NewIterator() *WriteBatchIterator {
// 	data := wb.Data()
// 	if len(data) < 8+4 {
// 		return &WriteBatchIterator{}
// 	}
// 	return &WriteBatchIterator{data: data[12:]}
// }

// // Clear removes all the enqueued Put and Deletes.
// func (wb *WriteBatch) Clear() {
// 	C.rocksdb_writebatch_clear(wb.c)
// }

// Destroy deallocates the WriteBatch object.
func (wb *WriteBatchWithIndex) Destroy() {
	C.rocksdb_writebatch_wi_destroy(wb.c)
	wb.c = nil
}

type WriteBatchBaseIterator struct {
	c *C.rocksdb_writebatch_wi_t
}

// NewNativeWriteBatchBaseIterator create a WriteBatchBaseIterator object.
func NewNativeWriteBatchBaseIterator(c *C.rocksdb_writebatch_wi_t) *WriteBatchBaseIterator {
	return &WriteBatchBaseIterator{c}
}

// Destroy deallocates the WriteBatch object.
func (wb *WriteBatchWithIndex) NewBaseIterator(base *Iterator) *Iterator {
	return NewNativeIterator(unsafe.Pointer(C.rocksdb_writebatch_wi_create_iterator_with_base(wb.c, base.c)))
}

// Destroy deallocates the WriteBatch object.
func (wb *WriteBatchWithIndex) NewBaseIteratorCF(cfh ColumnFamilyHandle, base *Iterator) *Iterator {
	return NewNativeIterator(unsafe.Pointer(C.rocksdb_writebatch_wi_create_iterator_with_base_cf(wb.c, base.c, cfh.c)))
}
