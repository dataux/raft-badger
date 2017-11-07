package raftbadger

import (
	"errors"
	"io"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

var ErrKeyNotFound = errors.New("not found")

// LogStore combines raft.LogStore and io.Closer
type LogStore interface {
	raft.LogStore
	io.Closer
}

// NewLogStore uses the supplied options to open a log store. badger.DefaultOptions
// will be used, if nil is passed for opt.
func NewLogStore(dir string, opt *badger.Options) (LogStore, error) { return newStore(dir, opt) }

// StableStore combines raft.StableStore and io.Closer
type StableStore interface {
	raft.StableStore
	io.Closer
}

// NewStableStore uses the supplied options to open a stable store. badger.DefaultOptions
// will be used, if nil is passed for opt.
func NewStableStore(dir string, opt *badger.Options) (StableStore, error) {
	return newStore(dir, opt)
}

// --------------------------------------------------------------------

type store struct {
	kv *badger.DB
}

func newStore(dir string, opt *badger.Options) (*store, error) {
	if opt == nil {
		opt = new(badger.Options)
		*opt = badger.DefaultOptions
	}
	opt.Dir = dir
	opt.ValueDir = dir

	if err := os.MkdirAll(opt.Dir, 0777); err != nil {
		return nil, err
	}

	kv, err := badger.Open(*opt)
	if err != nil {
		return nil, err
	}
	return &store{
		kv: kv,
	}, nil
}

// Close is used to gracefully close the connection.
func (s *store) Close() error { return s.kv.Close() }

// FirstIndex returns the first known index from the Raft log.
func (s *store) FirstIndex() (uint64, error) { return s.firstIndex(false) }

// LastIndex returns the last known index from the Raft log.
func (s *store) LastIndex() (uint64, error) { return s.firstIndex(true) }

// GetLog is used to retrieve a log from BoltDB at a given index.
func (s *store) GetLog(idx uint64, log *raft.Log) error {

	txn := s.kv.NewTransaction(false)
	defer txn.Commit(nil)

	item, err := txn.Get(uint64ToBytes(idx))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return raft.ErrLogNotFound
		}
		return err
	}
	val, err := item.Value()
	if val == nil {
		return raft.ErrLogNotFound
	}
	err = gobDecode(val, log)
	//u.Debugf("GetLog idx:%v  %v  log: %+v err=%v", idx, val, log, err)
	return err
}

// StoreLog is used to store a single raft log
func (s *store) StoreLog(log *raft.Log) error {
	buf, err := gobEncode(log)
	if err != nil {
		return err
	}
	defer bufPool.Put(buf)

	txn := s.kv.NewTransaction(true)
	defer txn.Commit(nil)

	return txn.Set(uint64ToBytes(log.Index), buf.Bytes())
}

// StoreLogs is used to store a set of raft logs
func (s *store) StoreLogs(logs []*raft.Log) error {

	for _, log := range logs {
		buf, err := gobEncode(log)
		if err != nil {
			return err
		}
		txn := s.kv.NewTransaction(true)
		err = txn.Set(uint64ToBytes(log.Index), buf.Bytes())
		bufPool.Put(buf)
		if err != nil {
			txn.Discard()
			return nil
		}
		txn.Commit(nil)
	}

	return nil
}

// DeleteRange is used to delete logs within a given range inclusively.
func (s *store) DeleteRange(min, max uint64) error {
	txn := s.kv.NewTransaction(true)
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	defer itr.Close()

	for itr.Seek(uint64ToBytes(min)); itr.Valid(); itr.Next() {
		key := itr.Item().Key()
		if bytesToUint64(key) > max {
			break
		}
		err := txn.Delete(key)
		if err != nil {
			txn.Discard()
			return err
		}
	}

	return txn.Commit(nil)
}

// Set is used to set a key/value set outside of the raft log
func (s *store) Set(k, v []byte) error {
	txn := s.kv.NewTransaction(true)
	defer txn.Commit(nil)
	return txn.Set(k, v)
}

// Get is used to retrieve a value from the k/v store by key
func (s *store) Get(k []byte) ([]byte, error) {

	txn := s.kv.NewTransaction(false)
	defer txn.Commit(nil)

	item, err := txn.Get(k)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	val, err := item.Value()
	if val == nil {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

// SetUint64 is like Set, but handles uint64 values
func (s *store) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (s *store) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

func (s *store) firstIndex(reverse bool) (uint64, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 1
	opt.PrefetchValues = false
	opt.Reverse = reverse

	txn := s.kv.NewTransaction(false)
	defer txn.Commit(nil)

	itr := txn.NewIterator(opt)
	defer itr.Close()

	for itr.Rewind(); itr.Valid(); itr.Next() {
		return bytesToUint64(itr.Item().Key()), nil
	}
	return 0, nil
}
