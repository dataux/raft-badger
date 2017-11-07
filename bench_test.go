package raftbadger

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hashicorp/raft/bench"
)

/*
pkg: github.com/bsm/raft-badger
BenchmarkStore_FirstIndex-4    	  300000	      3675 ns/op
BenchmarkStore_LastIndex-4     	  300000	      4031 ns/op
BenchmarkStore_GetLog-4        	   50000	     29795 ns/op
BenchmarkStore_StoreLog-4      	       5	 201922200 ns/op
BenchmarkStore_StoreLogs-4     	     100	  12640519 ns/op
BenchmarkStore_DeleteRange-4   	     300	   4994422 ns/op
BenchmarkStore_Set-4           	     300	   4291782 ns/op
BenchmarkStore_Get-4           	 2000000	       897 ns/op
BenchmarkStore_SetUint64-4     	     300	   4163580 ns/op
BenchmarkStore_GetUint64-4     	 2000000	       888 ns/op


pkg: github.com/hashicorp/raft-boltdb
BenchmarkBoltStore_FirstIndex-4    	 2000000	       881 ns/op
BenchmarkBoltStore_LastIndex-4     	 2000000	       862 ns/op
BenchmarkBoltStore_GetLog-4        	  500000	      2940 ns/op
BenchmarkBoltStore_StoreLog-4      	     300	   3831025 ns/op
BenchmarkBoltStore_StoreLogs-4     	     300	   3933017 ns/op
BenchmarkBoltStore_DeleteRange-4   	     500	   3833753 ns/op
BenchmarkBoltStore_Set-4           	     300	   4061328 ns/op
BenchmarkBoltStore_Get-4           	 1000000	      1232 ns/op
BenchmarkBoltStore_SetUint64-4     	     300	   3795993 ns/op
BenchmarkBoltStore_GetUint64-4     	 1000000	      1075 ns/op

*/
var storePath = "raftbadger"

func testStore(t testing.TB) *store {

	os.RemoveAll(storePath)

	dir, err := ioutil.TempDir("", storePath)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Successfully creates and returns a store
	store, err := newStore(dir, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	return store
}

func BenchmarkStore_FirstIndex(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.FirstIndex(b, store)
}

func BenchmarkStore_LastIndex(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.LastIndex(b, store)
}

func BenchmarkStore_GetLog(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.GetLog(b, store)
}

func BenchmarkStore_StoreLog(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.StoreLog(b, store)
}

func BenchmarkStore_StoreLogs(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.StoreLogs(b, store)
}

func BenchmarkStore_DeleteRange(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.DeleteRange(b, store)
}

func BenchmarkStore_Set(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.Set(b, store)
}

func BenchmarkStore_Get(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.Get(b, store)
}

func BenchmarkStore_SetUint64(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.SetUint64(b, store)
}

func BenchmarkStore_GetUint64(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.GetUint64(b, store)
}
