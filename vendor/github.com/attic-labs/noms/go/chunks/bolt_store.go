// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/boltdb/bolt"
	humanize "github.com/dustin/go-humanize"
	"github.com/golang/snappy"
	flag "github.com/juju/gnuflag"
)

/*
const (
	rootKeyConst     = "/root"
	versionKeyConst  = "/vers"
	chunkPrefixConst = "/chunk/"
)
*/

type BoltStoreFlags struct {
	maxFileHandles int
	dumpStats      bool
}

var (
	ldbFlagsBolt        = BoltStoreFlags{24, false}
	flagsRegisteredBolt = false
	ErrNotFound         = errors.New("boltdb: not found")
)

func RegisterBoltFlags(flags *flag.FlagSet) {
	if !flagsRegisteredBolt {
		flagsRegisteredBolt = true
		flags.IntVar(&ldbFlagsBolt.maxFileHandles, "ldb-max-file-handles", 24, "max number of open file handles")
		flags.BoolVar(&ldbFlagsBolt.dumpStats, "ldb-dump-stats", false, "print get/has/put counts on close")
	}
}

func NewBoltStoreUseFlags(dir, ns string) *BoltStore {
	fmt.Println("bolt_store NewBoltStoreUseFlags")
	return newBoltStore(newBoltBackingStore(dir, ldbFlags.dumpStats), []byte(ns), true)
}

func newBoltStore(store *internalBoltStore, ns []byte, closeBackingStore bool) *BoltStore {
	copyNsAndAppend := func(suffix string) (out []byte) {
		out = make([]byte, len(ns)+len(suffix))
		copy(out[copy(out, ns):], []byte(suffix))
		return
	}

	nslen := len(ns)
	d.PanicIfTrue(nslen == 0)
	if nslen < 1 {
		fmt.Println("bolt_store newBoltStore ns length is 0")
	}

	fmt.Println("bolt_store newBoltStore = ", string(ns[:nslen]))
	store.bucketName = ns
	return &BoltStore{
		internalBoltStore: store,
		rootKey:           copyNsAndAppend(rootKeyConst),
		versionKey:        copyNsAndAppend(versionKeyConst),
		chunkPrefix:       copyNsAndAppend(chunkPrefixConst),
		closeBackingStore: closeBackingStore,
	}
}

type BoltStore struct {
	*internalBoltStore
	rootKey           []byte
	versionKey        []byte
	chunkPrefix       []byte
	closeBackingStore bool
	versionSetOnce    sync.Once
}

func (l *BoltStore) Root() hash.Hash {
	d.PanicIfFalse(l.internalBoltStore != nil)
	return l.rootByKey(l.rootKey)
}

func (l *BoltStore) UpdateRoot(current, last hash.Hash) bool {
	d.PanicIfFalse(l.internalBoltStore != nil)
	l.versionSetOnce.Do(l.setVersIfUnset)
	return l.updateRootByKey(l.rootKey, current, last)
}

func (l *BoltStore) Get(ref hash.Hash) Chunk {
	d.PanicIfFalse(l.internalBoltStore != nil)
	return l.getByKey(l.toChunkKey(ref), ref)
}

func (l *BoltStore) GetMany(hashes []hash.Hash) (batch []Chunk) {
	batch = make([]Chunk, len(hashes))
	for i, h := range hashes {
		batch[i] = l.Get(h)
	}
	return
}

func (l *BoltStore) Has(ref hash.Hash) bool {
	d.PanicIfFalse(l.internalBoltStore != nil)
	return l.hasByKey(l.toChunkKey(ref))
}

func (l *BoltStore) Version() string {
	d.PanicIfFalse(l.internalBoltStore != nil)
	return l.versByKey(l.versionKey)
}

func (l *BoltStore) Put(c Chunk) {
	d.PanicIfFalse(l.internalBoltStore != nil)
	l.versionSetOnce.Do(l.setVersIfUnset)
	l.putByKey(l.toChunkKey(c.Hash()), c)
}

func (l *BoltStore) PutMany(chunks []Chunk) (e BackpressureError) {
	d.PanicIfFalse(l.internalBoltStore != nil)
	l.versionSetOnce.Do(l.setVersIfUnset)
	//numBytes := 0
	//b := new(leveldb.Batch)
	for _, c := range chunks {
		//data := snappy.Encode(nil, c.Data())
		//numBytes += len(data)
		l.Put(c)
	}
	//l.putBatch(b, numBytes)
	return
}

func (l *BoltStore) Flush() {}

func (l *BoltStore) Close() error {
	if l.closeBackingStore {
		l.internalBoltStore.Close()
	}
	l.internalBoltStore = nil
	return nil
}

func (l *BoltStore) toChunkKey(h hash.Hash) []byte {
	out := make([]byte, len(l.chunkPrefix), len(l.chunkPrefix)+hash.ByteLen)
	copy(out, l.chunkPrefix)
	return append(out, h[:]...)
}

func (l *BoltStore) setVersIfUnset() {
	/*
		exists, err := l.db.Has(l.versionKey, nil)
		d.Chk.NoError(err)
		if !exists {
			l.setVersByKey(l.versionKey)
		}
	*/
	l.setVersByKey(l.versionKey)
}

type internalBoltStore struct {
	db                                     *bolt.DB
	mu                                     sync.Mutex
	getCount, hasCount, putCount, putBytes int64
	dumpStats                              bool
	bucketName                             []byte
}

func newBoltBackingStore(dir string, dumpStats bool) *internalBoltStore {
	d.PanicIfTrue(dir == "")
	d.PanicIfError(os.MkdirAll(dir, 0700))
	filename := dir + "/bolt.db"
	db, err := bolt.Open(filename, 0644, nil)
	d.Chk.NoError(err, "opening internalBoltStore in %s", dir)
	return &internalBoltStore{
		db:        db,
		dumpStats: dumpStats,
	}
}

func (l *internalBoltStore) rootByKey(key []byte) hash.Hash {
	// val, err := l.db.Get(key, nil)
	/*
		val, err := l.viewBolt(key)
		fmt.Println("val = ", val)
		fmt.Println("err = ", err)
		return hash.Hash{}
	*/

	val, err := l.viewBolt(key)

	if len(val) == 0 {
		return hash.Hash{}
	}
	d.Chk.NoError(err)

	return hash.Parse(string(val))

}

func (l *internalBoltStore) updateRootByKey(key []byte, current, last hash.Hash) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last != l.rootByKey(key) {
		return false
	}

	// Sync: true write option should fsync memtable data to disk
	// err := l.db.Put(key, []byte(current.String()), &opt.WriteOptions{Sync: true})

	// need to add in the ability for Bolt sync option
	err := l.updateBolt(key, []byte(current.String()))

	d.Chk.NoError(err)
	return true
}

func (l *internalBoltStore) getByKey(key []byte, ref hash.Hash) Chunk {
	//compressed, err := l.db.Get(key, nil)
	compressed, err := l.viewBolt(key)
	l.getCount++
	if err == ErrNotFound {
		return EmptyChunk
	}
	d.Chk.NoError(err)
	data, err := snappy.Decode(nil, compressed)
	d.Chk.NoError(err)
	return NewChunkWithHash(ref, data)
}

func (l *internalBoltStore) hasByKey(key []byte) bool {
	// exists, err := l.db.Has(key, &opt.ReadOptions{DontFillCache: true}) // This isn't really a "read", so don't signal the cache to treat it as one.
	exists, err := l.hasBolt(key)
	if err == ErrNotFound {
		return false
	}
	d.Chk.NoError(err)
	l.hasCount++
	return exists
}

func (l *internalBoltStore) versByKey(key []byte) string {
	//val, err := l.db.Get(key, nil)
	val, err := l.viewBolt(key)
	if err == ErrNotFound {
		return constants.NomsVersion
	}
	d.Chk.NoError(err)
	return string(val)
}

func (l *internalBoltStore) setVersByKey(key []byte) {
	//err := l.db.Put(key, []byte(constants.NomsVersion), nil)

	err := l.updateBolt(key, []byte(constants.NomsVersion))

	d.Chk.NoError(err)
}

func (l *internalBoltStore) updateBolt(key []byte, value []byte) error {

	// store some data
	err := l.db.Update(func(tx *bolt.Tx) error {
		//bucket, err := tx.CreateBucketIfNotExists([]byte("sam"))
		bucket, err := tx.CreateBucketIfNotExists(l.bucketName)
		if err != nil {
			return err
		}

		err = bucket.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (l *internalBoltStore) viewBolt(key []byte) (val []byte, err error) {

	if l.bucketName == nil {
		fmt.Println("bolt_store viewBolt bucketName is nil")
	}

	byteary := l.bucketName
	n := len(byteary)

	if n < 1 {
		fmt.Println("bolt_store viewBolt bucketName length is 0")
	}

	s := string(byteary[:n])
	fmt.Println("bolt_store viewBolt bucketName = ", n, s)

	// retrieve the data
	err = l.db.View(func(tx *bolt.Tx) error {
		// bucket := tx.Bucket([]byte("sam"))
		bucket := tx.Bucket(l.bucketName)

		if bucket == nil {
			return fmt.Errorf("Bucket not found!")
		}

		val = bucket.Get(key)
		return nil
	})

	if err != nil {
		return nil, ErrNotFound
	}

	return val, nil
}

func (l *internalBoltStore) hasBolt(key []byte) (bool, error) {
	val, err := l.viewBolt(key)
	if err != nil {
		return false, err
	}
	size := len(val)
	if size > 0 {
		return true, err
	}
	return false, err
}

func (l *internalBoltStore) putByKey(key []byte, c Chunk) {
	value := snappy.Encode(nil, c.Data())

	// This was the way with leveldb
	//	err := l.db.Put(key, data, nil)

	err := l.updateBolt(key, value)
	d.Chk.NoError(err)
	l.putCount++
	l.putBytes += int64(len(value))
}

/*
I am pretty sure I will not need to implement this...

func (l *internalBoltStore) putBatch(b *leveldb.Batch, numBytes int) {
		err := l.db.Write(b, nil)
		d.Chk.NoError(err)
		l.putCount += int64(b.Len())
		l.putBytes += int64(numBytes)

	fmt.Println("Currently not yet implemented")
}
*/

func (l *internalBoltStore) Close() error {
	l.db.Close()
	if l.dumpStats {
		fmt.Println("--Bolt Stats--")
		fmt.Println("GetCount: ", l.getCount)
		fmt.Println("HasCount: ", l.hasCount)
		fmt.Println("PutCount: ", l.putCount)
		fmt.Printf("PutSize:   %s (%d/chunk)\n", humanize.Bytes(uint64(l.putCount)), l.putBytes/int64(math.Max(1, float64(l.putCount))))
	}
	return nil
}

func NewBoltStoreFactory(dir string, dumpStats bool) Factory {
	return &BoltStoreFactory{dir, dumpStats, newBoltBackingStore(dir, dumpStats)}
}

func NewBoltStoreFactoryUseFlags(dir string) Factory {
	return NewBoltStoreFactory(dir, ldbFlagsBolt.dumpStats)
}

type BoltStoreFactory struct {
	dir       string
	dumpStats bool
	store     *internalBoltStore
}

func (f *BoltStoreFactory) CreateStore(ns string) ChunkStore {
	d.PanicIfFalse(f.store != nil)
	fmt.Println("bolt_store CreateStore = ", ns)
	return newBoltStore(f.store, []byte(ns), true)
}

func (f *BoltStoreFactory) Shutter() {
	f.store.Close()
	f.store = nil
}
