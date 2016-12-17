// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	humanize "github.com/dustin/go-humanize"
	"github.com/golang/snappy"
	flag "github.com/juju/gnuflag"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	//"github.com/syndtr/goleveldb/leveldb/filter"
	//"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/garyburd/redigo/redis"
)

/*
const (
	rootKeyConst     = "/root"
	versionKeyConst  = "/vers"
	chunkPrefixConst = "/chunk/"
)
*/

type RedisStoreFlags struct {
	maxFileHandles int
	dumpStats      bool
}

var (
	redisFlags           = RedisStoreFlags{24, false}
	redisflagsRegistered = false
)

func RegisterRedisFlags(flags *flag.FlagSet) {
	if !redisflagsRegistered {
		redisflagsRegistered = true
		flags.IntVar(&redisFlags.maxFileHandles, "ldb-max-file-handles", 24, "max number of open file handles")
		flags.BoolVar(&redisFlags.dumpStats, "ldb-dump-stats", false, "print get/has/put counts on close")
	}
}

func NewRedisStoreUseFlags(dir, ns string) *RedisStore {
	return newRedisStore(newRedisBackingStore(dir, redisFlags.maxFileHandles, redisFlags.dumpStats), []byte(ns), true)
}

func NewRedisStore(dir, ns string, maxFileHandles int, dumpStats bool) *RedisStore {
	return newRedisStore(newRedisBackingStore(dir, maxFileHandles, dumpStats), []byte(ns), true)
}

func newRedisStore(store *internalRedisStore, ns []byte, closeBackingStore bool) *RedisStore {
	copyNsAndAppend := func(suffix string) (out []byte) {
		out = make([]byte, len(ns)+len(suffix))
		copy(out[copy(out, ns):], []byte(suffix))
		return
	}
	return &RedisStore{
		internalRedisStore: store,
		rootKey:            copyNsAndAppend(rootKeyConst),
		versionKey:         copyNsAndAppend(versionKeyConst),
		chunkPrefix:        copyNsAndAppend(chunkPrefixConst),
		closeBackingStore:  closeBackingStore,
	}
}

type RedisStore struct {
	*internalRedisStore
	rootKey           []byte
	versionKey        []byte
	chunkPrefix       []byte
	closeBackingStore bool
	versionSetOnce    sync.Once
}

func (l *RedisStore) Root() hash.Hash {
	d.PanicIfFalse(l.internalRedisStore != nil)
	return l.rootByKey(l.rootKey)
}

func (l *RedisStore) UpdateRoot(current, last hash.Hash) bool {
	d.PanicIfFalse(l.internalRedisStore != nil)
	l.versionSetOnce.Do(l.setVersIfUnset)
	return l.updateRootByKey(l.rootKey, current, last)
}

func (l *RedisStore) Get(ref hash.Hash) Chunk {
	d.PanicIfFalse(l.internalRedisStore != nil)
	return l.getByKey(l.toChunkKey(ref), ref)
}

func (l *RedisStore) GetMany(hashes []hash.Hash) (batch []Chunk) {
	batch = make([]Chunk, len(hashes))
	for i, h := range hashes {
		batch[i] = l.Get(h)
	}
	return
}

func (l *RedisStore) Has(ref hash.Hash) bool {
	d.PanicIfFalse(l.internalRedisStore != nil)
	return l.hasByKey(l.toChunkKey(ref))
}

func (l *RedisStore) Version() string {
	d.PanicIfFalse(l.internalRedisStore != nil)
	return l.versByKey(l.versionKey)
}

func (l *RedisStore) Put(c Chunk) {
	d.PanicIfFalse(l.internalRedisStore != nil)
	l.versionSetOnce.Do(l.setVersIfUnset)
	l.putByKey(l.toChunkKey(c.Hash()), c)
}

func (l *RedisStore) PutMany(chunks []Chunk) (e BackpressureError) {
	d.PanicIfFalse(l.internalRedisStore != nil)
	l.versionSetOnce.Do(l.setVersIfUnset)
	for _, c := range chunks {
		l.Put(c)
	}
	return
}

func (l *RedisStore) Flush() {}

func (l *RedisStore) Close() error {
	if l.closeBackingStore {
		l.internalRedisStore.Close()
	}
	l.internalRedisStore = nil
	return nil
}

func (l *RedisStore) toChunkKey(h hash.Hash) []byte {
	out := make([]byte, len(l.chunkPrefix), len(l.chunkPrefix)+hash.ByteLen)
	copy(out, l.chunkPrefix)
	return append(out, h[:]...)
}

func (l *RedisStore) setVersIfUnset() {
	exists, err := l.hexists(l.versionKey)
	d.Chk.NoError(err)
	if !exists {
		l.setVersByKey(l.versionKey)
	}
}

type RedisConfig struct {
	Hostname string
	Port     string
}

func (c *RedisConfig) Connect_string() string {
	connect := fmt.Sprint(c.Hostname, ":", c.Port)
	return connect
}

func NewRedisConfig() *RedisConfig {
	cfg := &RedisConfig{
		Hostname: "localhost",
		Port:     "6379",
	}
	return cfg
}

func getRedisConn() (c redis.Conn) {

	cfg := NewRedisConfig()
	connect_string := cfg.Connect_string()
	c, err := redis.Dial("tcp", connect_string)
	if err != nil {
		panic(err)
	}
	return c
	// defer c.Close()
}

type internalRedisStore struct {
	db                                     redis.Conn
	mu                                     sync.Mutex
	getCount, hasCount, putCount, putBytes int64
	dumpStats                              bool
}

func newRedisBackingStore(dir string, maxFileHandles int, dumpStats bool) *internalRedisStore {
	d.PanicIfTrue(dir == "")
	d.PanicIfError(os.MkdirAll(dir, 0700))

	c := getRedisConn()
	defer c.Close()

	return &internalRedisStore{
		db:        c,
		dumpStats: dumpStats,
	}
}

func (l *internalRedisStore) rootByKey(key []byte) hash.Hash {
	// old way
	// val, err := l.db.Get(key, nil)

	val, err := l.hget(key)

	if len(val) == 0 {
		return hash.Hash{}
	}

	d.Chk.NoError(err)

	return hash.Parse(string(val))
}

func (l *internalRedisStore) updateRootByKey(key []byte, current, last hash.Hash) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last != l.rootByKey(key) {
		return false
	}

	// Sync: true write option should fsync memtable data to disk
	// err := l.db.Put(key, []byte(current.String()))
	err := l.hset(key, []byte(current.String()))

	d.Chk.NoError(err)
	return true
}

func (l *internalRedisStore) getByKey(key []byte, ref hash.Hash) Chunk {
	// old way
	// compressed, err := l.db.Get(key, nil)

	compressed, err := l.hget(key)

	l.getCount++
	if err == errors.ErrNotFound {
		return EmptyChunk
	}
	d.Chk.NoError(err)
	data, err := snappy.Decode(nil, compressed)
	d.Chk.NoError(err)
	return NewChunkWithHash(ref, data)
}

func (l *internalRedisStore) hasByKey(key []byte) bool {
	exists, err := l.hexists(key)
	d.Chk.NoError(err)
	l.hasCount++
	return exists
}

func (l *internalRedisStore) versByKey(key []byte) string {
	// old way
	// val, err := l.db.Get(key, nil)

	val, err := l.hget(key)

	if err == errors.ErrNotFound {
		return constants.NomsVersion
	}
	d.Chk.NoError(err)
	return string(val)
}

func (l *internalRedisStore) setVersByKey(key []byte) {

	// old method
	// err := l.db.Put(key, []byte(constants.NomsVersion), nil)
	err := l.hset(key, []byte(constants.NomsVersion))

	d.Chk.NoError(err)
}

func (l *internalRedisStore) hset(field []byte, value []byte) error {
	c := getRedisConn()
	defer c.Close()

	_, err := c.Do("HSET", "noms-dataset-name", field, value)
	return err
}

func (l *internalRedisStore) hget(field []byte) (value []byte, err error) {

	c := getRedisConn()
	defer c.Close()

	//reply, err := redis.Values(c.Do("MGET", "key1", "key2"))

	//reply, err := redis.Values(l.db.Do("HGET", "noms-dataset-name", field))
	value, err = redis.Bytes(c.Do("HGET", "noms-dataset-name", field))

	// send back fake data at the moment until you convert reply to value
	//value = []byte("That's all folks!!")

	return value, err
}

func (l *internalRedisStore) hexists(field []byte) (bool, error) {

	c := getRedisConn()
	defer c.Close()

	//reply, err := redis.Int(l.db.Do("HGET", "noms-dataset-name", field))
	reply, err := redis.Int(c.Do("HEXISTS", "noms-dataset-name", field))
	// need to do the actual conversion

	if reply == 0 {
		return false, err
	}

	return true, err
}

func (l *internalRedisStore) putByKey(key []byte, c Chunk) {
	data := snappy.Encode(nil, c.Data())

	// old method
	// err := l.db.Put(key, data, nil)
	err := l.hset(key, data)

	d.Chk.NoError(err)
	l.putCount++
	l.putBytes += int64(len(data))
}

func (l *internalRedisStore) putBatch(b *leveldb.Batch, numBytes int) {
	/*
		err := l.db.Write(b, nil)
		d.Chk.NoError(err)
		l.putCount += int64(b.Len())
		l.putBytes += int64(numBytes)
	*/
	fmt.Println("Currently not yet implemented")
}

func (l *internalRedisStore) Close() error {
	l.db.Close()
	if l.dumpStats {
		fmt.Println("--Redis Stats--")
		fmt.Println("GetCount: ", l.getCount)
		fmt.Println("HasCount: ", l.hasCount)
		fmt.Println("PutCount: ", l.putCount)
		fmt.Printf("PutSize:   %s (%d/chunk)\n", humanize.Bytes(uint64(l.putCount)), l.putBytes/int64(math.Max(1, float64(l.putCount))))
	}
	return nil
}

func NewRedisStoreFactory(dir string, maxHandles int, dumpStats bool) Factory {
	return &RedisStoreFactory{dir, maxHandles, dumpStats, newRedisBackingStore(dir, maxHandles, dumpStats)}
}

func NewRedisStoreFactoryUseFlags(dir string) Factory {
	return NewRedisStoreFactory(dir, redisFlags.maxFileHandles, redisFlags.dumpStats)
}

type RedisStoreFactory struct {
	dir            string
	maxFileHandles int
	dumpStats      bool
	store          *internalRedisStore
}

func (f *RedisStoreFactory) CreateStore(ns string) ChunkStore {
	d.PanicIfFalse(f.store != nil)
	return newRedisStore(f.store, []byte(ns), false)
}

func (f *RedisStoreFactory) Shutter() {
	f.store.Close()
	f.store = nil
}
