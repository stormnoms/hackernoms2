// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package spec provides builders and parsers for spelling Noms databases,
// datasets and values.
package spec

import (
	"github.com/attic-labs/noms/go/chunks"
)

var (
	redisStores = map[string]*refCountingRedisStore{}
)

func getBoltStore(path, bucketname string) chunks.ChunkStore {
	store := chunks.NewBoltStoreUseFlags(path, bucketname)
	return store
}

func getRedisStore(path string) chunks.ChunkStore {
	if store, ok := redisStores[path]; ok {
		store.AddRef()
		return store
	}

	store := newRefCountingRedisStore(path, func() {
		delete(redisStores, path)
	})
	redisStores[path] = store
	return store
}
