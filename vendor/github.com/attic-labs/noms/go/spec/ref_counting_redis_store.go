// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

type refCountingRedisStore struct {
	*chunks.RedisStore
	refCount int
	closeFn  func()
}

func newRefCountingRedisStore(path string, closeFn func()) *refCountingRedisStore {
	return &refCountingRedisStore{chunks.NewRedisStoreUseFlags(path, ""), 1, closeFn}
}

func (r *refCountingRedisStore) AddRef() {
	r.refCount++
}

func (r *refCountingRedisStore) Close() (err error) {
	d.PanicIfFalse(r.refCount > 0)
	r.refCount--
	if r.refCount == 0 {
		err = r.RedisStore.Close()
		r.closeFn()
	}
	return
}
