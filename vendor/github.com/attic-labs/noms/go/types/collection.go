// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type Collection interface {
	Value
	Len() uint64
	Empty() bool
	sequence() sequence
}
