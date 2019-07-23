// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package edits

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func (coll *KVPCollection) String() string {
	ctx := context.Background()

	itr := coll.Iterator()
	val := itr.Next()

	keys := make([]types.Value, coll.totalSize)
	for i := 0; val != nil; i++ {
		keys[i] = val.Key.Value(ctx)
		val = itr.Next()
	}

	tpl := types.NewTuple(types.Format_7_18, keys...)
	return types.EncodedValue(ctx, tpl)
}

func TestKVPCollection(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	testKVPCollection(t, rng)

	for i := 0; i < 64; i++ {
		seed := time.Now().UnixNano()
		t.Log(seed)
		rng := rand.New(rand.NewSource(seed))
		testKVPCollection(t, rng)
	}
}

func testKVPCollection(t *testing.T, rng *rand.Rand) {
	const (
		maxSize = 1024
		minSize = 4

		maxColls = 128
		minColls = 3
	)

	numColls := int(minColls + rng.Int31n(maxColls-minColls))
	colls := make([]*KVPCollection, numColls)
	size := int(minSize + rng.Int31n(maxSize-minSize))

	t.Log("num collections:", numColls, "- buffer size", size)

	for i := 0; i < numColls; i++ {
		colls[i] = createKVPColl(rng, size)
	}

	for len(colls) > 1 {
		for i, coll := range colls {
			inOrder, _ := IsInOrder(NewItr(types.Format_7_18, coll))
			if !inOrder {
				t.Fatal(i, "not in order")
			}
		}

		var newColls []*KVPCollection
		for i, j := 0, len(colls)-1; i <= j; i, j = i+1, j-1 {
			if i == j {
				newColls = append(newColls, colls[i])
			} else {
				s1 := colls[i].Size()
				s2 := colls[j].Size()
				//fmt.Print(colls[i].String(), "+", colls[j].String())
				mergedColl := colls[i].DestructiveMerge(colls[j])

				ms := mergedColl.Size()

				if s1+s2 != ms {
					t.Fatal("wrong size")
				}

				//fmt.Println("=", mergedColl.String())
				newColls = append(newColls, mergedColl)
			}
		}

		colls = newColls
	}

	inOrder, numItems := IsInOrder(NewItr(types.Format_7_18, colls[0]))
	if !inOrder {
		t.Fatal("collection not in order")
	} else if numItems != numColls*size {
		t.Fatal("Unexpected size")
	}
}

func createKVPColl(rng *rand.Rand, size int) *KVPCollection {
	kvps := make(types.KVPSlice, size)

	for i := 0; i < size; i++ {
		kvps[i] = types.KVP{Key: types.Uint(rng.Uint64() % 10000), Val: types.NullValue}
	}

	sort.Stable(types.KVPSort{Values: kvps, NBF: types.Format_7_18})

	return NewKVPCollection(types.Format_7_18, kvps)
}
