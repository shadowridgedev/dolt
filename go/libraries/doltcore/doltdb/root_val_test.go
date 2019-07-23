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

package doltdb

import (
	"context"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestTableDiff(t *testing.T) {
	ddb, _ := LoadDoltDB(context.Background(), types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(context.Background(), "billy bob", "bigbillieb@fake.horse")

	cs, _ := NewCommitSpec("head", "master")
	cm, _ := ddb.Resolve(context.Background(), cs)

	root := cm.GetRootValue()
	added, modified, removed := root.TableDiff(context.Background(), root)

	if len(added)+len(modified)+len(removed) != 0 {
		t.Error("Bad table diff when comparing two repos")
	}

	sch := createTestSchema()
	tbl1, _ := createTestTable(ddb.ValueReadWriter(), sch, types.NewMap(context.Background(), ddb.ValueReadWriter()))

	root2 := root.PutTable(context.Background(), ddb, "tbl1", tbl1)

	added, modified, removed = root2.TableDiff(context.Background(), root)

	if len(added) != 1 || added[0] != "tbl1" || len(modified)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed = root.TableDiff(context.Background(), root2)

	if len(removed) != 1 || removed[0] != "tbl1" || len(modified)+len(added) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	rowData, _ := createTestRowData(ddb.ValueReadWriter(), sch)
	tbl1Updated, _ := createTestTable(ddb.ValueReadWriter(), sch, rowData)

	root3 := root.PutTable(context.Background(), ddb, "tbl1", tbl1Updated)

	added, modified, removed = root3.TableDiff(context.Background(), root2)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed = root2.TableDiff(context.Background(), root3)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	root4 := root3.PutTable(context.Background(), ddb, "tbl2", tbl1)

	added, modified, removed = root2.TableDiff(context.Background(), root4)
	if len(modified) != 1 || modified[0] != "tbl1" || len(removed) != 1 || removed[0] != "tbl2" || +len(added) != 0 {
		t.Error("Bad table diff after adding a second table")
	}

	added, modified, removed = root4.TableDiff(context.Background(), root2)
	if len(modified) != 1 || modified[0] != "tbl1" || len(added) != 1 || added[0] != "tbl2" || +len(removed) != 0 {
		t.Error("Bad table diff after adding a second table")
	}
}
