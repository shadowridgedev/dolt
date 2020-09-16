// Copyright 2020 Liquidata, Inc.
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

package sqle

import (
	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
)

// RootDatabase in an implementation of sql.Database for root values. Does not expose any of the internal dolt tables.
type RootDatabase struct {
	*doltdb.RootValue
}

var _ sql.Database = (*RootDatabase)(nil)

func (r *RootDatabase) Name() string {
	return "dolt"
}

func (r *RootDatabase) GetTableInsensitive(ctx *sql.Context, tableName string) (sql.Table, bool, error) {
	if doltdb.HasDoltPrefix(tableName) {
		return nil, false, nil
	}
	table, tableName, ok, err := r.RootValue.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}
	return &DoltTable{name: tableName, table: table, sch: sch, db: r}, true, nil
}

func (r *RootDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	tableNames, err := r.RootValue.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	return filterDoltInternalTables(tableNames), nil
}
