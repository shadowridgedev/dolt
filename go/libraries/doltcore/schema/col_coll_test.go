package schema

import (
	"github.com/attic-labs/noms/go/types"
	"math"
	"reflect"
	"strconv"
	"testing"
)

var firstNameCol = Column{"first", 0, types.StringKind, false, nil}
var lastNameCol = Column{"last", 1, types.StringKind, false, nil}

func TestGetByNameAndTag(t *testing.T) {
	cols := []Column{firstNameCol, lastNameCol}
	colColl, _ := NewColCollection(cols...)

	tests := []struct {
		name       string
		tag        uint64
		expected   Column
		shouldBeOk bool
	}{
		{firstNameCol.Name, firstNameCol.Tag, firstNameCol, true},
		{lastNameCol.Name, lastNameCol.Tag, lastNameCol, true},
		{"missing", math.MaxUint64, InvalidCol, false},
	}

	for _, test := range tests {
		actual, ok := colColl.GetByName(test.name)

		if ok != test.shouldBeOk {
			t.Errorf("name - shouldBeOk: %v, ok: %v", test.shouldBeOk, ok)
		} else if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("name - %v != %v", actual, test.expected)
		}

		actual, ok = colColl.GetByTag(test.tag)

		if ok != test.shouldBeOk {
			t.Errorf("tag - shouldBeOk: %v, ok: %v", test.shouldBeOk, ok)
		} else if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("tag - %v != %v", actual, test.expected)
		}
	}
}

func TestAppendAndItrInSortOrder(t *testing.T) {
	cols := []Column{
		{"0", 0, types.StringKind, false, nil},
		{"2", 2, types.StringKind, false, nil},
		{"4", 4, types.StringKind, false, nil},
		{"3", 3, types.StringKind, false, nil},
		{"1", 1, types.StringKind, false, nil},
	}
	cols2 := []Column{
		{"7", 7, types.StringKind, false, nil},
		{"9", 9, types.StringKind, false, nil},
		{"5", 5, types.StringKind, false, nil},
		{"8", 8, types.StringKind, false, nil},
		{"6", 6, types.StringKind, false, nil},
	}

	colColl, _ := NewColCollection(cols...)
	validateItrInSortOrder(len(cols), colColl, t)
	colColl2, _ := colColl.Append(cols2...)
	validateItrInSortOrder(len(cols), colColl, t) //validate immutability
	validateItrInSortOrder(len(cols)+len(cols2), colColl2, t)
}

func validateItrInSortOrder(numCols int, colColl *ColCollection, t *testing.T) {
	if numCols != colColl.Size() {
		t.Error("missing data")
	}

	var idx uint64
	colColl.ItrInSortedOrder(func(tag uint64, col Column) (stop bool) {
		if idx != tag {
			t.Error("Not in order")
		} else if col.Name != strconv.FormatUint(idx, 10) || col.Tag != tag {
			t.Errorf("tag:%d - %v", tag, col)
		}

		idx++

		return false
	})

	if idx != uint64(numCols) {
		t.Error("Did not iterate over all values")
	}
}