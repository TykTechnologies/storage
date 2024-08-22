package local

import (
	"encoding/json"
	"sort"
	"testing"
)

func TestSortedSetMarshal(t *testing.T) {
	o := NewSortedSetObject()

	newSortedSet := make(SortedSet, 0)
	entry1 := SortedSetEntry{Score: 1.0, Member: "one"}
	entry2 := SortedSetEntry{Score: 2.0, Member: "two"}

	newSortedSet = append(newSortedSet, entry1)
	newSortedSet = append(newSortedSet, entry2)

	sort.Sort(newSortedSet)

	o.Value = newSortedSet

	x, err := json.Marshal(o)
	if err != nil {
		t.Errorf("error marshalling sorted set: %v", err)
	}

	var y Object
	err = json.Unmarshal(x, &y)
	if err != nil {
		t.Errorf("error unmarshalling sorted set: %v", err)
	}

	if len(y.Value.([]interface{})) != 2 {
		t.Errorf("error unmarshalling sorted set: %v", y.Value)
	}

}
