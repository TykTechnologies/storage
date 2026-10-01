package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProjectionFields(t *testing.T) {
	tests := []struct {
		name   string
		fields []string
		want   []string
	}{
		{name: "nil", fields: nil, want: []string{}},
		{name: "only blanks", fields: []string{"", "  "}, want: []string{}},
		{name: "trims and drops blanks", fields: []string{" name ", "", "age"}, want: []string{"name", "age"}},
		{name: "keeps the first of a duplicate", fields: []string{"name", "age", "name"}, want: []string{"name", "age"}},
		{
			name:   "drops a child of a listed parent",
			fields: []string{"api_definition.name", "api_definition", "org_id"},
			want:   []string{"api_definition", "org_id"},
		},
		{
			name:   "drops a grandchild of a listed parent",
			fields: []string{"a.b.c", "a"},
			want:   []string{"a"},
		},
		{
			name:   "keeps siblings",
			fields: []string{"country.name", "country.code"},
			want:   []string{"country.name", "country.code"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ProjectionFields(tt.fields))
		})
	}
}

type rawID string

type namedMap map[string]interface{}

func TestNormalizeObjectIDs(t *testing.T) {
	normalize := func(id interface{}) (interface{}, bool) {
		raw, ok := id.(rawID)
		if !ok {
			return nil, false
		}

		return "normalized:" + string(raw), true
	}

	rows := []map[string]interface{}{{"_id": rawID("a")}, {"_id": "custom"}, nil, {"name": "no id"}}
	NormalizeObjectIDs(&rows, normalize)
	assert.Equal(t, "normalized:a", rows[0]["_id"])
	assert.Equal(t, "custom", rows[1]["_id"], "ids of another type are left alone")
	assert.Nil(t, rows[2])
	assert.NotContains(t, rows[3], "_id")

	row := map[string]interface{}{"_id": rawID("b")}
	NormalizeObjectIDs(&row, normalize)
	assert.Equal(t, "normalized:b", row["_id"])

	named := []namedMap{{"_id": rawID("c")}}
	NormalizeObjectIDs(&named, normalize)
	assert.Equal(t, "normalized:c", named[0]["_id"], "named map types such as bson.M qualify")

	var nilMap map[string]interface{}
	NormalizeObjectIDs(&nilMap, normalize)
	assert.Nil(t, nilMap)

	typed := []struct{ ID rawID }{{ID: "d"}}
	NormalizeObjectIDs(&typed, normalize)
	assert.Equal(t, rawID("d"), typed[0].ID, "typed destinations are untouched")

	intKeys := map[int]interface{}{1: rawID("e")}
	NormalizeObjectIDs(&intKeys, normalize)
	assert.Equal(t, rawID("e"), intKeys[1], "maps without string keys are untouched")

	NormalizeObjectIDs(nil, normalize)
	NormalizeObjectIDs(row, normalize)
	NormalizeObjectIDs((*map[string]interface{})(nil), normalize)
}
