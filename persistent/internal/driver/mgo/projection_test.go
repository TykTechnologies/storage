package mgo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"

	"github.com/TykTechnologies/storage/persistent/model"
)

func TestBuildProjection(t *testing.T) {
	tests := []struct {
		name   string
		fields []string
		want   bson.M
	}{
		{name: "nil", fields: nil, want: nil},
		{name: "only blanks", fields: []string{"", "  "}, want: nil},
		{
			name:   "trims, drops blanks and duplicates",
			fields: []string{" name ", "", "age", "name"},
			want:   bson.M{"name": 1, "age": 1},
		},
		{
			name:   "drops a child of a listed parent",
			fields: []string{"api_definition.name", "api_definition", "org_id"},
			want:   bson.M{"api_definition": 1, "org_id": 1},
		},
		{
			name:   "keeps siblings",
			fields: []string{"country.name", "country.code"},
			want:   bson.M{"country.name": 1, "country.code": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, buildProjection(tt.fields))
		})
	}
}

func TestNormalizeObjectIDs(t *testing.T) {
	raw := bson.NewObjectId()
	want := model.ObjectIDHex(raw.Hex())

	dbmRows := []model.DBM{{"_id": raw}, {"_id": "custom"}, nil}
	normalizeObjectIDs(&dbmRows)
	assert.Equal(t, want, dbmRows[0]["_id"])
	assert.Equal(t, "custom", dbmRows[1]["_id"], "ids of another type are left alone")

	dbm := model.DBM{"_id": raw}
	normalizeObjectIDs(&dbm)
	assert.Equal(t, want, dbm["_id"])

	plainRows := []map[string]interface{}{{"_id": raw}}
	normalizeObjectIDs(&plainRows)
	assert.Equal(t, want, plainRows[0]["_id"])

	plain := map[string]interface{}{"_id": raw}
	normalizeObjectIDs(&plain)
	assert.Equal(t, want, plain["_id"])

	bsonRows := []bson.M{{"_id": raw}}
	normalizeObjectIDs(&bsonRows)
	assert.Equal(t, want, bsonRows[0]["_id"])

	bsonRow := bson.M{"_id": raw}
	normalizeObjectIDs(&bsonRow)
	assert.Equal(t, want, bsonRow["_id"])

	normalizeObjectIDs(nil)
}
