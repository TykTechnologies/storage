package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/TykTechnologies/storage/persistent/model"
)

func TestBuildProjection(t *testing.T) {
	tests := []struct {
		name   string
		fields []string
		want   bson.D
	}{
		{name: "nil", fields: nil, want: nil},
		{name: "only blanks", fields: []string{"", "  "}, want: nil},
		{
			name:   "trims and drops blanks",
			fields: []string{" name ", "", "age"},
			want:   bson.D{{Key: "name", Value: 1}, {Key: "age", Value: 1}},
		},
		{
			name:   "keeps the first of a duplicate",
			fields: []string{"name", "age", "name"},
			want:   bson.D{{Key: "name", Value: 1}, {Key: "age", Value: 1}},
		},
		{
			name:   "drops a child of a listed parent",
			fields: []string{"api_definition.name", "api_definition", "org_id"},
			want:   bson.D{{Key: "api_definition", Value: 1}, {Key: "org_id", Value: 1}},
		},
		{
			name:   "keeps siblings",
			fields: []string{"country.name", "country.code"},
			want:   bson.D{{Key: "country.name", Value: 1}, {Key: "country.code", Value: 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, buildProjection(tt.fields))
		})
	}
}

func TestNormalizeObjectIDs(t *testing.T) {
	raw := primitive.NewObjectID()
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

	typed := []dummyTyped{{ID: raw}}
	normalizeObjectIDs(&typed)
	assert.Equal(t, raw, typed[0].ID, "typed destinations are untouched")
	normalizeObjectIDs(nil)
}

type dummyTyped struct {
	ID primitive.ObjectID `bson:"_id"`
}
