package postgres

import (
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent/model"
)

func TestSelectColumns(t *testing.T) {
	tests := []struct {
		name     string
		fields   []string
		idColumn string
		want     selection
		wantErr  string
	}{
		{name: "nil", fields: nil, idColumn: "id"},
		{name: "only blanks", fields: []string{"", " "}, idColumn: "id"},
		{
			name:     "quotes, trims and drops blanks and duplicates",
			fields:   []string{" name ", "", "value", "name"},
			idColumn: "id",
			want:     selection{columns: []string{`"name"`, `"value"`}},
		},
		{
			name:     "dots become underscores like filter keys",
			fields:   []string{"api_definition.active"},
			idColumn: "id",
			want:     selection{columns: []string{`"api_definition_active"`}},
		},
		{
			name:     "case folds like unquoted filter keys",
			fields:   []string{"Name", "NAME"},
			idColumn: "id",
			want:     selection{columns: []string{`"name"`}},
		},
		{
			name:     "_id names the model's id column",
			fields:   []string{"_id", "name"},
			idColumn: "id",
			want:     selection{columns: []string{`"id"`, `"name"`}, wantsObjectID: true},
		},
		{
			name:     "the id column listed by its own name is kept",
			fields:   []string{"id", "name"},
			idColumn: "id",
			want:     selection{columns: []string{`"id"`, `"name"`}, wantsRawColumn: true},
		},
		{
			name:     "_id and the id column share one selected column",
			fields:   []string{"_id", "id"},
			idColumn: "id",
			want:     selection{columns: []string{`"id"`}, wantsObjectID: true, wantsRawColumn: true},
		},
		{
			name:     "a model whose id column is _id",
			fields:   []string{"_id", "name"},
			idColumn: "_id",
			want:     selection{columns: []string{`"_id"`, `"name"`}, wantsObjectID: true},
		},
		{
			name:     "refuses sql",
			fields:   []string{"name, (SELECT password FROM users LIMIT 1) AS x"},
			idColumn: "id",
			wantErr:  "invalid field name",
		},
		{name: "refuses quotes", fields: []string{`name"`}, idColumn: "id", wantErr: "invalid field name"},
		{name: "refuses a leading digit", fields: []string{"1name"}, idColumn: "id", wantErr: "invalid field name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectColumns(tt.fields, tt.idColumn)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDBMRow(t *testing.T) {
	id := model.NewObjectID()

	tests := []struct {
		name          string
		row           map[string]interface{}
		idColumn      string
		keepRawColumn bool
		wantID        interface{}
		wantRawColumn interface{}
	}{
		{
			name:     "the id column becomes _id and is dropped",
			row:      map[string]interface{}{"id": id.Hex()},
			idColumn: "id",
			wantID:   id,
		},
		{
			name:          "the id column listed by name is kept next to _id",
			row:           map[string]interface{}{"id": id.Hex()},
			idColumn:      "id",
			keepRawColumn: true,
			wantID:        id,
			wantRawColumn: id.Hex(),
		},
		{
			name:     "a non hex id is exposed as _id unchanged",
			row:      map[string]interface{}{"id": "custom"},
			idColumn: "id",
			wantID:   "custom",
		},
		{
			name:          "a model whose id column is _id keeps the key as the object id",
			row:           map[string]interface{}{"_id": id.Hex()},
			idColumn:      "_id",
			wantID:        id,
			wantRawColumn: id,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dbmRow(tt.row, tt.idColumn, tt.keepRawColumn)
			assert.Equal(t, tt.wantID, got["_id"])

			if tt.wantRawColumn == nil {
				assert.NotContains(t, got, "id")
			} else {
				assert.Equal(t, tt.wantRawColumn, got[tt.idColumn])
			}
		})
	}

	row := dbmRow(map[string]interface{}{"name": []byte("bytes")}, "id", false)
	assert.Equal(t, "bytes", row["name"], "byte columns become strings")
	assert.NotContains(t, row, "_id", "a row without an id column cannot carry one")
}

type underscoreIDObject struct {
	ID   model.ObjectID `gorm:"primaryKey;column:_id"`
	Name string
}

func (o *underscoreIDObject) GetObjectID() model.ObjectID   { return o.ID }
func (o *underscoreIDObject) SetObjectID(id model.ObjectID) { o.ID = id }
func (o *underscoreIDObject) TableName() string             { return "underscore_id_objects" }

func TestIDColumnOf(t *testing.T) {
	db := &gorm.DB{Config: &gorm.Config{NamingStrategy: schema.NamingStrategy{}}}

	assert.Equal(t, "id", idColumnOf(&plainIDObject{}, db), "an untagged ID field maps to id")
	assert.Equal(t, "_id", idColumnOf(&underscoreIDObject{}, db), "the column tag of the primary key wins")
	assert.Equal(t, "id", idColumnOf(&dummyNoKey{}, db), "no primary key falls back to id")
}

type plainIDObject struct {
	ID   model.ObjectID `gorm:"primaryKey"`
	Name string
}

func (o *plainIDObject) GetObjectID() model.ObjectID   { return o.ID }
func (o *plainIDObject) SetObjectID(id model.ObjectID) { o.ID = id }
func (o *plainIDObject) TableName() string             { return "plain_id_objects" }

type dummyNoKey struct {
	Name string
}

func (d *dummyNoKey) GetObjectID() model.ObjectID { return "" }
func (d *dummyNoKey) SetObjectID(model.ObjectID)  {}
func (d *dummyNoKey) TableName() string           { return "dummy_no_key" }
