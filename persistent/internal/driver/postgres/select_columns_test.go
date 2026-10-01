package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent/model"
)

func TestSelectColumns(t *testing.T) {
	tests := []struct {
		name        string
		fields      []string
		wantColumns []string
		wantsID     bool
		wantErr     string
	}{
		{name: "nil", fields: nil},
		{name: "only blanks", fields: []string{"", " "}},
		{
			name:        "quotes, trims and drops blanks and duplicates",
			fields:      []string{" name ", "", "value", "name"},
			wantColumns: []string{`"name"`, `"value"`},
		},
		{
			name:        "dots become underscores like filter keys",
			fields:      []string{"api_definition.active"},
			wantColumns: []string{`"api_definition_active"`},
		},
		{
			name:        "_id names the id column",
			fields:      []string{"_id", "name"},
			wantColumns: []string{`"id"`, `"name"`},
			wantsID:     true,
		},
		{name: "_id and id collapse", fields: []string{"_id", "id"}, wantColumns: []string{`"id"`}, wantsID: true},
		{
			name:    "refuses sql",
			fields:  []string{"name, (SELECT password FROM users LIMIT 1) AS x"},
			wantErr: "invalid field name",
		},
		{name: "refuses quotes", fields: []string{`name"`}, wantErr: "invalid field name"},
		{name: "refuses a leading digit", fields: []string{"1name"}, wantErr: "invalid field name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns, wantsID, err := selectColumns(tt.fields)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantColumns, columns)
			assert.Equal(t, tt.wantsID, wantsID)
		})
	}
}

func TestDBMRow(t *testing.T) {
	id := model.NewObjectID()

	row := dbmRow(map[string]interface{}{"id": id.Hex(), "name": []byte("bytes")}, false)
	assert.Equal(t, id, row["_id"], "a valid hex id is exposed as _id, a model.ObjectID")
	assert.Equal(t, id.Hex(), row["id"], "id stays when it was not asked for as _id")
	assert.Equal(t, "bytes", row["name"], "byte columns become strings")

	row = dbmRow(map[string]interface{}{"id": id.Hex()}, true)
	assert.Equal(t, id, row["_id"])
	assert.NotContains(t, row, "id", "only _id was asked for")

	row = dbmRow(map[string]interface{}{"id": "not-hex"}, true)
	assert.Equal(t, "not-hex", row["id"], "an id that is not an ObjectID is left as it is")
	assert.NotContains(t, row, "_id")
}
