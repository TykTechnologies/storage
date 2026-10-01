package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/persistent/model"
)

func TestSelectColumns(t *testing.T) {
	tests := []struct {
		name    string
		fields  []string
		want    selection
		wantErr string
	}{
		{name: "nil", fields: nil},
		{name: "only blanks", fields: []string{"", " "}},
		{
			name:   "quotes, trims and drops blanks and duplicates",
			fields: []string{" name ", "", "value", "name"},
			want:   selection{columns: []string{`"name"`, `"value"`}},
		},
		{
			name:   "dots become underscores like filter keys",
			fields: []string{"api_definition.active"},
			want:   selection{columns: []string{`"api_definition_active"`}},
		},
		{
			name:   "_id names the id column",
			fields: []string{"_id", "name"},
			want:   selection{columns: []string{`"id"`, `"name"`}, hasID: true, wantsObjectID: true},
		},
		{
			name:   "id is an ordinary column",
			fields: []string{"id", "name"},
			want:   selection{columns: []string{`"id"`, `"name"`}, hasID: true, wantsRawID: true},
		},
		{
			name:   "_id and id share the column but both are remembered",
			fields: []string{"_id", "id"},
			want:   selection{columns: []string{`"id"`}, hasID: true, wantsObjectID: true, wantsRawID: true},
		},
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
			got, err := selectColumns(tt.fields)
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
		wantObjectID  bool
		wantRawID     bool
		wantID        interface{}
		wantRawColumn interface{}
	}{
		{
			name:          "neither requested keeps id and adds _id",
			row:           map[string]interface{}{"id": id.Hex()},
			wantID:        id,
			wantRawColumn: id.Hex(),
		},
		{
			name:         "_id only drops the raw column",
			row:          map[string]interface{}{"id": id.Hex()},
			wantObjectID: true,
			wantID:       id,
		},
		{
			name:          "id only keeps both",
			row:           map[string]interface{}{"id": id.Hex()},
			wantRawID:     true,
			wantID:        id,
			wantRawColumn: id.Hex(),
		},
		{
			name:          "both requested keeps both",
			row:           map[string]interface{}{"id": id.Hex()},
			wantObjectID:  true,
			wantRawID:     true,
			wantID:        id,
			wantRawColumn: id.Hex(),
		},
		{
			name:         "non hex id is still exposed as _id",
			row:          map[string]interface{}{"id": "custom"},
			wantObjectID: true,
			wantID:       model.ObjectID("custom"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dbmRow(tt.row, tt.wantObjectID, tt.wantRawID)
			assert.Equal(t, tt.wantID, got["_id"])

			if tt.wantRawColumn == nil {
				assert.NotContains(t, got, "id")
			} else {
				assert.Equal(t, tt.wantRawColumn, got["id"])
			}
		})
	}

	row := dbmRow(map[string]interface{}{"name": []byte("bytes")}, false, false)
	assert.Equal(t, "bytes", row["name"], "byte columns become strings")
	assert.NotContains(t, row, "_id", "a row without an id column cannot carry one")
}
