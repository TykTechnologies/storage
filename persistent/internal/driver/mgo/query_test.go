//go:build mongo
// +build mongo

package mgo

import (
	"reflect"
	"testing"

	"github.com/TykTechnologies/storage/persistent/dbm"

	"github.com/TykTechnologies/storage/persistent/id"
	"gopkg.in/mgo.v2/bson"
)

func TestBuildQuery(t *testing.T) {
	tests := []struct {
		name   string
		input  dbm.DBM
		output bson.M
	}{
		{
			name:   "Test empty input",
			input:  dbm.DBM{},
			output: bson.M{},
		},
		{
			name: "Test with nested query",
			input: dbm.DBM{
				"name": dbm.DBM{
					"$ne": "123",
				},
			},
			output: bson.M{
				"name": bson.M{
					"$ne": "123",
				},
			},
		},
		{
			name: "Test with $in query",
			input: dbm.DBM{
				"age": []int{20, 30, 40},
			},
			output: bson.M{
				"age": bson.M{
					"$in": []int{20, 30, 40},
				},
			},
		},
		{
			name: "Test with _id",
			input: dbm.DBM{
				"_id": id.ObjectIdHex("61634c7b5f46cc8c296edc36"),
			},
			output: bson.M{
				"_id": id.ObjectIdHex("61634c7b5f46cc8c296edc36"),
			},
		},
		{
			name: "Test with invalid _id",
			input: dbm.DBM{
				"_id": "invalid_id",
			},
			output: bson.M{
				"_id": "invalid_id",
			},
		},
		{
			name: "Test with $regex",
			input: dbm.DBM{
				"name": dbm.DBM{
					"$regex": "tyk.com$",
				},
			},
			output: bson.M{
				"name": bson.M{
					"$regex": "tyk.com$",
				},
			},
		},

		{
			name: "Test with $in",
			input: dbm.DBM{
				"age": dbm.DBM{
					"$in": []int{25, 30, 35},
				},
			},
			output: bson.M{
				"age": bson.M{
					"$in": []int{25, 30, 35},
				},
			},
		},
		{
			name: "Test with $i",
			input: dbm.DBM{
				"name": dbm.DBM{
					"$i": "tyk",
				},
			},
			output: bson.M{
				"name": &bson.RegEx{
					Pattern: "^tyk$",
					Options: "i",
				},
			},
		},
		{
			name: "Test with $text",
			input: dbm.DBM{
				"name": dbm.DBM{
					"$text": "tyk",
				},
			},
			output: bson.M{
				"name": bson.M{
					"$regex": bson.RegEx{
						Pattern: "tyk",
						Options: "i",
					},
				},
			},
		},
		{
			name: "Test with unsupported operator",
			input: dbm.DBM{
				"name": dbm.DBM{
					"$foo": "bar",
				},
			},
			output: bson.M{
				"name": bson.M{
					"$foo": "bar",
				},
			},
		},
		{
			name: "Test with slice of strings and _id key",
			input: dbm.DBM{
				"_id": []string{"61634c7b5f46cc8c296edc36", "61634c7b5f46cc8c296edc37"},
			},
			output: bson.M{
				"_id": bson.M{
					"$in": []id.ObjectId{
						id.ObjectIdHex("61634c7b5f46cc8c296edc36"),
						id.ObjectIdHex("61634c7b5f46cc8c296edc37"),
					},
				},
			},
		},
		{
			name: "Test with $min",
			input: dbm.DBM{
				"age": dbm.DBM{
					"$min": 20,
				},
			},
			output: bson.M{
				"age": bson.M{
					"$min": 20,
				},
			},
		},
		{
			name: "Test with $max",
			input: dbm.DBM{
				"age": dbm.DBM{
					"$max": 20,
				},
			},
			output: bson.M{
				"age": bson.M{
					"$max": 20,
				},
			},
		},
		{
			name: "Test with $inc",
			input: dbm.DBM{
				"age": dbm.DBM{
					"$inc": 20,
				},
			},
			output: bson.M{
				"age": bson.M{
					"$inc": 20,
				},
			},
		},
		{
			name: "Test with $set",
			input: dbm.DBM{
				"age": dbm.DBM{
					"$set": 20,
				},
			},
			output: bson.M{
				"age": bson.M{
					"$set": 20,
				},
			},
		},
		{
			name: "Default value",
			input: dbm.DBM{
				"name":      "John",
				"age":       30,
				"is_active": true,
			},
			output: bson.M{
				"name":      "John",
				"age":       30,
				"is_active": true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := buildQuery(test.input)

			if !reflect.DeepEqual(result, test.output) {
				t.Errorf("Expected output %v, but got %v", test.output, result)
			}
		})
	}
}

func Test_getColName(t *testing.T) {
	type args struct {
		query model.DBM
		row   id.DBObject
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "get collection name from query",
			args: args{
				query: model.DBM{
					"_collection": "test",
				},
				row: nil,
			},
			want:    "test",
			wantErr: false,
		},
		{
			name: "get collection name from row",
			args: args{
				query: nil,
				row:   &dummyDBObject{},
			},
			want:    "dummy",
			wantErr: false,
		},
		{
			name: "no collection name",
			args: args{
				query: nil,
				row:   nil,
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getColName(tt.args.query, tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("getColName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getColName() = %v, want %v", got, tt.want)
			}
		})
	}
}
