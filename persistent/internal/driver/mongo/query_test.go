//go:build mongo
// +build mongo

package mongo

import (
	"encoding/json"
	"testing"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBuildQuery(t *testing.T) {
	tcs := []struct {
		testName string
		input    model.DBM
		output   bson.M
	}{
		{
			testName: "Test empty input",
			input:    model.DBM{},
			output:   bson.M{},
		},
		{
			testName: "Test with nested query",
			input: model.DBM{
				"testName": model.DBM{
					"$ne": "123",
				},
			},
			output: bson.M{
				"testName": bson.M{
					"$ne": "123",
				},
			},
		},
		{
			testName: "Test with $in query",
			input: model.DBM{
				"age": []int{20, 30, 40},
			},
			output: bson.M{
				"age": bson.M{
					"$in": []int{20, 30, 40},
				},
			},
		},
		{
			testName: "Test with _id",
			input: model.DBM{
				"_id": id.ObjectIdHex("61634c7b5f46cc8c296edc36"),
			},
			output: bson.M{
				"_id": id.ObjectIdHex("61634c7b5f46cc8c296edc36"),
			},
		},
		{
			testName: "Test with invalid _id",
			input: model.DBM{
				"_id": "invalid_id",
			},
			output: bson.M{
				"_id": "invalid_id",
			},
		},
		{
			testName: "Test with $regex",
			input: model.DBM{
				"testName": model.DBM{
					"$regex": "tyk.com$",
				},
			},
			output: bson.M{
				"testName": bson.M{
					"$regex": "tyk.com$",
				},
			},
		},
		{
			testName: "Test with $in",
			input: model.DBM{
				"age": model.DBM{
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
			testName: "Test with $i",
			input: model.DBM{
				"testName": model.DBM{
					"$i": "tyk",
				},
			},
			output: bson.M{
				"testName": &primitive.Regex{
					Pattern: "^tyk$",
					Options: "i",
				},
			},
		},
		{
			testName: "Test with $text",
			input: model.DBM{
				"testName": model.DBM{
					"$text": "tyk",
				},
			},
			output: bson.M{
				"testName": bson.M{
					"$regex": primitive.Regex{
						Pattern: "tyk",
						Options: "i",
					},
				},
			},
		},
		{
			testName: "Test with unsupported operator",
			input: model.DBM{
				"testName": model.DBM{
					"$foo": "bar",
				},
			},
			output: bson.M{
				"testName": bson.M{
					"$foo": "bar",
				},
			},
		},
		{
			testName: "Test with slice of strings and _id key",
			input: model.DBM{
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
			testName: "Test with $min",
			input: model.DBM{
				"age": model.DBM{
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
			testName: "Test with $max",
			input: model.DBM{
				"age": model.DBM{
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
			testName: "Test with $inc",
			input: model.DBM{
				"age": model.DBM{
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
			testName: "Test with $set",
			input: model.DBM{
				"age": model.DBM{
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
			testName: "Default value",
			input: model.DBM{
				"testName":  "John",
				"age":       30,
				"is_active": true,
			},
			output: bson.M{
				"testName":  "John",
				"age":       30,
				"is_active": true,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			result := buildQuery(tc.input)

			got, errActual := json.Marshal(result)
			assert.Nil(t, errActual)

			expected, errExpected := json.Marshal(tc.output)
			assert.Nil(t, errExpected)

			assert.EqualValues(t, expected, got)
		})
	}
}

func TestBuildLimitQuery(t *testing.T) {
	tcs := []struct {
		testName string
		input    []string
		expected bson.D
	}{
		{
			testName: "Empty input",
			input:    []string{},
			expected: bson.D{},
		},
		{
			testName: "Single field",
			input:    []string{"testName"},
			expected: bson.D{{Key: "testName", Value: 1}},
		},
		{
			testName: "Descending order",
			input:    []string{"-age"},
			expected: bson.D{{Key: "age", Value: -1}},
		},
		{
			testName: "Ascending order",
			input:    []string{"+salary"},
			expected: bson.D{{Key: "salary", Value: 1}},
		},
		{
			testName: "Mixed order",
			input:    []string{"testName", "+age", "-salary"},
			expected: bson.D{{Key: "testName", Value: 1}, {Key: "age", Value: 1}, {Key: "salary", Value: -1}},
		},
		{
			testName: "Text score order",
			input:    []string{"$textScore:testName"},
			expected: bson.D{{Key: "testName", Value: bson.M{"$meta": "textScore"}}},
		},
		{
			testName: "Multiple text score order",
			input:    []string{"$textScore:testName", "$textScore:address"},
			expected: bson.D{
				{Key: "testName", Value: bson.M{"$meta": "textScore"}},
				{Key: "address", Value: bson.M{"$meta": "textScore"}},
			},
		},
		{
			testName: "Empty field",
			input:    []string{"testName", ""},
			expected: bson.D{{Key: "testName", Value: 1}},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			got := buildLimitQuery(tc.input...)

			assert.EqualValues(t, tc.expected, got)
		})
	}
}
