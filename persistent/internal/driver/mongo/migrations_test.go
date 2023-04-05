//go:build mongo
// +build mongo

package mongo

import (
	"testing"

	"github.com/TykTechnologies/storage/persistent/dbm"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Test_buildOpt(t *testing.T) {
	type args struct {
		opt dbm.DBM
	}
	tests := []struct {
		name string
		args args
		want *options.CreateCollectionOptions
	}{
		{
			name: "test buildOpt",
			args: args{
				opt: dbm.DBM{
					"capped": true,
					"collation": dbm.DBM{
						"locale":          "en_US",
						"caseLevel":       true,
						"caseFirst":       "caseFirst",
						"strength":        1,
						"numericOrdering": true,
						"alternate":       "alternate",
						"maxVariable":     "maxVariable",
						"normalization":   true,
						"backwards":       true,
					},
					"max":                100,
					"size":               100,
					"storageEngine":      "storageEngine",
					"validationAction":   "validationAction",
					"validationLevel":    "validationLevel",
					"validator":          "validator",
					"expireAfterSeconds": 100,
					"timeSeries": dbm.DBM{
						"timeField":   "timeField",
						"metaField":   "metaField",
						"granularity": "granularity",
					},
					"encryptedFields": "encryptedFields",
					"clusteredIndex":  "clusteredIndex",
				},
			},
			want: &options.CreateCollectionOptions{
				Capped: pointerBool(true),
				Collation: &options.Collation{
					Locale:          "en_US",
					CaseLevel:       true,
					CaseFirst:       "caseFirst",
					Strength:        1,
					NumericOrdering: true,
					Alternate:       "alternate",
					MaxVariable:     "maxVariable",
					Normalization:   true,
					Backwards:       true,
				},
				MaxDocuments:       pointerInt64(100),
				SizeInBytes:        pointerInt64(100),
				StorageEngine:      nil,
				ValidationAction:   pointerString("validationAction"),
				ValidationLevel:    pointerString("validationLevel"),
				Validator:          "validator",
				ExpireAfterSeconds: pointerInt64(100),
				TimeSeriesOptions: &options.TimeSeriesOptions{
					TimeField:   "timeField",
					MetaField:   pointerString("metaField"),
					Granularity: pointerString("granularity"),
				},
				EncryptedFields: "encryptedFields",
				ClusteredIndex:  "clusteredIndex",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := buildOpt(tt.args.opt)
			assert.Equal(t, true, *res.Capped)
			assert.Equal(t, "en_US", res.Collation.Locale)
			assert.Equal(t, true, res.Collation.CaseLevel)
			assert.Equal(t, "caseFirst", res.Collation.CaseFirst)
			assert.Equal(t, 1, res.Collation.Strength)
			assert.Equal(t, true, res.Collation.NumericOrdering)
			assert.Equal(t, "alternate", res.Collation.Alternate)
			assert.Equal(t, "maxVariable", res.Collation.MaxVariable)
			assert.Equal(t, true, res.Collation.Normalization)
			assert.Equal(t, true, res.Collation.Backwards)
			assert.Equal(t, int64(100), *res.MaxDocuments)
			assert.Equal(t, int64(100), *res.SizeInBytes)
			assert.Equal(t, nil, res.StorageEngine)
			assert.Equal(t, "validationAction", *res.ValidationAction)
			assert.Equal(t, "validationLevel", *res.ValidationLevel)
			assert.Equal(t, "validator", res.Validator)
			assert.Equal(t, int64(100), *res.ExpireAfterSeconds)
			assert.Equal(t, "timeField", res.TimeSeriesOptions.TimeField)
			assert.Equal(t, "metaField", *res.TimeSeriesOptions.MetaField)
			assert.Equal(t, "granularity", *res.TimeSeriesOptions.Granularity)
			assert.Equal(t, "encryptedFields", res.EncryptedFields)
			assert.Equal(t, "clusteredIndex", res.ClusteredIndex)
		})
	}
}

func pointerBool(b bool) *bool       { return &b }
func pointerInt64(i int64) *int64    { return &i }
func pointerString(s string) *string { return &s }
