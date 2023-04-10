//go:build mongo
// +build mongo

package mgo

import (
	"reflect"
	"testing"

	"gopkg.in/mgo.v2"
)

func TestBuildOpt(t *testing.T) {
	tests := []struct {
		name string
		opt  map[string]interface{}
		want *mgo.CollectionInfo
	}{
		{
			name: "test buildOpt",
			opt: map[string]interface{}{
				"capped":           true,
				"maxBytes":         10,
				"maxDocs":          10,
				"disableIdIndex":   true,
				"forceIdIndex":     true,
				"validator":        "validator",
				"validationLevel":  "validationLevel",
				"validationAction": "validationAction",
				"storageEngine":    "storageEngine",
			},
			want: &mgo.CollectionInfo{
				Capped:           true,
				MaxBytes:         10,
				MaxDocs:          10,
				DisableIdIndex:   true,
				ForceIdIndex:     true,
				Validator:        "validator",
				ValidationLevel:  "validationLevel",
				ValidationAction: "validationAction",
				StorageEngine:    "storageEngine",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildOpt(tt.opt); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildOpt() = %v, want %v", got, tt.want)
			}
		})
	}
}
