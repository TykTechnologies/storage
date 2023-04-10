package utils

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2"
)

func TestIsErrNoRows(t *testing.T) {
	tests := []struct {
		name  string
		input error
		want  bool
	}{
		{
			name:  "mgo error",
			input: mgo.ErrNotFound,
			want:  true,
		},
		{
			name:  "mongo error",
			input: mongo.ErrNoDocuments,
			want:  true,
		},
		{
			name:  "other error",
			input: errors.New("other error"),
			want:  false,
		},
		{
			name:  "nil error",
			input: nil,
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsErrNoRows(tt.input); got != tt.want {
				t.Errorf("IsErrNoRows() = %v, want %v", got, tt.want)
			}
		})
	}
}
