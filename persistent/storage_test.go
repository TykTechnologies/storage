package persistent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPersistentStorage(t *testing.T) {
	testCases := []string{Mgo, OfficialMongo, "unvalid"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			_, err := NewPersistentStorage(&ClientOpts{Type: tc})

			if tc == Mgo {
				assert.Nil(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
