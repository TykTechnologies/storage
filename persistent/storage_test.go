package persistent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

func TestNewPersistentStorage(t *testing.T) {
	testCases := []string{Mgo, OfficialMongo, "unvalid"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			_, err := NewPersistentStorage(&ClientOpts{Type: tc})

			// at this moment we expect an error until the driver is implemented

			if tc == Mgo {
				assert.Nil(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
