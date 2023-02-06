package persistent

import (
	"testing"

	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestNewPersistentStorage(t *testing.T) {
	testCases := []string{Mgo, OfficialMongo, "unvalid"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			_, err := NewPersistentStorage(&model.ClientOpts{Type: tc})

			// at this moment we expect an error until the driver is implemented
			assert.Error(t, err)
		})
	}
}
