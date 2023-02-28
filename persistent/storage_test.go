package persistent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPersistentStorage(t *testing.T) {
	testCases := []string{Mgo, OfficialMongo, "unvalid"}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			_, err := NewPersistentStorage(&ClientOpts{
				ConnectionString: "mongodb://localhost:27017/test",
				UseSSL:           false,
				Type:             tc,
			})

			if tc == "unvalid" {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
