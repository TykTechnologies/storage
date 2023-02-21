package id

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidObjectId(t *testing.T) {
	valid, _ := hex.DecodeString("63efa41f4713944d6f696768")
	tcs := []struct {
		testName      string
		givenObjectId OID
		expectedValid bool
	}{
		{
			testName:      "valid",
			givenObjectId: OID(valid),
			expectedValid: true,
		},
		{
			testName:      "invalid",
			givenObjectId: OID("test"),
			expectedValid: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			actual := tc.givenObjectId.Valid()

			assert.Equal(t, tc.expectedValid, actual)
		})
	}
}

func TestHex(t *testing.T) {

}
