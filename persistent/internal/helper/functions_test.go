package helper

import (
	"bytes"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/persistent/dbm"
	"github.com/stretchr/testify/assert"
)

func TestErrPrint(t *testing.T) {
	tcs := []struct {
		testName    string
		givenErr    error
		expectedLog string
	}{
		{
			testName: "err nil",
			givenErr: nil,
		},
		{
			testName:    "err not nil",
			givenErr:    errors.New("test"),
			expectedLog: time.Now().Format("2006/01/02 15:04:05") + " test\n",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			var buf bytes.Buffer
			log.SetOutput(&buf)
			defer func() {
				log.SetOutput(os.Stderr)
			}()

			ErrPrint(tc.givenErr)
			assert.Equal(t, tc.expectedLog, buf.String())
		})
	}
}

func TestIsSlice(t *testing.T) {
	tcs := []struct {
		testName string
		given    interface{}
		expected bool
	}{
		{
			testName: "string",
			given:    "test",
			expected: false,
		},
		{
			testName: "struct",
			given: struct {
				test string
			}{},
			expected: false,
		},
		{
			testName: "[]string",
			given:    []string{"test"},
			expected: true,
		},
		{
			testName: "[]struct",
			given: []struct {
				test string
			}{},
			expected: true,
		},

		{
			testName: "[]error",
			given:    []error{},
			expected: true,
		},
		{
			testName: "dbm.DBM",
			given:    dbm.DBM{},
			expected: false,
		},
		{
			testName: "[]dbm.DBM",
			given:    []dbm.DBM{},
			expected: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			actual := IsSlice(tc.given)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
