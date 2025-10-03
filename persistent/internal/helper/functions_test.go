package helper

import (
	"bytes"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
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

func TestIsCosmosDB(t *testing.T) {
	cases := []struct {
		connectionString string
		expectedResult   bool
	}{
		{"AccountEndpoint=https://mycosmosdb.documents.azure.com:443/;AccountKey=myaccountkey;Database=mydatabase;", true},
		{"AccountEndpoint=https://mycosmosdb.documents.azure.com:443/;Database=mydatabase;", true},
		{"AccountEndpoint=https://mycosmosdb.documents.azure.com:443/;AccountKey=myaccountkey;", true},
		{"https://mycosmosdb.documents.azure.com:443/;AccountKey=myaccountkey;Database=mydatabase;", true},
		{"mongodb://localhost:27017/mydatabase", false},
	}

	for _, c := range cases {
		result := IsCosmosDB(c.connectionString)
		if result != c.expectedResult {
			t.Errorf("IsCosmosDB(%q) == %v, expected %v", c.connectionString, result, c.expectedResult)
		}
	}
}

func TestShouldReconnect(t *testing.T) {
	testCases := []struct {
		name            string
		err             error
		shouldReconnect bool
	}{
		// Cases that should return true - network errors
		{
			name: "network error with NetworkError label",
			err: mongo.CommandError{
				Message: "connection error",
				Labels:  []string{"NetworkError"},
			},
			shouldReconnect: true,
		},

		// Cases that should return true - reconnect error codes
		{
			name: "HostUnreachable (code 6)",
			err: mongo.CommandError{
				Code:    6,
				Message: "host unreachable",
			},
			shouldReconnect: true,
		},
		{
			name: "HostNotFound (code 7)",
			err: mongo.CommandError{
				Code:    7,
				Message: "host not found",
			},
			shouldReconnect: true,
		},
		{
			name: "NetworkTimeout (code 89)",
			err: mongo.CommandError{
				Code:    89,
				Message: "network timeout",
			},
			shouldReconnect: true,
		},
		{
			name: "ShutdownInProgress (code 91)",
			err: mongo.CommandError{
				Code:    91,
				Message: "shutdown in progress",
			},
			shouldReconnect: true,
		},
		{
			name: "FailedToSatisfyReadPreference (code 133)",
			err: mongo.CommandError{
				Code:    133,
				Message: "failed to satisfy read preference",
			},
			shouldReconnect: true,
		},
		{
			name: "PrimarySteppedDown (code 189)",
			err: mongo.CommandError{
				Code:    189,
				Message: "primary stepped down",
			},
			shouldReconnect: true,
		},
		{
			name: "ExceededTimeLimit (code 262)",
			err: mongo.CommandError{
				Code:    262,
				Message: "exceeded time limit",
			},
			shouldReconnect: true,
		},
		{
			name: "SocketException (code 9001)",
			err: mongo.CommandError{
				Code:    9001,
				Message: "socket exception",
			},
			shouldReconnect: true,
		},
		{
			name: "InterruptedAtShutdown (code 11600)",
			err: mongo.CommandError{
				Code:    11600,
				Message: "interrupted at shutdown",
			},
			shouldReconnect: true,
		},
		{
			name: "InterruptedDueToReplStateChange (code 11602)",
			err: mongo.CommandError{
				Code:    11602,
				Message: "interrupted due to repl state change",
			},
			shouldReconnect: true,
		},
		{
			name: "NotPrimaryNoSecondaryOk (code 13435)",
			err: mongo.CommandError{
				Code:    13435,
				Message: "not primary no secondary ok",
			},
			shouldReconnect: true,
		},
		{
			name: "NotPrimaryOrSecondary (code 13436)",
			err: mongo.CommandError{
				Code:    13436,
				Message: "not primary or secondary",
			},
			shouldReconnect: true,
		},

		// Some cases that should return false
		{
			name:            "nil error",
			err:             nil,
			shouldReconnect: false,
		},
		{
			name:            "generic error",
			err:             errors.New("some random error"),
			shouldReconnect: false,
		},
		{
			name:            "ErrNoDocuments",
			err:             mongo.ErrNoDocuments,
			shouldReconnect: false,
		},
		{
			name:            "ErrNilDocument",
			err:             mongo.ErrNilDocument,
			shouldReconnect: false,
		},
		{
			name:            "ErrNonStringIndexName",
			err:             mongo.ErrNonStringIndexName,
			shouldReconnect: false,
		},
		{
			name: "ErrClientDisconnected",
			err:  mongo.ErrClientDisconnected,
			// This error indicates that the client is disconnected, but it doesn't necessarily mean
			// that a reconnection should be attempted automatically.
			shouldReconnect: false,
		},
		{
			name: "BSONObjectTooLarge (code 10334)",
			err: mongo.CommandError{
				Code:    10334,
				Message: "BSON object too large",
			},
			shouldReconnect: false,
		},
		{
			name: "DuplicateKey (code 11000)",
			err: mongo.CommandError{
				Code:    11000,
				Message: "duplicate key error",
			},
			shouldReconnect: false,
		},
		{
			name: "Unauthorized (code 13)",
			err: mongo.CommandError{
				Code:    13,
				Message: "not authorized",
			},
			shouldReconnect: false,
		},
		{
			name: "NamespaceNotFound (code 26)",
			err: mongo.CommandError{
				Code:    26,
				Message: "namespace not found",
			},
			shouldReconnect: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ShouldReconnect(tc.err)
			assert.Equal(t, tc.shouldReconnect, result,
				"ShouldReconnect() = %v, want %v for error: %v", result, tc.shouldReconnect, tc.err)
		})
	}
}
