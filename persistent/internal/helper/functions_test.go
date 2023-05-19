package helper

import (
	"bytes"
	"errors"
	"log"
	"os"
	"testing"
	"time"

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

func TestParsePassword(t *testing.T) {
	tests := []struct {
		name               string
		originalConnString string
		expectedConnString string
	}{
		{
			name:               "valid connection string",
			originalConnString: "mongodb://user:password@localhost:27017/test",
			expectedConnString: "mongodb://user:password@localhost:27017/test",
		},
		{
			name:               "valid connection string with @",
			originalConnString: "mongodb://user:p@ssword@localhost:27017",
			expectedConnString: "mongodb://user:p%40ssword@localhost:27017",
		},
		{
			name:               "valid connection string with @ and /",
			originalConnString: "mongodb://user:p@sswor/d@localhost:27017/test",
			expectedConnString: "mongodb://user:p%40sswor%2Fd@localhost:27017/test",
		},
		{
			name:               "valid connection string with @ and / and '?' outside of the credentials part",
			originalConnString: "mongodb://user:p@sswor/d@localhost:27017/test?authSource=admin",
			expectedConnString: "mongodb://user:p%40sswor%2Fd@localhost:27017/test?authSource=admin",
		},
		{
			name:               "special characters and multiple hosts",
			originalConnString: "mongodb://user:p@sswor/d@localhost:27017,localhost:27018/test?authSource=admin",
			expectedConnString: "mongodb://user:p%40sswor%2Fd@localhost:27017,localhost:27018/test?authSource=admin",
		},
		{
			name:               "url without credentials",
			originalConnString: "mongodb://localhost:27017/test?authSource=admin",
			expectedConnString: "mongodb://localhost:27017/test?authSource=admin",
		},
		{
			name:               "invalid connection string",
			originalConnString: "test",
			expectedConnString: "test",
		},
		{
			name:               "connection string full of special characters",
			originalConnString: "mongodb://user:p@ss:/?#[]wor/d@localhost:27017,localhost:27018",
			expectedConnString: "mongodb://user:p%40ss%3A%2F%3F%23%5B%5Dwor%2Fd@localhost:27017,localhost:27018",
		},
		{
			name:               "srv connection string",
			originalConnString: "mongodb+srv://tyk:tyk@clur0.zlgl.mongodb.net/tyk?w=majority",
			expectedConnString: "mongodb+srv://tyk:tyk@clur0.zlgl.mongodb.net/tyk?w=majority",
		},
		{
			name:               "srv connection string with special characters",
			originalConnString: "mongodb+srv://tyk:p@ssword@clur0.zlgl.mongodb.net/tyk?w=majority",
			expectedConnString: "mongodb+srv://tyk:p%40ssword@clur0.zlgl.mongodb.net/tyk?w=majority",
		},
		{
			name:               "connection string without username",
			originalConnString: "mongodb://:password@localhost:27017/test",
			expectedConnString: "mongodb://:password@localhost:27017/test",
		},
		{
			name:               "connection string without password",
			originalConnString: "mongodb://user:@localhost:27017/test",
			expectedConnString: "mongodb://user:@localhost:27017/test",
		},
		{
			name:               "connection string without host",
			originalConnString: "mongodb://user:password@/test",
			expectedConnString: "mongodb://user:password@/test",
		},
		{
			name:               "connection string without database",
			originalConnString: "mongodb://user:password@localhost:27017",
			expectedConnString: "mongodb://user:password@localhost:27017",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connString := ParsePassword(test.originalConnString)
			assert.Equal(t, test.expectedConnString, connString)
		})
	}
}
