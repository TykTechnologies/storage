//go:build mongo
// +build mongo

package mongo

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/persistent/databaseinfo"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

func TestGetReadPrefFromConsistency(t *testing.T) {
	testCases := []struct {
		consistency string
		expected    *readpref.ReadPref
	}{
		{
			consistency: "eventual",
			expected:    readpref.Nearest(),
		},
		{
			consistency: "monotonic",
			expected:    readpref.PrimaryPreferred(),
		},
		{
			consistency: "strong",
			expected:    readpref.Primary(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.consistency, func(t *testing.T) {
			actual := getReadPrefFromConsistency(tc.consistency)
			assert.EqualValues(t, tc.expected, actual)
		})
	}
}

func TestMongoOptsBuilder(t *testing.T) {
	validMongoURL := "mongodb://localhost:27017"

	defaultClient := options.Client()
	defaultClient.SetTimeout(model.DEFAULT_CONN_TIMEOUT)
	defaultClient.ApplyURI(validMongoURL)
	defaultClient.SetReadPreference(readpref.Primary())

	tcs := []struct {
		name           string
		opts           *model.ClientOpts
		expectedOpts   func() *options.ClientOptions
		shouldErr      bool
		expectedErrMsg string
	}{
		{
			name: "default options",
			opts: &model.ClientOpts{
				ConnectionString: validMongoURL,
			},
			expectedOpts: func() *options.ClientOptions {
				cl := *defaultClient
				return &cl
			},
			shouldErr: false,
		},
		{
			name: "use SSL",
			opts: &model.ClientOpts{
				ConnectionString: validMongoURL,
				UseSSL:           true,
			},
			expectedOpts: func() *options.ClientOptions {
				cl := *defaultClient
				cl.SetTLSConfig(&tls.Config{})
				return &cl
			},
			shouldErr: false,
		},
		{
			name: "connection timeout",
			opts: &model.ClientOpts{
				ConnectionString:  validMongoURL,
				ConnectionTimeout: 20,
			},
			expectedOpts: func() *options.ClientOptions {
				cl := *defaultClient
				cl.SetTimeout(20 * time.Second)
				return &cl
			},
			shouldErr: false,
		},
		{
			name: "invalid URI",
			opts: &model.ClientOpts{
				ConnectionString: "invalid-uri",
			},
			expectedOpts: func() *options.ClientOptions {
				return nil
			},
			shouldErr:      true,
			expectedErrMsg: "error parsing uri: scheme must be \"mongodb\" or \"mongodb+srv\"",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := mongoOptsBuilder(tc.opts)
			assert.Equal(t, tc.expectedOpts(), opts)
			assert.Equal(t, tc.shouldErr, err != nil)
			if err != nil {
				assert.Equal(t, tc.expectedErrMsg, err.Error())
			}
		})
	}
}

func TestConnect(t *testing.T) {
	tests := []struct {
		name string
		opts *model.ClientOpts
		want error
	}{
		{
			name: "valid connection_string",
			opts: &model.ClientOpts{
				ConnectionString: "mongodb://localhost:27017/test",
				UseSSL:           false,
				Type:             "mongodb",
			},
			want: nil,
		},
		{
			name: "invalid connection_string",
			opts: &model.ClientOpts{
				ConnectionString: "invalid_conn_string",
				UseSSL:           false,
				Type:             "mongodb",
			},
			want: errors.New("invalid connection string"),
		},
		{
			name: "valid connection_string and invalid tls config",
			opts: &model.ClientOpts{
				ConnectionString: "mongodb://localhost:27017/test",
				UseSSL:           true,
				Type:             "mongodb",
				SSLPEMKeyfile:    "invalid_pem_file",
			},
			want: errors.New("failure reading certificate file: open invalid_pem_file: no such file or directory"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lc := &lifeCycle{}
			gotErr := lc.Connect(test.opts)
			assert.Equal(t, gotErr, test.want)

			defer lc.Close()
		})
	}
}

func TestClose(t *testing.T) {
	lc := &lifeCycle{}
	opts := &model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
	}

	err := lc.Connect(opts)
	assert.Nil(t, err)

	err = lc.Close()
	assert.Nil(t, err)

	assert.NotNil(t, lc.client.Ping(context.Background(), nil))

	err = lc.Close()
	assert.NotNil(t, err)
	assert.Equal(t, "client is disconnected", err.Error())
}

func TestDBType(t *testing.T) {
	lc := &lifeCycle{}
	opts := &model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
	}

	err := lc.Connect(opts)
	assert.Nil(t, err)

	dbType := lc.DBType()
	assert.Equal(t, utilsStandardMongo, dbType)
}
