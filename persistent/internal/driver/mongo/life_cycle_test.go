//go:build mongo
// +build mongo

package mongo

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/utils"
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
	defaultClient.SetTimeout(types.DEFAULT_CONN_TIMEOUT)
	defaultClient.ApplyURI(validMongoURL)
	defaultClient.SetReadPreference(readpref.Primary())
	defaultClient.SetDirect(false)

	tcs := []struct {
		name           string
		opts           *types.ClientOpts
		expectedOpts   func() *options.ClientOptions
		shouldErr      bool
		expectedErrMsg string
	}{
		{
			name: "default options",
			opts: &types.ClientOpts{
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
			opts: &types.ClientOpts{
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
			opts: &types.ClientOpts{
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
			opts: &types.ClientOpts{
				ConnectionString: "invalid-uri",
			},
			expectedOpts: func() *options.ClientOptions {
				return nil
			},
			shouldErr:      true,
			expectedErrMsg: "error parsing uri: scheme must be \"mongodb\" or \"mongodb+srv\"",
		},
		{
			name: "direct connection",
			opts: &types.ClientOpts{
				ConnectionString: validMongoURL,
				UseSSL:           true,
				DirectConnection: true,
			},
			expectedOpts: func() *options.ClientOptions {
				cl := *defaultClient
				cl.SetTLSConfig(&tls.Config{})
				cl.SetDirect(true)
				return &cl
			},
			shouldErr: false,
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
		opts *types.ClientOpts
		want error
	}{
		{
			name: "valid connection_string",
			opts: &types.ClientOpts{
				ConnectionString: "mongodb://localhost:27017/test",
				UseSSL:           false,
				Type:             "mongodb",
			},
			want: nil,
		},
		{
			name: "invalid connection_string",
			opts: &types.ClientOpts{
				ConnectionString: "invalid_conn_string",
				UseSSL:           false,
				Type:             "mongodb",
			},
			want: errors.New("invalid connection string, no prefix found"),
		},
		{
			name: "valid connection_string and invalid tls config",
			opts: &types.ClientOpts{
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

func TestParseURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    string
		wantErr bool
	}{
		{
			name: "valid connection_string with special characters",
			url:  "mongodb://lt_tyk:6}3cZQU.9KvM/hVR4qkm-hHqZTu3yg=G@localhost:27017/tyk_analytics",
			want: "mongodb://lt_tyk:6%7D3cZQU.9KvM%2FhVR4qkm-hHqZTu3yg%3DG@localhost:27017/tyk_analytics",
		},
		{
			name: "already encoded valid url",
			url:  "mongodb://lt_tyk:6%7D3cZQU.9KvM%2FhVR4qkm-hHqZTu3yg%3DG@localhost:27017/tyk_analytics",
			want: "mongodb://lt_tyk:6%7D3cZQU.9KvM%2FhVR4qkm-hHqZTu3yg%3DG@localhost:27017/tyk_analytics",
		},
		{
			name:    "invalid connection_string",
			url:     "invalid_conn_string",
			want:    "",
			wantErr: true,
		},
		{
			name: "valid connection string with @",
			url:  "mongodb://user:p@ssword@localhost:27017",
			want: "mongodb://user:p@ssword@localhost:27017",
		},
		{
			name: "valid connection string with @ and /",
			url:  "mongodb://u=s@r:p@sswor/d@localhost:27017/test",
			want: "mongodb://u%3Ds@r:p@sswor/d@localhost:27017/test",
		},
		{
			name: "valid connection string with @ and / and '?' outside of the credentials part",
			url:  "mongodb://user:p@sswor/d@localhost:27017/test?authSource=admin",
			want: "mongodb://user:p@sswor/d@localhost:27017/test?authSource=admin",
		},
		{
			name: "special characters and multiple hosts",
			url:  "mongodb://user:p@sswor/d@localhost:27017,localhost:27018/test?authSource=admin",
			want: "mongodb://user:p@sswor/d@localhost:27017,localhost:27018/test?authSource=admin",
		},
		{
			name: "url without credentials",
			url:  "mongodb://localhost:27017/test?authSource=admin",
			want: "mongodb://localhost:27017/test?authSource=admin",
		},
		{
			name:    "invalid connection string",
			url:     "test",
			want:    "",
			wantErr: true,
		},
		{
			name: "srv connection string",
			url:  "mongodb+srv://tyk:tyk@clur0.zlgl.mongodb.net/tyk?w=majority",
			want: "mongodb+srv://tyk:tyk@clur0.zlgl.mongodb.net/tyk?w=majority",
		},
		{
			name: "srv connection string with special characters",
			url:  "mongodb+srv://tyk:p@ssword@clur0.zlgl.mongodb.net/tyk?w=majority",
			want: "mongodb+srv://tyk:p@ssword@clur0.zlgl.mongodb.net/tyk?w=majority",
		},
		{
			name:    "connection string without username",
			url:     "mongodb://:password@localhost:27017/test",
			want:    "",
			wantErr: true,
		},
		{
			name: "connection string without password",
			url:  "mongodb://user:@localhost:27017/test",
			want: "mongodb://user@localhost:27017/test",
		},
		{
			name: "connection string without host",
			url:  "mongodb://user:password@/test",
			want: "mongodb://user:password@/test",
		},
		{
			name: "connection string without database",
			url:  "mongodb://user:password@localhost:27017",
			want: "mongodb://user:password@localhost:27017",
		},
		{
			name: "cosmosdb url",
			url:  "mongodb+srv://4-0-qa:zFAQ==@4-0-qa.azure:10/a1?appName=@4-testing@&maxIdleTimeMS=120000",
			want: "mongodb+srv://4-0-qa:zFAQ%3D%3D@4-0-qa.azure:10/a1?appName=@4-testing@&maxIdleTimeMS=120000",
		},
		{
			name: "already encoded cosmosdb url",
			url:  "mongodb+srv://4-0-qa:zFAQ%3D%3D@4-0-qa.azure:10/a1?appName=@4-testing@&maxIdleTimeMS=120000",
			want: "mongodb+srv://4-0-qa:zFAQ%3D%3D@4-0-qa.azure:10/a1?appName=@4-testing@&maxIdleTimeMS=120000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsedURL, _, err := parseURL(test.url)
			assert.Equal(t, test.want, parsedURL)
			assert.Equal(t, test.wantErr, err != nil)
		})
	}

}

func TestClose(t *testing.T) {
	lc := &lifeCycle{}
	opts := &types.ClientOpts{
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
	opts := &types.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
	}

	err := lc.Connect(opts)
	assert.Nil(t, err)

	dbType := lc.DBType()
	assert.Equal(t, utils.StandardMongo, dbType)
}

func TestIsOptSep(t *testing.T) {
	tests := []struct {
		input rune
		want  bool
	}{
		{';', true},
		{'&', true},
		{':', false},
		{'a', false},
		{'1', false},
		{' ', false},
		{'\t', false},
		{'\n', false},
		{'!', false},
	}

	for _, test := range tests {
		got := isOptSep(test.input)
		if got != test.want {
			t.Errorf("isOptSep(%q) = %v, want %v", test.input, got, test.want)
		}
	}
}
