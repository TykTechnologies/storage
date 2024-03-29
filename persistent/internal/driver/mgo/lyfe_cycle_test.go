//go:build mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mgo

import (
	"errors"
	"testing"

	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/utils"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
)

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
				ConnectionString:  "invalid_conn_string",
				UseSSL:            false,
				Type:              "mongodb",
				ConnectionTimeout: 1,
			},
			want: errors.New("no reachable servers"),
		},
		{
			name: "unsupported connection URL opts",
			opts: &types.ClientOpts{
				ConnectionString: "mongodb://localhost:27017/test?foo=1",
				UseSSL:           false,
				Type:             "mongodb",
			},
			want: errors.New("unsupported connection URL option: foo=1"),
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

func TestSetSessionConsistency(t *testing.T) {
	tcs := []struct {
		name         string
		givenMode    string
		expectedMode mgo.Mode
	}{
		{
			name:         "default consistency",
			givenMode:    "",
			expectedMode: mgo.Strong,
		},
		{
			name:         "eventual consistency",
			givenMode:    "eventual",
			expectedMode: mgo.Eventual,
		},
		{
			name:         "monotonic consistency",
			givenMode:    "monotonic",
			expectedMode: mgo.Monotonic,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			lc := &lifeCycle{}
			opts := &types.ClientOpts{
				ConnectionString:   "mongodb://localhost:27017/test",
				UseSSL:             false,
				Type:               "mongodb",
				SessionConsistency: tc.givenMode,
			}

			err := lc.Connect(opts)
			assert.Nil(t, err)

			defer lc.Close()

			lc.setSessionConsistency(opts)

			assert.NotNil(t, lc.session)
			assert.Equal(t, tc.expectedMode, lc.session.Mode())
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

	assert.Nil(t, lc.session)
	assert.Nil(t, lc.db)

	err = lc.Close()
	assert.NotNil(t, err)
	assert.Equal(t, "closing a no connected database", err.Error())
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
