package mgo

import (
	"errors"
	"testing"

	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
)

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
			want: errors.New("no reachable servers"),
		},
		{
			name: "unsupported connection URL opts",
			opts: &model.ClientOpts{
				ConnectionString: "mongodb://localhost:27017/test?foo=1",
				UseSSL:           false,
				Type:             "mongodb",
			},
			want: errors.New("unsupported connection URL option: foo=1"),
		},
		{
			name: "valid connection_string and invalid tls config",
			opts: &model.ClientOpts{
				ConnectionString: "mongodb://localhost:27017/test",
				UseSSL:           true,
				Type:             "mongodb",
				SSLPEMKeyfile:    "invalid_pem_file",
			},
			want: errors.New("unable to load tls certificate: open invalid_pem_file: no such file or directory"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lc := lifeCycle{}
			 gotErr := lc.Connect(test.opts)
			 assert.Equal(t, gotErr, test.want)
		})
	}
}