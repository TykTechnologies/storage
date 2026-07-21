package gcp

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		location  string
		transport string
		want      string
	}{
		// --- global: no endpoint override regardless of transport ---
		{
			name: "global default transport has no endpoint override",
			want: "",
		},
		{
			name:      "global grpc has no endpoint override",
			transport: "grpc",
			want:      "",
		},
		{
			name:      "global rest has no endpoint override",
			transport: "rest",
			want:      "",
		},

		// --- regional: format follows the transport ---
		{
			name:     "regional default transport uses the grpc host:443 endpoint",
			location: "europe-west1",
			want:     "secretmanager.europe-west1.rep.googleapis.com:443",
		},
		{
			name:      "regional grpc uses the grpc host:443 endpoint",
			location:  "europe-west1",
			transport: "grpc",
			want:      "secretmanager.europe-west1.rep.googleapis.com:443",
		},
		{
			name:      "regional rest uses the https URL endpoint",
			location:  "europe-west1",
			transport: "rest",
			want:      "https://secretmanager.europe-west1.rep.googleapis.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &gcpProvider{cfg: &Config{
				ProjectID: "proj",
				Location:  tt.location,
				Transport: tt.transport,
			}}
			require.Equal(t, tt.want, p.endpoint())
		})
	}
}

func TestInit_RESTTransportBuildsClient(t *testing.T) {
	t.Parallel()

	p := &gcpProvider{
		cfg:      &Config{ProjectID: "proj", Transport: "rest"},
		testOpts: []option.ClientOption{option.WithoutAuthentication()},
	}

	require.NoError(t, p.Init(t.Context()))
	require.NotNil(t, p.client, "REST transport must build a client")
	t.Cleanup(func() { _ = p.Close(t.Context()) })
}
