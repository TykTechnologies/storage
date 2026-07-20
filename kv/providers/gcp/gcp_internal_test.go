package gcp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestProvider(projectID, location string) *gcpProvider {
	return &gcpProvider{cfg: &Config{ProjectID: projectID, Location: location}}
}

func TestVersionName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		projectID string
		location  string
		key       string
		want      string
	}{
		// --- global (no location) ---
		{
			name:      "bare key gets latest",
			projectID: "my-proj",
			key:       "db-password",
			want:      "projects/my-proj/secrets/db-password/versions/latest",
		},
		{
			name:      "explicit version is preserved (no latest appended)",
			projectID: "my-proj",
			key:       "db-password/versions/7",
			want:      "projects/my-proj/secrets/db-password/versions/7",
		},
		{
			// A full-resource-name key must NOT escape the configured project: it is
			// nested under project_id, never used verbatim.
			name:      "full resource name does not escape the configured project",
			projectID: "my-proj",
			key:       "projects/other/secrets/x",
			want:      "projects/my-proj/secrets/projects/other/secrets/x/versions/latest",
		},

		// --- regional (location set) ---
		{
			name:      "regional bare key gets latest under /locations/",
			projectID: "my-proj",
			location:  "europe-west1",
			key:       "db-password",
			want:      "projects/my-proj/locations/europe-west1/secrets/db-password/versions/latest",
		},
		{
			name:      "regional explicit version is preserved",
			projectID: "my-proj",
			location:  "europe-west1",
			key:       "db-password/versions/2",
			want:      "projects/my-proj/locations/europe-west1/secrets/db-password/versions/2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newTestProvider(tt.projectID, tt.location)
			require.Equal(t, tt.want, p.versionName(tt.key))
		})
	}
}

func TestSecretName(t *testing.T) {
	t.Parallel()

	t.Run("global", func(t *testing.T) {
		t.Parallel()

		p := newTestProvider("my-proj", "")
		require.Equal(t, "projects/my-proj/secrets/db-password", p.secretName("db-password"))
	})

	t.Run("regional", func(t *testing.T) {
		t.Parallel()

		p := newTestProvider("my-proj", "europe-west1")
		require.Equal(t,
			"projects/my-proj/locations/europe-west1/secrets/db-password",
			p.secretName("db-password"),
		)
	})
}

func TestProjectParent(t *testing.T) {
	t.Parallel()

	t.Run("global", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "projects/my-proj", newTestProvider("my-proj", "").projectParent())
	})

	t.Run("regional", func(t *testing.T) {
		t.Parallel()

		require.Equal(t,
			"projects/my-proj/locations/europe-west1",
			newTestProvider("my-proj", "europe-west1").projectParent(),
		)
	})
}
