//go:build postgres || postgres16.10 || postgres16.1 || postgres15.0 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.10 postgres16.1 postgres15.0 postgres15 postgres14.11 postgres13.3 postgres12.22

package persistent_test

import (
	"context"
	"os"
	"testing"

	"github.com/TykTechnologies/storage/persistent"
	"github.com/TykTechnologies/storage/persistent/internal/testutil"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/require"
)

type pgConformanceEntity struct {
	ID   model.ObjectID `json:"id" gorm:"primaryKey"`
	Name string         `json:"name"`
}

func (e *pgConformanceEntity) TableName() string             { return "conformance_entities" }
func (e *pgConformanceEntity) GetObjectID() model.ObjectID   { return e.ID }
func (e *pgConformanceEntity) SetObjectID(id model.ObjectID) { e.ID = id }

func postgresConnStr() string {
	if dsn := os.Getenv("postgres_test_dsn"); dsn != "" {
		return dsn
	}

	// Fallback matches the Docker credentials set by bin/Taskfile-db.yml start-postgres.
	// In CI, postgres_test_dsn is set explicitly in the workflow env.
	return "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"
}

func TestConformancePostgres(t *testing.T) {
	storage, err := persistent.NewPersistentStorage(&persistent.ClientOpts{
		ConnectionString: postgresConnStr(),
		Type:             persistent.Postgres,
	})
	require.NoError(t, err)
	require.NoError(t, storage.Ping(context.Background()))

	testutil.RunSuite(t, testutil.Suite{
		Storage:   storage,
		NewObject: func() model.DBObject { return &pgConformanceEntity{} },
		IDFilter:  func(id model.ObjectID) model.DBM { return model.DBM{"id": id} },
	})
}
