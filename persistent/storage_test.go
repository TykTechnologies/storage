package persistent

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPersistentStorage(t *testing.T) {
	var testCases []string

	switch os.Getenv("DB") {
	case "mongo":
		testCases = []string{Mgo, OfficialMongo, "unvalid"}

		// skip problematic versions
		if os.Getenv("DB_VERSION") == "6" || os.Getenv("DB_VERSION") == "7" {
			testCases = []string{OfficialMongo, "unvalid"}
		}

	case "postgres":
		testCases = []string{Postgres, "unvalid"}

	default:
		t.Skip("DB_TYPE not set, skipping TestNewPersistentStorage")
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			_, err := NewPersistentStorage(&ClientOpts{
				ConnectionString: connStrFor(tc),
				UseSSL:           false,
				Type:             tc,
			})

			if tc == "unvalid" {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func connStrFor(driver string) string {
	switch driver {
	case Postgres:
		return "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable"
	default: // Mongo
		return "mongodb://localhost:27017/test"
	}
}
