package utils

import (
	"database/sql"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2"
)

type Info struct {
	Type               DBType
	Version            string
	Name               string
	User               string
	FullVersion        string
	SizeBytes          int64
	StartTime          time.Time
	MaxConnections     int
	CurrentConnections int
	TableCount         int
}

type DBType string

const (
	StandardMongo DBType = "mongo"
	AWSDocumentDB DBType = "docdb"
	CosmosDB      DBType = "cosmosdb"
	PostgresDB    DBType = "postgres"
)

func IsErrNoRows(err error) bool {
	if errors.Is(err, mongo.ErrNoDocuments) {
		return true
	}

	if errors.Is(err, mgo.ErrNotFound) {
		return true
	}

	if errors.Is(err, sql.ErrNoRows) {
		return true
	}

	return false
}
