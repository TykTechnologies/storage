package utils

import (
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2"
)

type Info struct {
	Type               DBType
	Version            string    `json:"version"`
	Name               string    `json:"name"`
	User               string    `json:"user;omitempty"`
	FullVersion        string    `json:"full_version;omitempty"`
	SizeBytes          int64     `json:"size_bytes;omitempty"`
	StartTime          time.Time `json:"start_time;omitempty"`
	MaxConnections     int       `json:"max_connections;omitempty"`
	CurrentConnections int       `json:"current_connections;omitempty"`
	TableCount         int       `json:"table_count;omitempty"`
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

	return false
}
