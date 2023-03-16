package model

import (
	"context"

	"github.com/TykTechnologies/storage/persistent/dbm"
	"github.com/TykTechnologies/storage/persistent/index"

	"github.com/TykTechnologies/storage/persistent/id"
)

type PersistentStorage interface {
	// Insert a DbObject into the database
	Insert(context.Context, id.DBObject) error
	// Delete a DbObject from the database
	Delete(context.Context, id.DBObject, ...dbm.DBM) error
	// Update a DbObject in the database
	Update(context.Context, id.DBObject, ...dbm.DBM) error
	// Count counts all rows for a DBTable if no filter dbm.DBM given.
	// If a filter dbm.DBM is specified it will count the rows given the built query for that filter.
	// If multiple filters dbm.DBM are specified, it will return an error.
	// In case of an error, the count result is going to be 0.
	Count(ctx context.Context, row id.DBObject, filter ...dbm.DBM) (count int, error error)
	// Query one or multiple DBObjects from the database
	Query(context.Context, id.DBObject, interface{}, dbm.DBM) error
	// BulkUpdate updates multiple rows
	BulkUpdate(context.Context, []id.DBObject, ...dbm.DBM) error
	// UpdateAll executes the update query dbm.DBM over
	// the elements filtered by query dbm.DBM in the row id.DBObject collection
	UpdateAll(ctx context.Context, row id.DBObject, query, update dbm.DBM) error
	// IsErrNoRows Checking if an error is a "no rows error"
	IsErrNoRows(err error) bool
	// Drop drops the collection given the TableName() of the id.DBObject
	Drop(context.Context, id.DBObject) error
	// CreateIndex creates an index.Index in row id.DBObject TableName()
	CreateIndex(ctx context.Context, row id.DBObject, index index.Index) error
	// GetIndexes returns all the index.Index associated to row id.DBObject
	GetIndexes(ctx context.Context, row id.DBObject) ([]index.Index, error)
	// Ping checks if the database is reachable
	Ping(context.Context) error
	// HasTable checks if the table/collection exists
	HasTable(context.Context, string) (bool, error)
	// DropDatabase removes the database
	DropDatabase(ctx context.Context) error
	// AutoMigrate creates the table/collection if it doesn't exist
	AutoMigrate(context.Context, []id.DBObject, ...dbm.DBM) error
}
