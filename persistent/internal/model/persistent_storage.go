package model

import (
	"context"

	"github.com/TykTechnologies/storage/persistent/dbm"
	"github.com/TykTechnologies/storage/persistent/index"

	"github.com/TykTechnologies/storage/persistent/id"
)

type PersistentStorage interface {
	// Insert a DbObject into the database
	Insert(context.Context, ...id.DBObject) error
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
	// Migrate creates the table/collection if it doesn't exist
	Migrate(context.Context, []id.DBObject, ...dbm.DBM) error
	// DBTableStats retrieves statistics for a specified table in the database.
	// The function takes a context.Context and an id.DBObject as input parameters,
	// where the DBObject represents the table to get stats for.
	// The result is decoded into a dbm.DBM object, along with any error that occurred during the command execution.
	// Example: stats["capped"] -> true
	DBTableStats(ctx context.Context, row id.DBObject) (dbm.DBM, error)
	// Aggregate performs an aggregation query on the row id.DBObject collection
	// query is the aggregation pipeline to be executed
	// it returns the aggregation result and an error if any
	Aggregate(ctx context.Context, row id.DBObject, query []dbm.DBM) ([]dbm.DBM, error)
	// CleanIndexes removes all the indexes from the row id.DBObject collection
	CleanIndexes(ctx context.Context, row id.DBObject) error
	// Upsert performs an upsert operation on the row id.DBObject collection
	// query is the filter to be used to find the document to update
	// update is the update to be applied to the document
	// result is the result of the upsert operation
	// it returns an error if any
	Upsert(ctx context.Context, row id.DBObject, query, update dbm.DBM, result interface{}) error
}
