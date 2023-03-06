package model

import (
	"context"

	"github.com/TykTechnologies/storage/persistent/id"
)

type DBM map[string]interface{}

type PersistentStorage interface {
	// Insert a DbObject into the database
	Insert(context.Context, id.DBObject) error
	// Delete a DbObject from the database
	Delete(context.Context, id.DBObject) error
	// Update a DbObject in the database
	Update(ctx context.Context, row id.DBObject, query ...DBM) error // Count counts all rows for a DBTable
	Count(context.Context, id.DBObject) (int, error)
	// Query one or multiple DBObjects from the database
	Query(context.Context, id.DBObject, interface{}, DBM) error
	// DeleteWhere deletes all rows that match the query
	DeleteWhere(context.Context, id.DBObject, DBM) error
	// IsErrNoRows Checking if an error is a "no rows error"
	IsErrNoRows(err error) bool
}
