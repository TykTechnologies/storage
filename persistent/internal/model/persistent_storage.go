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
	Update(context.Context, id.DBObject) error
	// Query select multiple records for table, writes to dest.
	Query(context.Context, string, interface{}, DBM) error
	// Count counts all rows for a DBTable
	Count(context.Context, id.DBObject) (int, error)
	// IsErrNoRows Checking if an error is a "no rows error"
	IsErrNoRows(err error) bool
}
