package model

import (
	"context"

	"github.com/TykTechnologies/storage/persistent/dbm"

	"github.com/TykTechnologies/storage/persistent/id"
)

type PersistentStorage interface {
	// Insert a DbObject into the database
	Insert(context.Context, id.DBObject) error
	// Delete a DbObject from the database
	Delete(context.Context, id.DBObject) error
	// Update a DbObject in the database
	Update(context.Context, id.DBObject, ...DBM) error
	// Count counts all rows for a DBTable
	Count(context.Context, id.DBObject) (int, error)
	// Query one or multiple DBObjects from the database
	Query(context.Context, id.DBObject, interface{}, dbm.DBM) error
	// DeleteWhere deletes all rows that match the query

	DeleteWhere(context.Context, id.DBObject, dbm.DBM) error
	// UpdateMany updates multiple rows
	UpdateMany(context.Context, []id.DBObject, ...dbm.DBM) error

	// IsErrNoRows Checking if an error is a "no rows error"
	IsErrNoRows(err error) bool
	// Drop drops the collection given the TableName() of the id.DBObject
	Drop(context.Context, id.DBObject) error
}
