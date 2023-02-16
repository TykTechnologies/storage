package model

import (
	"context"

	"github.com/TykTechnologies/storage/persistent/id"
)

type PersistentStorage interface {
	// Insert a DbObject into the database
	Insert(context.Context, DBTable, id.DBObject) error
	// NewObjectID returns a new id object
	NewObjectID() id.ObjectID
	// ObjectIdHex returns an object id created from an existent id
	ObjectIdHex(id string) id.ObjectID
  // Delete a DbObject from the database
	Delete(ctx context.Context, table DBTable, row id.DBObject) error
	// Checking if an error is a "no rows error"
	IsErrNoRows(err error) bool
}
