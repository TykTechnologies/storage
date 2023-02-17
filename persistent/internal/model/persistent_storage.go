package model

import (
	"context"

	"github.com/TykTechnologies/storage/persistent/id"
)

type PersistentStorage interface {
	// Insert a DbObject into the database
	Insert(context.Context, id.DBObject) error
	// Delete a DbObject from the database
	Delete(context.Context, id.DBObject) error
	// Update a DbObject in the database
	Update(context.Context, id.DBObject) error
	// IsErrNoRows Checking if an error is a "no rows error"
	IsErrNoRows(err error) bool
}
