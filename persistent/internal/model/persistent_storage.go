package model

import (
	"context"
	"github.com/TykTechnologies/storage/persistent/id"
)

type PersistentStorage interface {
	Insert(context.Context, DBTable, id.DBObject) error
	NewObjectID() id.ObjectID
	ObjectIdHex(s string) id.ObjectID
}
