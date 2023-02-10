package model

import (
	"context"
	"github.com/TykTechnologies/storage/persistent"
)

type PersistentStorage interface {
	Insert(context.Context, DBTable, persistent.DBObject) error
}
