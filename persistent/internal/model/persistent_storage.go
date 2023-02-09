package model

import "context"

type PersistentStorage interface {
	Insert(context.Context, DBTable, DBObject) error
}
