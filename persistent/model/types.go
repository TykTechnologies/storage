package model

type DBObject interface {
	GetObjectId() ObjectId
	SetObjectId(id ObjectId)
	TableName() string
}
