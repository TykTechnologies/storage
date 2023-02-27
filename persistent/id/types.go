package id

type DBObject interface {
	GetObjectID() ObjectId
	SetObjectID(id ObjectId)
	TableName() string
}
