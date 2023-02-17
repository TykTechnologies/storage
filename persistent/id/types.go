package id

type DBObject interface {
	GetObjectID() OID
	SetObjectID(id OID)
	TableName() string
}
