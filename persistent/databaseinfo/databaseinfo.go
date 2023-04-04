package databaseinfo

type DbType int

const (
	StandardMongo = iota
	AWSDocumentDB
)

type Info struct {
	Type DbType
}
