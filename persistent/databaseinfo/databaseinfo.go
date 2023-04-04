package databaseinfo

type Info struct {
	Type    DBType
	Version string
}

type DBType string

const (
	StandardMongo DBType = "mongo"
	AWSDocumentDB DBType = "docdb"
	CosmosDB      DBType = "cosmosdb"
)
