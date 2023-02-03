package model

// DBTable is an interface that should be implemented by
// database models in order to perform CRUD operations
type DBTable interface {
	TableName() string
}
