package model

const (
	ErrorRowQueryDiffLenght        = "only one query per row is allowed"
	ErrorEmptyRow                  = "rows cannot be empty"
	ErrorMultipleQueryForSingleRow = "multiple queries for one row"
	ErrorReconnecting              = "error reconnecting"
	ErrorIndexEmpty                = "index keys cannot be empty"
	ErrorIndexAlreadyExist         = "index already exists with a different name"
	ErrorIndexComposedTTL          = "TTL indexes are single-field indexes, compound indexes do not support TTL"
	ErrorSessionClosed             = "session closed"
)
