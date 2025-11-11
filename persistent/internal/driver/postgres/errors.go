package postgres

import (
	"errors"
	"github.com/TykTechnologies/storage/persistent/internal/types"
)

var (
	ErrorSessionClosed      = errors.New(types.ErrorSessionClosed)
	ErrorEmptyRow           = errors.New(types.ErrorEmptyRow)
	ErrorEmptyTableName     = errors.New(types.ErrorEmptyTableName)
	ErrorMultipleDBM        = errors.New(types.ErrorMultipleDBM)
	ErrorNilObject          = errors.New(types.ErrorNilObject)
	ErrorCollectionNotFound = errors.New(types.ErrorCollectionNotFound)
	ErrorIndexEmpty         = errors.New(types.ErrorIndexEmpty)
	ErrorNilContext         = errors.New(types.ErrorNilContext)
	ErrorIndexAlreadyExist  = errors.New(types.ErrorIndexAlreadyExist)
	ErrorIndexComposedTTL   = errors.New(types.ErrorIndexComposedTTL)
	ErrorRowOptDiffLength   = errors.New(types.ErrorRowOptDiffLenght)
	ErrorEmptyConnStr       = errors.New("empty connection string")
)
