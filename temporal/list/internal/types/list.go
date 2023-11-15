package types

import (
	"context"
)

type List interface {
	// Remove the first count occurrences of elements equal to element from the list stored at key.
	Remove(ctx context.Context, key string, count int64, element interface{}) (int64, error)

	// Returns the specified elements of the list stored at key.
	// The offsets start and stop are zero-based indexes.
	Range(ctx context.Context, key string, start, stop int64) ([]string, error)

	// Returns the length of the list stored at key.
	Length(ctx context.Context, key string) (int64, error)

	// Insert all the specified values at the head of the list stored at key.
	// If key does not exist, it is created.
	// pipelined: If true, the operation is pipelined and executed in a single roundtrip.
	Prepend(ctx context.Context, pipelined bool, key string, values ...[]byte) error

	// Insert all the specified values at the tail of the list stored at key.
	// If key does not exist, it is created.
	// pipelined: If true, the operation is pipelined and executed in a single roundtrip.
	Append(ctx context.Context, pipelined bool, key string, values ...[]byte) error

	// Pop removes and returns the first count elements of the list stored at key.
	// If stop is -1, all the elements from start to the end of the list are removed and returned.
	Pop(ctx context.Context, key string, stop int64) ([]string, error)
}
