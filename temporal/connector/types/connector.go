package types

import "context"

type Connector interface {
	// Disconnect disconnects from the backend
	Disconnect(context.Context) error

	// Ping executes a ping to the backend
	Ping(context.Context) error

	// Type returns the  connector type
	Type() string

	// As converts i to driver-specific types.
	// Same concept as https://gocloud.dev/concepts/as/ but for connectors.
	As(i interface{}) bool
}
