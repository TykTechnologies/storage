package utils

import "context"

type MockConnector struct{}

func (m MockConnector) Type() string {
	return "mock"
}

func (m MockConnector) As(interface{}) bool {
	return false
}

func (m MockConnector) Disconnect(context.Context) error {
	return nil
}

func (m MockConnector) Ping(context.Context) error {
	return nil
}
