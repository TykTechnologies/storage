package testutil

import (
	"context"
	"testing"
)

func TestType(t *testing.T) {
	m := &StubConnector{}

	if m.Type() != "mock" {
		t.Errorf("Type() = %s; want mock", m.Type())
	}
}

func TestConnect(t *testing.T) {
	m := &StubConnector{}

	if err := m.Connect(context.Background()); err != nil {
		t.Errorf("Connect() = %v; want nil", err)
	}
}

func TestDisconnect(t *testing.T) {
	m := &StubConnector{}

	if err := m.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect() = %v; want nil", err)
	}
}

func TestPing(t *testing.T) {
	m := &StubConnector{}

	if err := m.Ping(context.Background()); err != nil {
		t.Errorf("Ping() = %v; want nil", err)
	}
}

func TestAs(t *testing.T) {
	m := &StubConnector{}

	if m.As(nil) {
		t.Errorf("As() = true; want false")
	}
}
