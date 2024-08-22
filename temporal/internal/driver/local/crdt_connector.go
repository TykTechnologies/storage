package local

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/TykTechnologies/cannery/v2/connector"
	"github.com/TykTechnologies/storage/temporal/model"
)

type CRDTStorConnector struct {
	cfg       *CRDTConfig
	connected bool
	Conn      *connector.Connector
	mx        sync.Mutex
}

// Disconnect disconnects from the backend
func (c *CRDTStorConnector) Disconnect(context.Context) error {
	if c.Conn == nil {
		return nil
	}

	if c.cfg.MockDisconnect {
		return nil
	}

	c.connected = false
	c.Conn.Stop()
	return nil
}

// Ping executes a ping to the backend
func (c *CRDTStorConnector) Ping(context.Context) error {
	if !c.connected {
		return fmt.Errorf("not connected")
	}

	return nil
}

// Type returns the  connector type
func (c *CRDTStorConnector) Type() string {
	return model.CRDTType
}

// As converts i to driver-specific types.
// Same concept as https://gocloud.dev/concepts/as/ but for connectors.
func (c *CRDTStorConnector) As(i interface{}) bool {
	var ok bool
	i, ok = i.(*CRDTStorConnector)
	return ok
}

func (c *CRDTStorConnector) Connect(i interface{}) error {
	logger := slog.Default()
	cntr := connector.NewConnector(c.cfg.TagName, c.cfg.TagName, 3, logger, c.cfg.PrimaryKey, c.cfg.DBName)

	cntr.ListenAddr = c.cfg.ListenAddr
	err := cntr.Initialize(c.cfg.BootstrapAddr)
	if err != nil {
		return err
	}

	c.Conn = cntr
	c.connected = true

	return nil
}
