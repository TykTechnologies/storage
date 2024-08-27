package local

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/TykTechnologies/cannery/v2/publisher"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/ipfs/go-datastore"
)

type CRDTStore struct {
	cfg       *model.CRDTConfig
	crdt      *CRDTStorConnector
	mx        sync.Mutex
	publisher *publisher.Publisher
}

func NewLocalStoreWithCRDTBackend(connector model.Connector) (*API, error) {
	asCRDTConn, ok := connector.(*CRDTStorConnector)
	if !ok {
		return nil, fmt.Errorf("connector is not a CRDT connector")
	}

	store, err := NewCRDTStore(asCRDTConn.cfg, asCRDTConn)
	if err != nil {
		return nil, err
	}

	api := &API{
		Connector: connector,
		Store:     store,
		Broker:    store,
	}

	initAlready, err := api.Store.Get(keyIndexKey)
	if initAlready != nil {
		return api, nil
	}

	// init the key indexes
	api.initialiseKeyIndexes()
	return api, nil
}

func NewCRDTStore(cfg *model.CRDTConfig, conn model.Connector) (*CRDTStore, error) {
	return &CRDTStore{
		cfg:  cfg,
		crdt: conn.(*CRDTStorConnector),
	}, nil
}

func (c *CRDTStore) Get(key string) (*Object, error) {
	ctx := context.Background()
	v, err := c.crdt.Conn.Store.Get(ctx, datastore.NewKey(key))
	if err != nil {
		if strings.Contains(err.Error(), "datastore: key not found") {
			// not found is nil
			return nil, nil
		}
		return nil, err
	}

	// decode object
	var o Object
	err = json.Unmarshal(v, &o)

	return &o, err
}

func (c *CRDTStore) Set(key string, value interface{}) error {
	ctx := context.Background()
	v, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.crdt.Conn.Store.Put(ctx, datastore.NewKey(key), v)
}
func (c *CRDTStore) Delete(key string) error {
	ctx := context.Background()
	err := c.crdt.Conn.Store.Delete(ctx, datastore.NewKey(key))
	if err != nil {
		slog.Error("failed to delete", "error", err)
		return err
	}

	return nil
}

func (c *CRDTStore) FlushAll() error {
	// unsupported
	return nil
}
func (c *CRDTStore) Features() map[ExtendedFeature]bool {
	return map[ExtendedFeature]bool{
		FeatureHardDelete: true,
	}
}
