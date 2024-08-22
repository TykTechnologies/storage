package local

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/TykTechnologies/cannery/v2/connector"
	"golang.org/x/exp/rand"
)

type CRDTConfig struct {
	ListenAddr     string
	BootstrapAddr  string
	PrimaryKey     []byte
	DBName         string
	TagName        string
	MockDisconnect bool
}

func NewCRDTConnector(cfg *CRDTConfig) *CRDTStorConnector {
	return &CRDTStorConnector{
		cfg: cfg,
		mx:  sync.Mutex{},
	}
}

func NewCRDTConfig(opts ...Option) *CRDTConfig {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func defaultConfig() *CRDTConfig {
	return &CRDTConfig{
		ListenAddr:    "/ip4/0.0.0.0/tcp/7654",
		BootstrapAddr: "",
		PrimaryKey:    nil,
		DBName:        "crdt-db",
		TagName:       "crdt-service",
	}
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateRandomString(length int) string {
	rand.Seed(uint64(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func NewCRDTConfigForTests() *CRDTConfig {
	// random string
	return NewCRDTConfig(
		WithRandomKey(),
		WithDBName(generateRandomString(5)),
		WithMockDisconnect(),
	)
}

type Option func(*CRDTConfig)

func WithRandomKey() Option {
	return func(c *CRDTConfig) {
		err := connector.GenerateIdentityKey(defaultKeyFileName)
		if err != nil {
			fmt.Errorf("error generating key: %v", err)
			return
		}

		dat, err := os.ReadFile(defaultKeyFileName)
		if err != nil {
			fmt.Errorf("error reading key file: %v", err)
			return
		}

		c.PrimaryKey = dat
	}
}

func WithMockDisconnect() Option {
	return func(c *CRDTConfig) {
		c.MockDisconnect = true
	}
}

func WithKeyFromFile(keyFile string) Option {
	return func(c *CRDTConfig) {
		dat, err := os.ReadFile(keyFile)
		if err != nil {
			fmt.Errorf("error reading key file: %v", err)
			return
		}

		c.PrimaryKey = dat
	}
}

func WithListenAddr(addr string) Option {
	return func(c *CRDTConfig) {
		c.ListenAddr = addr
	}
}

func WithBootstrapAddr(addr string) Option {
	return func(c *CRDTConfig) {
		c.BootstrapAddr = addr
	}
}

func WithPrimaryKey(key []byte) Option {
	return func(c *CRDTConfig) {
		c.PrimaryKey = key
	}
}

func WithDBName(name string) Option {
	return func(c *CRDTConfig) {
		c.DBName = name
	}
}

func WithTagName(name string) Option {
	return func(c *CRDTConfig) {
		c.TagName = name
	}
}
