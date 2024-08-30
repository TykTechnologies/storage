package model

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TykTechnologies/cannery/v2/connector"
	"github.com/libp2p/go-libp2p/core/pnet"
	"golang.org/x/exp/rand"
)

type CRDTConfig struct {
	ListenAddrs          []string
	BootstrapAddr        string
	PrimaryKey           []byte
	SharedKey            []byte
	DBName               string
	TagName              string
	MockDisconnect       bool
	ConnectOnInstantiate bool
}

func NewCRDTConfig(opts ...CRDTOption) *CRDTConfig {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func defaultConfig() *CRDTConfig {
	return &CRDTConfig{
		ListenAddrs:   []string{"/ip4/0.0.0.0/tcp/7654"},
		BootstrapAddr: "",
		PrimaryKey:    nil,
		DBName:        "crdt-db",
		TagName:       "crdt-service",
		SharedKey:     nil,
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

type CRDTOption func(*CRDTConfig)

const defaultKeyFileName = "id.key"

func WithConnectOnInstantiate() CRDTOption {
	return func(c *CRDTConfig) {
		c.ConnectOnInstantiate = true
	}
}

func WithSharedKey(filename string) CRDTOption {
	return func(c *CRDTConfig) {
		d, err := os.ReadFile(filename)
		if err != nil {
			log.Fatal("could not read PSK file from", filename, err)
		}

		s := ""
		s += fmt.Sprintln("/key/swarm/psk/1.0.0/")
		s += fmt.Sprintln("/base64/")
		s += fmt.Sprintf("%s", string(d))
		psk, err := pnet.DecodeV1PSK(bytes.NewBuffer([]byte(s)))
		if err != nil {
			log.Fatal(err)
		}

		c.SharedKey = psk
	}
}

func WithRandomKey() CRDTOption {
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

func WithMockDisconnect() CRDTOption {
	return func(c *CRDTConfig) {
		c.MockDisconnect = true
	}
}

func WithKeyFromFile(keyFile string) CRDTOption {
	return func(c *CRDTConfig) {
		dat, err := os.ReadFile(keyFile)
		if err != nil {
			fmt.Errorf("error reading key file: %v", err)
			return
		}

		c.PrimaryKey = dat
	}
}

func WithListenAddr(addr string) CRDTOption {
	return func(c *CRDTConfig) {
		c.ListenAddrs = append(c.ListenAddrs, addr)
	}
}

func WithListenAddrs(addrs []string) CRDTOption {
	return func(c *CRDTConfig) {
		c.ListenAddrs = addrs
	}
}

func WithBootstrapAddr(addr string) CRDTOption {
	return func(c *CRDTConfig) {
		c.BootstrapAddr = addr
	}
}

func WithPrimaryKey(key []byte) CRDTOption {
	return func(c *CRDTConfig) {
		c.PrimaryKey = key
	}
}

func WithDBName(name string) CRDTOption {
	return func(c *CRDTConfig) {
		c.DBName = name
	}
}

func WithTagName(name string) CRDTOption {
	return func(c *CRDTConfig) {
		c.TagName = name
	}
}
