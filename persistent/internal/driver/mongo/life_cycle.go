package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/TykTechnologies/storage/persistent/internal/model"
)

type lifeCycle struct {
	client *mongo.Client
}

// Connect connects to the mongo database given the ClientOpts.
func (lc *lifeCycle) Connect(opts *model.ClientOpts) error {
	var err error
	var client *mongo.Client

	connOpts, err := mongoOptsBuilder(opts)
	if err != nil {
		return err
	}

	if client, err = mongo.Connect(context.Background(), connOpts); err != nil {
		return err
	}

	lc.client = client

	return lc.client.Ping(context.Background(), nil)
}

// mongoOptsBuilder build Mongo options.ClientOptions from our own model.ClientOpts. Also sets default values.
// mongo URI parameters specified in the model.ClientOpts ConnectionString have precedence over the ones configured in
// other fields.
func mongoOptsBuilder(opts *model.ClientOpts) (*options.ClientOptions, error) {
	connOpts := options.Client()

	if opts.UseSSL {
		tlsConfig, err := opts.GetTLSConfig()
		if err != nil {
			return nil, err
		}

		connOpts.SetTLSConfig(tlsConfig)
	}

	connOpts.SetTimeout(model.DEFAULT_CONN_TIMEOUT)

	if opts.ConnectionTimeout != 0 {
		connOpts.SetTimeout(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	connOpts.SetReadPreference(getReadPrefFromConsistency(opts.SessionConsistency))

	// we apply URI here so if we specify a different configuration in the URI it can be overridden
	connOpts.ApplyURI(opts.ConnectionString)

	err := connOpts.Validate()
	if err != nil {
		return nil, err
	}

	return connOpts, nil
}

// getReadPrefFromConsistency returns the equivalent of the readPreference for session consistency
func getReadPrefFromConsistency(consistency string) *readpref.ReadPref {
	var mode *readpref.ReadPref

	switch consistency {
	case "eventual":
		mode = readpref.Nearest()
	case "monotonic":
		mode = readpref.PrimaryPreferred()
	default:
		mode = readpref.Primary()
	}

	return mode
}
