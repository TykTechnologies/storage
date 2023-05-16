package mongo

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/utils"
	"gopkg.in/mgo.v2"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

	"github.com/TykTechnologies/storage/persistent/internal/types"
)

type lifeCycle struct {
	client *mongo.Client

	connectionString string
	database         string
}

var _ types.StorageLifecycle = &lifeCycle{}

// Connect connects to the mongo database given the ClientOpts.
func (lc *lifeCycle) Connect(opts *types.ClientOpts) error {
	var err error
	var client *mongo.Client

	dialInfo, err := mgo.ParseURL(opts.ConnectionString)
	if err != nil {
		return err
	}

	// we check if the connection string is valid before building the connOpts.
	cs, err := connstring.ParseAndValidate(opts.ConnectionString)
	if err != nil {
		return errors.New("invalid connection string")
	}

	connOpts, err := mongoOptsBuilder(opts)
	if err != nil {
		return errors.New(err.Error())
	}

	// SetRegistry allow us to marshall/unmarshall old mgo ID's structures and mgo default values.
	connOpts.SetRegistry(createCustomRegistry().Build())

	if client, err = mongo.Connect(context.Background(), connOpts); err != nil {
		return err
	}
	connectionString := opts.ConnectionString
	if cs.PasswordSet {
		u, err := url.Parse(connectionString)
		if err != nil {
			return err
		}

		username := u.User.Username()
		password, _ := u.User.Password()

		connectionString = fmt.Sprintf("mongodb://%s:%s@%s", username, password, u.Host)
	}

	lc.connectionString = opts.ConnectionString
	lc.database = cs.Database
	lc.client = client

	return lc.client.Ping(context.Background(), nil)
}

// Close finish the session.
func (lc *lifeCycle) Close() error {
	if lc.client != nil {
		return lc.client.Disconnect(context.Background())
	}

	return errors.New("closing a no connected database")
}

// DBType returns the type of the registered storage driver.
func (lc *lifeCycle) DBType() utils.DBType {
	if helper.IsCosmosDB(lc.connectionString) {
		return utils.CosmosDB
	}

	var result struct {
		Code int `bson:"code"`
	}

	cmd := bson.D{{Key: "features", Value: 1}}
	singleResult := lc.client.Database("admin").RunCommand(context.Background(), cmd)

	if err := singleResult.Decode(&result); (singleResult.Err() != nil || err != nil) && result.Code == 303 {
		return utils.AWSDocumentDB
	}

	return utils.StandardMongo
}

// mongoOptsBuilder build Mongo options.ClientOptions from our own types.ClientOpts. Also sets default values.
// mongo URI parameters specified in the types.ClientOpts ConnectionString have precedence over the ones configured in
// other input.
func mongoOptsBuilder(opts *types.ClientOpts) (*options.ClientOptions, error) {
	connOpts := options.Client()

	if opts.UseSSL {
		tlsConfig, err := opts.GetTLSConfig()
		if err != nil {
			return nil, err
		}

		connOpts.SetTLSConfig(tlsConfig)
	}

	connOpts.SetTimeout(types.DEFAULT_CONN_TIMEOUT)

	if opts.ConnectionTimeout != 0 {
		connOpts.SetTimeout(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	connOpts.SetReadPreference(getReadPrefFromConsistency(opts.SessionConsistency))

	// we apply URI here so if we specify a different configuration in the URI it can be overridden
	connOpts.ApplyURI(opts.ConnectionString)

	connOpts.SetDirect(opts.DirectConnection)

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
