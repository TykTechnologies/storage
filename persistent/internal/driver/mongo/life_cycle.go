package mongo

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/utils"

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

	opts.ConnectionString = parsePassword(opts.ConnectionString)

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

	lc.connectionString = opts.ConnectionString
	lc.database = cs.Database
	lc.client = client

	return lc.client.Ping(context.Background(), nil)
}

// parsePassword parses the password from the connection string and URL encodes it. Useful when the password contains special characters.
// Example: mongodb://user:p@ssword@localhost:27017/db -> mongodb://user:p%40word@40localhost:27017/db
func parsePassword(connectionString string) string {
	// Find the last '@' (the delimiter between credentials and host)
	at := strings.LastIndex(connectionString, "@")
	if at == -1 {
		return connectionString
	}
	credentialsAndScheme := connectionString[:at]
	hostAndDB := connectionString[at+1:]

	// Split the credentials and scheme
	credentialsAndSchemeParts := strings.SplitN(credentialsAndScheme, "://", 2)
	if len(credentialsAndSchemeParts) != 2 {
		return connectionString
	}
	credentials := credentialsAndSchemeParts[1]

	// Split the username and password
	credentialsParts := strings.SplitN(credentials, ":", 2)
	if len(credentialsParts) != 2 {
		return connectionString
	}
	username := credentialsParts[0]
	password := credentialsParts[1]

	// URL encode the password
	encodedPassword := url.QueryEscape(password)

	// Construct the new connection string
	newConnectionString := fmt.Sprintf("mongodb://%s:%s@%s", username, encodedPassword, hostAndDB)

	return newConnectionString
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
