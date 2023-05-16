package helper

import (
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"
)

func IsSlice(o interface{}) bool {
	return reflect.TypeOf(o).Elem().Kind() == reflect.Slice
}

func ErrPrint(err error) {
	if err != nil {
		log.Println(err.Error())
	}
}

func IsCosmosDB(connectionString string) bool {
	return strings.Contains(connectionString, ".cosmos.") ||
		strings.HasPrefix(connectionString, "https://") && strings.Contains(connectionString, ".documents.azure.com") ||
		strings.HasPrefix(connectionString, "tcp://") && strings.Contains(connectionString, ".documents.azure.com") ||
		strings.HasPrefix(connectionString, "mongodb://") && strings.Contains(connectionString, ".documents.azure.com") ||
		strings.Contains(connectionString, "AccountEndpoint=") ||
		strings.Contains(connectionString, "AccountKey=")
}

// ParsePassword parses the password from the connection string and URL encodes it.
// It's useful when the password contains special characters.
// Example: mongodb://user:p@ssword@localhost:27017/db -> mongodb://user:p%40word@40localhost:27017/db
func ParsePassword(connectionString string) string {
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
