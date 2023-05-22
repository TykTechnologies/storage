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
// If there's any conflict, the function returns the original connection string.
func ParsePassword(connectionString string) string {
	// Find the last '@' before the last ':' (the delimiter between credentials and host)
	// we use ':' since the URL can contain '@' characters after the port number
	at := findLastAtBeforeLastColon(connectionString)
	if at == -1 {
		// If there's no ':' in the connection string, we use the last '@' as delimiter
		at = findLastAt(connectionString)
		// If there's no '@' in the connection string, we return the original connection string
		if at == -1 {
			return connectionString
		}
	}

	credentialsAndScheme := connectionString[:at]
	hostAndDB := connectionString[at+1:]

	// Split the credentials and scheme
	credentialsAndSchemeParts := strings.SplitN(credentialsAndScheme, "://", 2)
	if len(credentialsAndSchemeParts) != 2 {
		return connectionString
	}

	scheme := credentialsAndSchemeParts[0] // here we extract the scheme
	credentials := credentialsAndSchemeParts[1]

	// Split the username and password
	credentialsParts := strings.Split(credentials, ":")
	if len(credentialsParts) < 2 {
		return connectionString
	}

	username := credentialsParts[0]
	password := strings.Join(credentialsParts[1:], ":")

	// URL encode the password
	encodedPassword := url.QueryEscape(password)

	encodedUsername := url.QueryEscape(username)

	// Construct the new connection string
	newConnectionString := fmt.Sprintf("%s://%s:%s@%s", scheme, encodedUsername, encodedPassword, hostAndDB)

	return newConnectionString
}

func findLastAtBeforeLastColon(str string) int {
	lastColon := strings.LastIndex(str, ":")
	if lastColon == -1 {
		return -1
	}

	subStr := str[:lastColon]

	lastAt := strings.LastIndex(subStr, "@")

	return lastAt
}

func findLastAt(str string) int {
	return strings.LastIndex(str, "@")
}
