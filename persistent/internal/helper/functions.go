package helper

import (
	"errors"
	"log"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
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

// shouldReconnect checks if the error is a network error or a server error that requires reconnection
func ShouldReconnect(err error) bool {
	if mongo.IsNetworkError(err) {
		return true
	}

	var serverErr mongo.ServerError
	if !errors.As(err, &serverErr) {
		return false
	}

	// Only reconnect on specific error codes that indicate connection issues
	// MongoDB error codes reference: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
	reconnectErrorCodes := []int{
		6,     // HostUnreachable
		7,     // HostNotFound
		89,    // NetworkTimeout
		91,    // ShutdownInProgress
		133,   // FailedToSatisfyReadPreference
		189,   // PrimarySteppedDown
		262,   // ExceededTimeLimit
		9001,  // SocketException
		11600, // InterruptedAtShutdown
		11602, // InterruptedDueToReplStateChange
		13435, // NotPrimaryNoSecondaryOk
		13436, // NotPrimaryOrSecondary
	}

	for _, code := range reconnectErrorCodes {
		if serverErr.HasErrorCode(code) {
			return true
		}
	}

	return false
}
