package model

type ClientOpts struct {
	// ConnectionString is the expression used to connect to a storage db server.
	// It contains parameters such as username, hostname, password and port
	ConnectionString string
	// UseSSL is SSL connection is required to connect
	UseSSL bool
	// This setting allows the use of self-signed certificates when connecting to an encrypted storage database.
	SSLInsecureSkipVerify bool
	// Ignore hostname check when it differs from the original (for example with SSH tunneling).
	// The rest of the TLS verification will still be performed
	SSLAllowInvalidHostnames bool
	// Path to the PEM file with trusted root certificates
	SSLCAFile string
	// Path to the PEM file which contains both client certificate and private key. This is required for Mutual TLS.
	SSLPEMKeyfile string
	// Sets the session consistency for the storage connection
	SessionConsistency string
}
