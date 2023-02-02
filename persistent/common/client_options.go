package common

type ClientOpts struct {
	// ConnectionString is the expression used to connect to a mongo db server. It contain parameters such as username, hostname, password and port
	ConnectionString string
	// should we add here any other option that we will support for connection to mongo? Eg: timeouts, certificates, mutual TLS config, etc
	UseSSL bool
	// This setting allows the use of self-signed certificates when connecting to an encrypted MongoDB database.
	SSLInsecureSkipVerify bool
	// Ignore hostname check when it differs from the original (for example with SSH tunneling). The rest of the TLS verification will still be performed
	SSLAllowInvalidHostnames bool
	// Path to the PEM file with trusted root certificates
	SSLCAFile string
	// Path to the PEM file which contains both client certificate and private key. This is required for Mutual TLS.
	SSLPEMKeyfile string
	// Sets the session consistency for the mongo connection
	SessionConsistency string
}
