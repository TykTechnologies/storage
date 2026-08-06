package consul

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/TykTechnologies/storage/kv"
	consulsdk "github.com/hashicorp/consul/api"
)

// Config is used to configure a HashiCorp Consul store.
//
// Every field is optional: a store with no settings at all connects to a Consul
// agent on the same host. Note that most of the fields, when left empty, fall
// back to a standard CONSUL_* environment variable of the Tyk process before
// falling back to the default — each field below says which variable applies to
// it. This is Consul's own client behaviour, which Tyk does not override, and it
// means that in a Tyk process holding several Consul stores, every store that
// omits a field inherits the same environment value. Set the fields explicitly
// per store wherever they need to differ.
type Config struct {
	// Address is the Consul HTTP API endpoint Tyk reads secrets from, given as
	// host and port with no scheme, for example "consul.example.com:8500". Point
	// it at whatever your deployment exposes — a Consul server, a load balancer or
	// service address in front of the cluster, a managed endpoint such as HCP
	// Consul, or a local agent on hosts that run one. Consul's own default ports
	// are 8500 for plain HTTP and 8501 for HTTPS, but a cluster may use others.
	//
	// Left empty, the address comes from the CONSUL_HTTP_ADDR environment
	// variable, falling back to "127.0.0.1:8500". That last fallback is the Consul
	// client's own default and assumes an agent on the same host as Tyk, which is
	// unlikely to be what you want outside local development — set this field
	// explicitly for anything else. Optional.
	Address string `json:"address"`

	// Scheme is the protocol Tyk uses to reach the address above: "http" or
	// "https". Use "https" whenever the connection leaves the host, and see
	// TLSConfig for the certificates that go with it. Left empty, it is "https"
	// if the CONSUL_HTTP_SSL environment variable is set to a true value, and
	// "http" otherwise. Optional.
	Scheme string `json:"scheme"`

	// Datacenter is the name of the Consul datacenter to read from, for example
	// "eu-west-1". Consul groups its servers into datacenters, and the key/value
	// data of each is separate. Leave it empty to use the datacenter the
	// configured agent itself belongs to, which is what you want unless you are
	// deliberately reading across datacenters. Optional.
	Datacenter string `json:"datacenter"`

	// HttpAuth holds a username and password for HTTP Basic authentication, sent
	// with every request. This is not Consul's own access control — see Token for
	// that — but for setups where the Consul API sits behind a reverse proxy or
	// load balancer that demands Basic auth. Leave both empty otherwise, in which
	// case the CONSUL_HTTP_AUTH environment variable applies if it is set.
	// Optional.
	HttpAuth struct {
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"http_auth"`

	// WaitTime is how long Consul may hold a blocking query open before
	// answering. Blocking queries are a Consul mechanism for waiting on a change
	// rather than polling for it.
	//
	// Tyk's secret reads are ordinary reads and never block, so this setting has
	// no effect on them and is not the time limit for a read. That limit is not
	// configurable for Consul stores and is fixed at 5 seconds per request.
	//
	// Give it as a Go duration string ("5s") if you have a reason to set it;
	// empty or "0s" keeps Consul's own default. A value Tyk cannot read as a
	// duration stops the store from starting. Optional.
	WaitTime string `json:"wait_time"`

	// Token is the Consul ACL token Tyk authenticates with. Consul's access
	// control system uses tokens to decide which keys a caller may read, so this
	// token's policies must grant read access to the keys Tyk resolves. Setting
	// it overrides any default token configured on the agent. Left empty, the
	// token comes from the CONSUL_HTTP_TOKEN or CONSUL_HTTP_TOKEN_FILE
	// environment variable if either is set; with none of them, requests carry no
	// token, which works only where the cluster has ACLs disabled or a permissive
	// default token. Optional.
	Token string `json:"token"`

	// TLSConfig holds the certificate settings used when Scheme is "https". It is
	// ignored for plain "http". All fields are optional; a Consul cluster using
	// certificates from a public authority needs none of them. As with the fields
	// above, each one left empty falls back to its standard Consul environment
	// variable — CONSUL_TLS_SERVER_NAME, CONSUL_CACERT, CONSUL_CAPATH,
	// CONSUL_CLIENT_CERT, CONSUL_CLIENT_KEY, and CONSUL_HTTP_SSL_VERIFY — if the
	// Tyk process has it set.
	TLSConfig struct {
		// Address is the hostname to expect in the Consul server's certificate,
		// when that differs from the host in Address — for example when
		// connecting through an IP address or a load balancer while the
		// certificate names the cluster. Any port given here is discarded.
		Address string `json:"address"`

		// CAFile is a path to a PEM file holding the certificate authority that
		// signed the Consul server's certificate. Set it when Consul uses a
		// private or internal authority, so Tyk can verify the server. Defaults
		// to the host's system trust store when omitted.
		CAFile string `json:"ca_file"`

		// CAPath is a path to a directory of PEM certificate authority files, as
		// an alternative to naming a single CAFile.
		CAPath string `json:"ca_path"`

		// CertFile and KeyFile are paths to Tyk's own client certificate and its
		// private key, in PEM form. Set both when the Consul cluster requires
		// clients to identify themselves with a certificate (mutual TLS). Leave
		// both empty otherwise.
		CertFile string `json:"cert_file"`
		KeyFile  string `json:"key_file"`

		// InsecureSkipVerify turns off verification of the Consul server's
		// certificate. It makes the connection vulnerable to interception, so
		// only use it against a local test cluster with a self-signed
		// certificate — never in production. Defaults to false.
		InsecureSkipVerify bool `json:"insecure_skip_verify"`
	} `json:"tls_config"`
}

// NewFactory returns a kv.ProviderFactory for HashiCorp Consul stores.
//
// The factory parses Config and builds a consul API client, but performs no
// network I/O. It errors only for config that is present but unusable: malformed
// JSON, an unparseable wait_time, or TLS settings the client rejects (e.g. an
// unreadable ca_file). Absent config is valid — an empty blob builds a client
// against consul's default local agent.
func NewFactory() kv.ProviderFactory {
	return func(raw json.RawMessage) (kv.Provider, error) {
		var conf Config
		if err := parseConfig(raw, &conf); err != nil {
			return nil, err
		}

		clientCfg, err := conf.clientConfig()
		if err != nil {
			return nil, err
		}

		client, err := consulsdk.NewClient(clientCfg)
		if err != nil {
			return nil, fmt.Errorf("consul: create client: %w", err)
		}

		return &consulProvider{kvClient: client.KV()}, nil
	}
}

func parseConfig(raw json.RawMessage, conf *Config) error {
	if len(raw) == 0 {
		return nil
	}

	if err := json.Unmarshal(raw, conf); err != nil {
		return fmt.Errorf("consul: invalid config: %w", err)
	}

	return nil
}

func (conf *Config) clientConfig() (*consulsdk.Config, error) {
	clientCfg := consulsdk.DefaultConfig()

	if conf.Address != "" {
		clientCfg.Address = conf.Address
	}

	if conf.Scheme != "" {
		clientCfg.Scheme = conf.Scheme
	}

	if conf.Datacenter != "" {
		clientCfg.Datacenter = conf.Datacenter
	}

	if conf.Token != "" {
		clientCfg.Token = conf.Token
	}

	if conf.HttpAuth.Username != "" || conf.HttpAuth.Password != "" {
		clientCfg.HttpAuth = &consulsdk.HttpBasicAuth{
			Username: conf.HttpAuth.Username,
			Password: conf.HttpAuth.Password,
		}
	}

	if err := conf.applyWaitTime(clientCfg); err != nil {
		return nil, err
	}

	conf.applyTLSConfig(clientCfg)

	return clientCfg, nil
}

func (conf *Config) applyWaitTime(clientCfg *consulsdk.Config) error {
	if conf.WaitTime == "" {
		return nil
	}

	waitTime, err := time.ParseDuration(conf.WaitTime)
	if err != nil {
		return fmt.Errorf("consul: invalid wait_time %q: %w", conf.WaitTime, err)
	}

	if waitTime > 0 {
		clientCfg.WaitTime = waitTime
	}

	return nil
}

func (conf *Config) applyTLSConfig(clientCfg *consulsdk.Config) {
	tls := conf.TLSConfig

	if tls.Address != "" {
		clientCfg.TLSConfig.Address = tls.Address
	}

	if tls.CAFile != "" {
		clientCfg.TLSConfig.CAFile = tls.CAFile
	}

	if tls.CAPath != "" {
		clientCfg.TLSConfig.CAPath = tls.CAPath
	}

	if tls.CertFile != "" {
		clientCfg.TLSConfig.CertFile = tls.CertFile
	}

	if tls.KeyFile != "" {
		clientCfg.TLSConfig.KeyFile = tls.KeyFile
	}

	if tls.InsecureSkipVerify {
		clientCfg.TLSConfig.InsecureSkipVerify = true
	}
}
