package types

// BaseStorageOptions contains options that are common across all storage types.
type BaseStorageOptions struct {
	Username string `json:"username"`
	Password string `json:"password"`
	UseSSL   bool   `json:"use_ssl"`
}

// RedisOptions contains options specific to Redis storage.
type RedisOptions struct {
	BaseStorageOptions

	Host                  string            `json:"host"`
	Port                  int               `json:"port"`
	Addrs                 []string          `json:"addrs"`
	Database              int               `json:"database"`
	MasterName            string            `json:"master_name"`
	SentinelPassword      string            `json:"sentinel_password"`
	MaxIdle               int               `json:"optimisation_max_idle"`
	MaxActive             int               `json:"optimisation_max_active"`
	Timeout               int               `json:"timeout"`
	EnableCluster         bool              `json:"enable_cluster"`
	SSLInsecureSkipVerify bool              `json:"ssl_insecure_skip_verify"`
	Hosts                 map[string]string `json:"hosts"` // Deprecated. Addrs instead.
}

// ClientOpts serves as a union of all possible configurations.
// It can be extended to include other storage options in the future.
type ClientOpts struct {
	Type  string        `json:"type"`
	Redis *RedisOptions `json:"redis,omitempty"`
	// any other storage backends...
}
