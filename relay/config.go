package relay

import (
	"fmt"
	"net/url"
	"os"

	"github.com/naoina/toml"
)

type Config struct {
	HTTPRelays          []HTTPConfig          `toml:"http"`
	UDPRelays           []UDPConfig           `toml:"udp"`
	Collectd2HTTPRelays []Collectd2HTTPConfig `toml:"collectd2http"`
	Persistence         []PersistenceConfig   `toml:"persistence"`
}

type HTTPConfig struct {
	// Name identifies the HTTP relay
	Name string `toml:"name"`

	// Addr should be set to the desired listening host:port
	Addr string `toml:"bind-addr"`

	// Set certificate in order to handle HTTPS requests
	SSLCombinedPem string `toml:"ssl-combined-pem"`

	// Default retention policy to set for forwarded requests
	DefaultRetentionPolicy string `toml:"default-retention-policy"`

	// Outputs is a list of backed servers where writes will be forwarded
	PersistenceName string `toml:"persistence"`
}

type HTTPOutputConfig struct {
	// Name of the backend server
	Name string `toml:"name"`

	// Location should be set to the URL of the backend server's write endpoint
	Location string `toml:"location"`

	// Timeout sets a per-backend timeout for write requests. (Default 10s)
	// The format used is the same seen in time.ParseDuration
	Timeout string `toml:"timeout"`

	// Buffer failed writes up to maximum count. (Default 0, retry/buffering disabled)
	BufferSizeMB int `toml:"buffer-size-mb"`

	// Maximum batch size in KB (Default 512)
	MaxBatchKB int `toml:"max-batch-kb"`

	// Maximum delay between retry attempts.
	// The format used is the same seen in time.ParseDuration (Default 10s)
	MaxDelayInterval string `toml:"max-delay-interval"`

	// Skip TLS verification in order to use self signed certificate.
	// WARNING: It's insecure. Use it only for developing and don't use in production.
	SkipTLSVerification bool `toml:"skip-tls-verification"`

	Mode string `toml:"mode"`
}

type UDPConfig struct {
	// Name identifies the UDP relay
	Name string `toml:"name"`

	// Addr is where the UDP relay will listen for packets
	Addr string `toml:"bind-addr"`

	// Precision sets the precision of the timestamps (input and output)
	Precision string `toml:"precision"`

	// ReadBuffer sets the socket buffer for incoming connections
	ReadBuffer int `toml:"read-buffer"`

	// Outputs is a list of backend servers where writes will be forwarded
	Outputs []UDPOutputConfig `toml:"output"`
}

type UDPOutputConfig struct {
	// Name identifies the UDP backend
	Name string `toml:"name"`

	// Location should be set to the host:port of the backend server
	Location string `toml:"location"`

	// MTU sets the maximum output payload size, default is 1024
	MTU int `toml:"mtu"`
}

type Collectd2HTTPConfig struct {
	// Name identifies the Collectd2HTTP relay
	Name string `toml:"name"`

	// Addr is where the UDP relay will listen for packets
	Addr string `toml:"bind-addr"`

	TypesDBPath string `toml:"typesdb"`

	// ReadBuffer sets the socket buffer for incoming connections
	ReadBuffer int `toml:"read-buffer"`

	PersistenceName string `toml:"persistence"`
}

type PersistenceConfig struct {
	Name                  string               `toml:"name"`
	Shards                []ShardConfig        `toml:"shards"`
	ShardAssignmentStores ShardAssignmentStore `toml:"assignment-store"`
}

type ShardConfig struct {
	// Name identifies the UDP backend
	Name string `toml:"name"`

	ProvisionWeight uint32 `toml:"provision-weight"`

	Outputs []HTTPOutputConfig `toml:"output"`
}

type ShardAssignmentStore struct {
	Type string `toml:"type"`

	Endpoints []string `toml:"endpoints"`

	EtcdPrefix string `toml:"etcd-prefix"`
}

// LoadConfigFile parses the specified file into a Config object
func LoadConfigFile(filename string) (cfg Config, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	err = toml.NewDecoder(f).Decode(&cfg)
	if err != nil {
		return
	}
	// Validate HTTP endpoints
	for _, p := range cfg.Persistence {
		if len(p.Shards) == 0 {
			return cfg, fmt.Errorf("No shards defined for shard collection")
		}

		for _, s := range p.Shards {
			for _, o := range s.Outputs {
				if _, err = url.ParseRequestURI(o.Location); err != nil {
					return
				}
			}
		}
	}

	return
}
