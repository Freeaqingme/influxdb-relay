package relay

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"

	"github.com/twmb/murmur3"
)

type shardCollection struct {
	shards []shard

	sumOfProvisionWeights int
	assignmentStore       *assignmentStore
}

type shard struct {
	name            string
	provisionWeight int
	backends        []httpBackend
}

func (s *shard) GetReaders() []httpBackend {
	out := make([]httpBackend, 0)
	for _, b := range s.backends {
		if b.reader {
			out = append(out, b)
		}
	}

	return out
}

func (s *shard) GetRandomReader() *httpBackend {
	readers := s.GetReaders()
	if len(readers) == 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	return &readers[rand.Intn(len(readers))]
}

func NewShardCollection(persistenceName string, globalConf Config) (*shardCollection, error) {
	collection := &shardCollection{}
	persistenceConf := collection.getPersistenceConf(persistenceName, globalConf)
	if persistenceConf == nil {
		return nil, fmt.Errorf("No persistence found by name '%s'", persistenceName)
	}

	for _, shardConfig := range persistenceConf.Shards {
		if err := collection.AddShardFromConfig(&shardConfig); err != nil {
			return nil, err
		}
	}

	for _, shard := range collection.shards {
		collection.sumOfProvisionWeights += shard.provisionWeight
	}

	store, err := newShardAssignmentStore(persistenceConf.ShardAssignmentStores, collection.shards)
	if err != nil {
		return nil, err
	}
	collection.assignmentStore = store
	return collection, nil
}

func (c *shardCollection) getPersistenceConf(persistenceName string, globalConf Config) *PersistenceConfig {
	for _, persistenceConf := range globalConf.Persistence {
		if persistenceConf.Name == persistenceName {
			return &persistenceConf
		}
	}
	return nil
}

func (c *shardCollection) AddShardFromConfig(cfg *ShardConfig) error {
	shard := shard{
		name:            cfg.Name,
		provisionWeight: int(cfg.ProvisionWeight),
	}

	for i := range cfg.Outputs {
		backend, err := newHTTPBackend(&cfg.Outputs[i])
		if err != nil {
			return err
		}

		shard.backends = append(shard.backends, *backend)
	}

	c.shards = append(c.shards, shard)
	return nil
}

func (c *shardCollection) Shards() []shard {
	return c.shards
}

func (c *shardCollection) GetShardForPoint(p models.Point) (shard *shard) {
	shardKey := c.GetShardKey(p)

	return c.GetShardForShardKey(shardKey)
}

func (c *shardCollection) GetShardForShardKey(shardKey string) (shard *shard) {
	if shard = c.GetAssignedShardForShardKey(shardKey); shard != nil {
		return shard
	}

	shard = c.GetNewShardForShardKey(shardKey)
	if shard != nil {
		c.assignmentStore.Set(shardKey, shard)
	}

	return shard
}

func (c *shardCollection) GetAssignedShardForShardKey(shardKey string) (shard *shard) {
	return c.assignmentStore.Get(shardKey)
}

// See also: http://eli.thegreenplace.net/2010/01/22/weighted-random-generation-in-python/
func (c *shardCollection) GetNewShardForShardKey(shardKey string) *shard {
	if c.sumOfProvisionWeights == 0 {
		return nil
	}

	hash := murmur3.Sum32([]byte(shardKey))

	// Drop 1 bit so we can cast to int without overflowing
	r := int(hash>>1) % c.sumOfProvisionWeights
	for _, s := range c.shards {
		r -= s.provisionWeight
		if r < 0 {
			return &s
		}
	}
	return nil
}

// TODO: Ideally we do this more dynamically in LUA
func (c *shardCollection) GetShardKey(p models.Point) string {
	suffix := p.Name()

	// We want rx and tx to always end up in the same shard so they
	// can always be queried together. Yet another good reason to
	// implement LUA, for application specific stuff...
	suffix = strings.Replace(suffix, "libvirt_tx", "libvirt_rxtx", 1)
	suffix = strings.Replace(suffix, "libvirt_rx", "libvirt_rxtx", 1)

	// Same for _read & _write
	suffix = strings.Replace(suffix, "libvirt_read", "libvirt_readwrite", 1)
	suffix = strings.Replace(suffix, "libvirt_write", "libvirt_readwrite", 1)

	if vhost, exists := p.Tags()["vhost"]; exists {
		return vhost + "__" + suffix
	}
	return p.Tags()["host"] + "__" + suffix
}

func (s *shard) Name() string {
	return s.name
}
