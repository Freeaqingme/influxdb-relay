package relay

import (
	"fmt"

	"github.com/influxdata/influxdb/models"

	"github.com/twmb/murmur3"
)

type shardCollection struct {
	shards []shard

	sumOfProvisionWeights int
}

type shard struct {
	name            string
	provisionWeight int
	backends        []httpBackend
}

func NewShardCollection(shardConfigs []Collectd2HTTPShardConfig) (*shardCollection, error) {
	collection := &shardCollection{}
	if len(shardConfigs) == 0 {
		return nil, fmt.Errorf("No shards defined for shard collection")
	}

	for _, shardConfig := range shardConfigs {
		if err := collection.AddShardFromConfig(&shardConfig); err != nil {
			return nil, err
		}
	}

	for _, shard := range collection.shards {
		collection.sumOfProvisionWeights += shard.provisionWeight
	}

	return collection, nil
}

func (c *shardCollection) AddShardFromConfig(cfg *Collectd2HTTPShardConfig) error {
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

// See also: http://eli.thegreenplace.net/2010/01/22/weighted-random-generation-in-python/
func (c *shardCollection) GetShardForPoint(p models.Point) *shard {
	// TODO: Retrieve from LRU cache

	// TODO: Retrieve from persistent cache (etcd)

	return c.GetNewShardForPoint(p)
}

func (c *shardCollection) GetNewShardForPoint(p models.Point) *shard {
	if c.sumOfProvisionWeights == 0 {
		return nil
	}

	hash := murmur3.Sum32(c.GetShardKey(p))

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
func (c *shardCollection) GetShardKey(p models.Point) []byte {
	return []byte(p.Tags()["host"] + "__" + p.Name())
}

func (s *shard) Name() string {
	return s.name
}
