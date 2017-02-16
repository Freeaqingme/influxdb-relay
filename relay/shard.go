package relay

import (
	"fmt"

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

func NewShardCollection(shardConfigs []Collectd2HTTPShardConfig, etcdCfg ShardAssignmentStore) (*shardCollection, error) {
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

	store, err := newShardAssignmentStore(etcdCfg, collection.shards)
	if err != nil {
		return nil, err
	}
	collection.assignmentStore = store
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

func (c *shardCollection) GetShardForPoint(p models.Point) (shard *shard) {
	shardKey := c.GetShardKey(p)
	if shard = c.assignmentStore.Get(shardKey); shard != nil {
		return shard
	}

	shard = c.GetNewShardForPoint(shardKey, p)
	if shard != nil {
		c.assignmentStore.Set(shardKey, shard)
	}

	return shard
}

// See also: http://eli.thegreenplace.net/2010/01/22/weighted-random-generation-in-python/
func (c *shardCollection) GetNewShardForPoint(shardKey string, p models.Point) *shard {
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
	if vhost, exists := p.Tags()["vhost"]; exists {
		return vhost + "__" + p.Name()
	}
	return p.Tags()["host"] + "__" + p.Name()
}

func (s *shard) Name() string {
	return s.name
}
