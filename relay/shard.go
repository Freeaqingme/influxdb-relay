package relay

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/models"

	cache "github.com/Freeaqingme/go-cache"
	"github.com/aristanetworks/goarista/monotime"
	"github.com/twmb/murmur3"
)

type shardCollection struct {
	shards []shard

	sumOfProvisionWeights int
	allocationCache       *cache.Cache
	startTime             uint64
}

type shard struct {
	name            string
	provisionWeight int
	backends        []httpBackend
}

type allocationCacheItem struct {
	key        string
	value      *shard
	accessTime uint64
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

	collection.startTime = monotime.Now()
	collection.allocationCache = cache.New(90*time.Minute, 1*time.Minute)
	collection.allocationCache.OnEvicted(collection.reinsertEvictingCacheItem)

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
	res, found := c.allocationCache.GetPossiblyExpired(shardKey)
	if found {
		item := res.(*allocationCacheItem)
		item.accessTime = monotime.Now()
		return item.value
	}

	// TODO: Retrieve from persistent cache (etcd)

	if shard == nil {
		shard = c.GetNewShardForPoint(shardKey, p)
	}

	if shard == nil {
		return nil
	}

	cacheItem := &allocationCacheItem{
		key:        shardKey,
		value:      shard,
		accessTime: monotime.Now(),
	}
	c.allocationCache.Set(shardKey, cacheItem, cache.DefaultExpiration)
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
	return p.Tags()["host"] + "__" + p.Name()
}

func (s *shard) Name() string {
	return s.name
}

// Don't evict keys if they were used in the past 65 minutes,
// or if the application has only been running for <65 minutes
// (since monotime should be considered relative to startup time.
func (c *shardCollection) reinsertEvictingCacheItem(key string, value interface{}) {
	item := value.(*allocationCacheItem)
	if monotime.Since(c.startTime) < 65*time.Minute {
		item.accessTime = monotime.Now()
	} else if monotime.Since(item.accessTime) > 65*time.Minute {
		return // evict
	}

	c.allocationCache.Set(key, item, cache.DefaultExpiration)
}
