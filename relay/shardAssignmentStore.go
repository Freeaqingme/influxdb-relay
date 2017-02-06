package relay

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	goCache "github.com/Freeaqingme/go-cache"
	"github.com/aristanetworks/goarista/monotime"
	etcdClient "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	EtcdTTL = 2 * 365 * 24 * time.Hour // ~2 years
)

type assignmentStore struct {
	startTime uint64
	memory    *goCache.Cache

	etcdPrefix string
	etcdClient etcdClient.Client
	etcdKapi   etcdClient.KeysAPI

	shards []shard
}

type allocationCacheItem struct {
	key        string
	value      *shard
	accessTime uint64
	createTime uint64
}

func newShardAssignmentStore(etcdCfg ShardAssignmentStore, shards []shard) (*assignmentStore, error) {
	s := &assignmentStore{}
	s.startTime = monotime.Now()
	s.shards = shards

	s.memory = goCache.New(5*time.Minute, 1*time.Minute)
	s.memory.OnEvicted(s.reinsertEvictingCacheItem)

	if etcdCfg.Type != "etcd" {
		return nil, fmt.Errorf("Invalid Shard Assignment Store type specified. Must be one of: etcd")
	}

	cfg := etcdClient.Config{
		Endpoints:               etcdCfg.Endpoints,
		Transport:               etcdClient.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := etcdClient.New(cfg)
	if err != nil {
		return nil, err
	}
	s.etcdClient = c
	s.etcdKapi = etcdClient.NewKeysAPI(c)
	s.etcdPrefix = etcdCfg.EtcdPrefix

	return s, nil
}

func (c *assignmentStore) Get(shardKey string) *shard {
	res, found := c.memory.GetPossiblyExpired(shardKey)
	if found {
		item := res.(*allocationCacheItem)
		item.accessTime = monotime.Now()
		return item.value
	}

	shard := c.getShardForPointFromEtcd(shardKey)
	if shard != nil {
		c.storeInMemory(shardKey, shard)
	}
	return shard
}

func (c *assignmentStore) Set(shardKey string, shard *shard) {
	if err := c.cacheAllocationInEtcd(shardKey, shard.Name()); err != nil {
		// We don't want to persist any possibly non-persisted things in memory
		return
	}

	c.storeInMemory(shardKey, shard)
}

func (c *assignmentStore) storeInMemory(shardKey string, shard *shard) {
	cacheItem := &allocationCacheItem{
		key:        shardKey,
		value:      shard,
		accessTime: monotime.Now(),
		createTime: monotime.Now(),
	}
	c.memory.Set(shardKey, cacheItem, c.getRandExpirationTime())
}

func (c *assignmentStore) cacheAllocationInEtcd(shardKey, shardName string) error {
	opt := &etcdClient.SetOptions{
		TTL:              EtcdTTL,
		PrevExist:        etcdClient.PrevNoExist,
		NoValueOnSuccess: true,
	}

	key := c.etcdPrefix + shardKey + "/" + strconv.Itoa(int(time.Now().Unix()))
	_, err := c.etcdKapi.Set(context.Background(), key, shardName, opt)
	if err != nil {
		return err
	}
	return nil
}

func (c *assignmentStore) getShardForPointFromEtcd(shardKey string) *shard {
	optRecursive := &etcdClient.GetOptions{
		Recursive: true,
		Quorum:    false,
	}
	resp, err := c.etcdKapi.Get(context.Background(), c.etcdPrefix+shardKey+"/", optRecursive)
	if err != nil {
		return nil
	}

	if len(resp.Node.Nodes) == 0 {
		return nil
	}

	shardName := c.getMostRecentEtcdEntry(resp.Node.Nodes)
	for _, shard := range c.shards {
		if shard.Name() == shardName {
			return &shard
		}
	}

	return nil
}

func (c *assignmentStore) renewEtcdValue(key string) {
	renewOps := &etcdClient.SetOptions{
		TTL:              EtcdTTL,
		NoValueOnSuccess: true,
		Refresh:          true,
	}

	c.etcdKapi.Set(context.Background(), key, "", renewOps)
}

// Reinsert an old item cache item if the application was just started
// or if it cannot be refreshed through ETCD.
func (c *assignmentStore) reinsertEvictingCacheItem(key string, value interface{}) {
	item := value.(*allocationCacheItem)
	if monotime.Since(c.startTime) < 30*time.Minute {
		// Application was only just started, lets preserve whatever cache we have
		item.accessTime = monotime.Now()
		item.createTime = monotime.Now()
		c.memory.Set(key, item, c.getRandExpirationTime())
		return
	}

	if monotime.Since(item.accessTime) > 24*time.Hour {
		return // evict
	}

	// Refresh item from ETCD
	shardFromEtcd := c.Get(key)

	// If shard could not be refreshed from ETCD we reinsert the old one
	if shardFromEtcd == nil && monotime.Since(item.createTime) < 24*time.Hour {
		c.memory.Set(key, item, c.getRandExpirationTime())
	}
}

func (c *assignmentStore) getMostRecentEtcdEntry(nodes etcdClient.Nodes) string {
	orderableNodes := make(orderableEtcdNodes, len(nodes))
	for k, v := range nodes {
		if v.TTL < 180*24*60*60 {
			c.renewEtcdValue(v.Key) // Cache is about to expire (in 180 ~days), renew
		}

		parts := strings.Split(v.Key, "/")
		timestamp, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			log.Println("Could not parse ETCD key for key: %s", v.Key)
		}
		orderableNodes[k] = orderableEtcdNode{
			v,
			timestamp,
		}
	}

	sort.Sort(orderableNodes)
	return orderableNodes[len(orderableNodes)-1].Value
}

// Get a Random Expiration Time between 10 and 30 minutes
func (c *assignmentStore) getRandExpirationTime() time.Duration {
	rand.Seed(int64(time.Now().Nanosecond()))
	return time.Duration(10+rand.Intn(20)) * time.Minute
}

type orderableEtcdNodes []orderableEtcdNode
type orderableEtcdNode struct {
	*etcdClient.Node
	timestamp int
}

func (a orderableEtcdNodes) Len() int           { return len(a) }
func (a orderableEtcdNodes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a orderableEtcdNodes) Less(i, j int) bool { return a[i].timestamp < a[j].timestamp }
