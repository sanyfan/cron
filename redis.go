package cron // Copyright 2018 FreeWheel. All Rights Reserved.

// Author: rfma@freewheel.tv
// Date: 06/02/2018

// This file is a redis client helper which wrap some of common commands.

import (
	"errors"
	"fmt"
	"time"

	"wheels/log"

	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"sync"
)

const (
	//MODE_LOCAL mode: server type for init different redis client by it
	MODE_LOCAL = "local"
	//MODE_SENTINEL mode
	MODE_SENTINEL = "sentinel"
	//GLOBAL like a common service name identify each domain service, and act as key prefix by default
	GLOBAL = "service:global:"
	// FORECAST_SERVICE const
	FORECAST_SERVICE  = "service:forecast:"
	INVENTORY_SERVICE = "service:inventory:"
	CREATIVE_SERVICE  = "service:creative:"
	PARTNER_SERVICE   = "service:partner:"
	ORDER_SERVICE     = "service:order:"
	TARGET_SERVICE    = "service:target:"
	METADATA_SERVICE  = "service:metadata:"

	DAL_SERVICE = "infra:dal:"
)

var (
	// safeMap.Clients save a redis client list for all of services
	safeMap *ConcurrentMap
)

// RedisCache is a wrapper for real redis client
type RedisCache struct {
	Client    *redis.Client //go-redis client
	Config    *RedisConfig  //config info
	Namespace string        //namespace as key prefix for each service
}

// RedisConfig is a configuration which is created by init from service config file
type RedisConfig struct {
	serverType string
	masterName string
	servers    []string
}

// ConcurrentMap is a wrapper for saving redis client instances and supports concurrency.
type ConcurrentMap struct {
	sync.Mutex
	//due to map has a safe issue in concurrency situation, use this struct to handle that.
	Clients map[string]*RedisCache
}

func init() {
	//init concurrent map
	safeMap = &ConcurrentMap{
		Clients: make(map[string]*RedisCache),
	}

}

//RedisCli is a global instance
var RedisCli *RedisCache

//InitRedis init redis client for ours
func InitRedisClient(c *viper.Viper, ns string) error {
	err := InitRedis(c, ns)
	RedisCli = RedisClient(ns)
	return err
}

// InitRedis function create redis client for service by config input.
// It should be called in InitApp function for service.
// Avoid to re-create the same client, it will return existing client if have.
// config format:
// 		redis:
//			type: local  #local,sentinel,cluster
//			master_name: mymaster
//			servers:
//				- 127.0.0.1:6379
func InitRedis(c *viper.Viper, service string) error {
	log.Warn("Redis init get started...")
	config, err := buildConfig(c)
	if err != nil {
		log.Errorf("build redis config error: %s", err)
		return err
	}
	//this function maybe be called by multiple thread, so add lock for concurrency issue.
	safeMap.Lock()
	defer safeMap.Unlock()
	//check client for current service is created or not
	if RedisClient(service) != nil {
		log.Warnf("redis client for %s service has been created", service)
		return nil
	}
	//init client via type
	var client *redis.Client
	switch config.serverType {
	case MODE_LOCAL:
		client = redis.NewClient(&redis.Options{
			Addr: config.servers[0],
		})
	case MODE_SENTINEL:
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    config.masterName,
			SentinelAddrs: config.servers,
		})
		//do not need default branch, build config already restrict that
	}
	//connecting check
	ping, err := client.Ping().Result()
	log.Warnf("Redis client ping result: %s", ping)
	if err != nil {
		return err
	}
	//build cache struct
	redisCache := &RedisCache{
		Client:    client,
		Config:    config,
		Namespace: service,
	}
	safeMap.Clients[service] = redisCache

	log.Warn("Redis connected successfully")
	return nil
}

// RedisClient return redis client via service name
func RedisClient(serviceName string) *RedisCache {
	if cache, exist := safeMap.Clients[serviceName]; exist {
		return cache
	}
	return nil
}

//buildConfig check config error and return RedisConfig struct
func buildConfig(c viper.Viper) (*RedisConfig, error) {
	serverType := c.GetString("redis.type")
	if serverType != MODE_LOCAL && serverType != MODE_SENTINEL {
		return nil, errors.New("config redis.type is invalid, your type is " + serverType)
	}
	master := c.GetString("redis.master_name")
	if master == "" {
		master = "mymaster"
	}
	servers := c.GetStringSlice("redis.servers")
	if len(servers) == 0 {
		return nil, errors.New("config redis.servers is empty")
	}
	return &RedisConfig{
		serverType: serverType,
		masterName: master,
		servers:    servers,
	}, nil
}

// buildKey return a real redis key with namespace as prefix
// NOTE: All of client api(get/set...) in here already add namespace as key's prefix, do not need call buildKey again.
func (c *RedisCache) buildKey(key string) string {
	return fmt.Sprintf("%s%s", c.Namespace, key)
}

// Close redis client should be called when server shutdown, and clean up cache bean
func (c *RedisCache) Close() {
	// Do not need to check client nil or not, if invoker can handle c, means client already exists.
	// The same situation for other functions.
	c.Client.Close()
	safeMap.Lock()
	safeMap.Clients[c.Namespace] = nil
	defer safeMap.Unlock()
}

// GetString is wrapping redis.get command
// client only support return basic type, so default we only return string.
// save struct data should convert to string(json) firstly
func (c *RedisCache) GetString(key string) (string, error) {
	return c.Client.Get(c.buildKey(key)).Result()
}

// Get return redis.StringCmd directly, invoker can use its' converting functions
func (c *RedisCache) Get(key string) *redis.StringCmd {
	return c.Client.Get(c.buildKey(key))
}

// Set is redis.set command, cached value with key and expire time.
// if set expiration=0, means do not need set expired time
func (c *RedisCache) Set(key string, value interface{}, expiration time.Duration) (string, error) {
	return c.Client.Set(c.buildKey(key), value, expiration).Result()
}

//Delete remove multiple keys from redis
func (c *RedisCache) Delete(keys ...string) (int64, error) {
	if len(keys) > 0 {
		var realKeys []string
		for _, key := range keys {
			realKeys = append(realKeys, c.buildKey(key))
		}
		return c.Client.Del(realKeys...).Result()
	}
	return 0, errors.New("No keys to delete")
}

//Exists check a key exists or not
func (c *RedisCache) Exists(key string) bool {
	ret, err := c.Client.Exists(c.buildKey(key)).Result()
	if ret == 0 || err != nil {
		return false
	}
	return true
}

// Expire set a expired duration for key
func (c *RedisCache) Expire(key string, expiration time.Duration) {
	c.Client.Expire(c.buildKey(key), expiration)
}

// HMGet is command hmget, multiple get data from hash table
// invoker should convert result by self
func (c *RedisCache) HMGet(key string, fields []string) ([]interface{}, error) {
	cmd := c.Client.HMGet(c.buildKey(key), fields...)
	return cmd.Result()
}

// HMSet is command hmset, multiple set data to hash table
// NOTE: the bean is being set need implement BinaryMarshaler interface
func (c *RedisCache) HMSet(key string, fields map[string]interface{}) error {
	cmd := c.Client.HMSet(c.buildKey(key), fields)
	return cmd.Err()
}

// HSet is command hset, set one data to hash table
func (c *RedisCache) HSet(key, field string, value interface{}) error {
	cmd := c.Client.HSet(c.buildKey(key), field, value)
	return cmd.Err()
}

// HGet is command hget, get one data from hash table
func (c *RedisCache) HGet(key, field string) (string, error) {
	cmd := c.Client.HGet(c.buildKey(key), field)
	return cmd.Result()
}

// HGetAll is command hget, get one data from hash table
func (c *RedisCache) HGetAll(key string) (map[string]string, error) {
	cmd := c.Client.HGetAll(c.buildKey(key))
	return cmd.Result()
}

// HDel is command hget, get one data from hash table
func (c *RedisCache) HDel(key, field string) (int64, error) {
	cmd := c.Client.HDel(c.buildKey(key), field)
	return cmd.Result()
}

// HIncrBy is command hincrby, inrc data
func (c *RedisCache) HIncrBy(key, field string, incr int64) (int64, error) {
	cmd := c.Client.HIncrBy(c.buildKey(key), field, incr)
	return cmd.Result()
}

//ListAll return all of elements for list type
func (c *RedisCache) ListAll(key string) ([]string, error) {
	cmd := c.Client.LRange(c.buildKey(key), 0, -1)
	return cmd.Result()
}

//ListPush save multiple elements into list type, recommend save string([]byte) instead of interface
func (c *RedisCache) ListPush(key string, values ...interface{}) error {
	if len(values) == 0 {
		return errors.New("values is nil, key is " + key)
	}
	cmd := c.Client.LPush(c.buildKey(key), values...)
	return cmd.Err()
}

// SAdd adds one or more members to a set.
func (c *RedisCache) SAdd(key string, members ...interface{}) (int64, error) {
	if len(members) == 0 {
		return 0, errors.New("members is nil, key is " + key)
	}
	return c.Client.SAdd(c.buildKey(key), members...).Result()
}

// SMembers gets all the members in a set.
func (c *RedisCache) SMembers(key string) ([]string, error) {
	return c.Client.SMembers(c.buildKey(key)).Result()
}

// ZRange gets the items in a sort set
func (c *RedisCache) ZRange(key string, start, stop int64) ([]string, error) {
	return c.Client.ZRange(c.buildKey(key), start, stop).Result()
}

// ZRevRange reverse the items
func (c *RedisCache) ZRevRange(key string, start, stop int64) ([]string, error) {
	return c.Client.ZRevRange(c.buildKey(key), start, stop).Result()
}

// ZRangeByScore get items rank by items' score
func (c *RedisCache) ZRangeByScore(key string, opt redis.ZRangeBy) ([]string, error) {
	return c.Client.ZRangeByScore(c.buildKey(key), opt).Result()
}

// ZRevRangeByScore get items desc rank by items' score
func (c *RedisCache) ZRevRangeByScore(key string, opt redis.ZRangeBy) ([]string, error) {
	return c.Client.ZRevRangeByScore(c.buildKey(key), opt).Result()
}

// Redis `ZADD key score member [score member ...]` command.
func (c *RedisCache) ZAdd(key string, members ...redis.Z) (int64, error) {
	const n = 2
	a := make([]interface{}, n+2*len(members))
	a[0], a[1] = "zadd", key
	return c.Client.ZAdd(c.buildKey(key), members...).Result()
}

// Get the redis opt structure
func (c *RedisCache) BuildOptZRangeBy(max string, min string, offset int64, count int64) redis.ZRangeBy {
	return redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: offset,
		Count:  count,
	}
}

// Get redis structure
func (c *RedisCache) BuildZ(score float64, member interface{}) redis.Z {
	return redis.Z{
		Score:  score,
		Member: member,
	}
}
