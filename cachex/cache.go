package cachex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/redis/go-redis/v9"
)

type Loader[K comparable, V any] func(ctx context.Context, key K) (value V, found bool, err error)

type Config struct {
	Prefix        string
	LocalCapacity int
	LocalTTL      time.Duration
	RedisTTL      time.Duration
	MissTTL       time.Duration
	LoadTimeout   time.Duration
}

type Cache[K comparable, V any] struct {
	redis   redis.UniversalClient
	load    Loader[K, V]
	config  Config
	prefix  string
	channel string

	mu        sync.Mutex
	running   bool
	listening bool
	local     *simplelru.LRU[K, localEntry[V]]
	flights   map[K]chan struct{}
}

type localEntry[V any] struct {
	item    record[V]
	expires time.Time
}

func New[K comparable, V any](client redis.UniversalClient, load Loader[K, V], config Config) *Cache[K, V] {
	if client == nil || load == nil || config.Prefix == "" {
		panic("cachex: redis, loader and prefix are required")
	}
	if config.LocalCapacity == 0 {
		config.LocalCapacity = 10000
	}
	if config.LocalTTL == 0 {
		config.LocalTTL = 5 * time.Second
	}
	if config.RedisTTL == 0 {
		config.RedisTTL = 5 * time.Minute
	}
	if config.MissTTL == 0 {
		config.MissTTL = 2 * time.Second
	}
	if config.LoadTimeout == 0 {
		config.LoadTimeout = 5 * time.Second
	}
	if config.LocalTTL <= 0 || config.RedisTTL < time.Millisecond ||
		config.MissTTL < time.Millisecond || config.LoadTimeout < time.Millisecond {
		panic("cachex: TTLs must be positive")
	}
	prefix := "cachex:" + base64.RawURLEncoding.EncodeToString([]byte(config.Prefix))
	local, err := simplelru.NewLRU[K, localEntry[V]](config.LocalCapacity, nil)
	if err != nil {
		panic(err)
	}
	return &Cache[K, V]{
		redis:   client,
		load:    load,
		config:  config,
		prefix:  prefix,
		channel: prefix + ":invalidate",
		local:   local,
		flights: make(map[K]chan struct{}),
	}
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	for {
		if err := ctx.Err(); err != nil {
			return zero, false, err
		}
		c.mu.Lock()
		if entry, ok := c.local.Get(key); ok {
			if time.Now().Before(entry.expires) {
				c.mu.Unlock()
				return entry.item.Value, entry.item.Found, nil
			}
			c.local.Remove(key)
		}
		if !c.listening {
			c.mu.Unlock()
			item, _, err := c.read(ctx, key)
			return item.Value, item.Found, err
		}
		if pending := c.flights[key]; pending != nil {
			c.mu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return zero, false, ctx.Err()
			}
		}
		pending := make(chan struct{})
		c.flights[key] = pending
		c.mu.Unlock()

		item, expires, err := c.read(ctx, key)
		c.mu.Lock()
		current := c.flights[key] == pending
		if current {
			delete(c.flights, key)
			close(pending)
			now := time.Now()
			if err == nil && now.Before(expires) {
				if localExpires := now.Add(c.config.LocalTTL); localExpires.Before(expires) {
					expires = localExpires
				}
				c.local.Add(key, localEntry[V]{
					item:    item,
					expires: expires,
				})
			}
		}
		c.mu.Unlock()
		if err != nil {
			return zero, false, err
		}
		if current {
			return item.Value, item.Found, nil
		}
		// 已收到失效或订阅重置。旧查询不能回填，新读者也不会等待它。
	}
}

func (c *Cache[K, V]) Invalidate(ctx context.Context, key K) error {
	encodedKey, err := json.Marshal(key)
	if err != nil {
		return err
	}
	err = invalidateScript.Run(ctx, c.redis, c.redisKeys(encodedKey), c.channel, encodedKey).Err()
	c.invalidateLocal(key)
	return err
}

func (c *Cache[K, V]) invalidateLocal(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.local.Remove(key)
	if pending := c.flights[key]; pending != nil {
		delete(c.flights, key)
		close(pending)
	}
}
