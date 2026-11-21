package cachex

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed read.lua
	readLua string
	//go:embed fill.lua
	fillLua string
	//go:embed invalidate.lua
	invalidateLua string

	readScript       = redis.NewScript(readLua)
	fillScript       = redis.NewScript(fillLua)
	invalidateScript = redis.NewScript(invalidateLua)
)

const (
	readBusy     int64 = 0
	readHit      int64 = 1
	readAcquired int64 = 2
	loadRetry          = 10 * time.Millisecond
)

type record[V any] struct {
	Found bool `json:"found"`
	Value V    `json:"value"`
}

// 每条记录的值和回源 token 同 slot，整个命名空间可分散到多个 slot。
func (c *Cache[K, V]) redisKeys(encodedKey []byte) []string {
	key := "{" + c.prefix + ":" + base64.RawURLEncoding.EncodeToString(encodedKey) + "}"
	return []string{key + ":value", key + ":load"}
}

func (c *Cache[K, V]) read(ctx context.Context, key K) (record[V], time.Time, error) {
	encodedKey, err := json.Marshal(key)
	if err != nil {
		return record[V]{}, time.Time{}, err
	}
	keys := c.redisKeys(encodedKey)
	token := rand.Text()
	for {
		started := time.Now()
		result, err := readScript.Run(ctx, c.redis, keys, token, c.config.LoadTimeout.Milliseconds()).Slice()
		if err != nil {
			return record[V]{}, time.Time{}, err
		}
		switch result[0].(int64) {
		case readHit:
			var item record[V]
			err := json.Unmarshal([]byte(result[1].(string)), &item)
			expires := started.Add(time.Duration(result[2].(int64)) * time.Millisecond)
			return item, expires, err
		case readAcquired:
			item, expires, stored, err := c.loadAndFill(ctx, key, keys, token)
			if err != nil {
				return record[V]{}, time.Time{}, err
			}
			if stored {
				return item, expires, nil
			}
			// 失效或租约到期，重新读取，丢弃旧查询的结果。
			token = rand.Text()
		case readBusy:
			timer := time.NewTimer(loadRetry)
			select {
			case <-ctx.Done():
				timer.Stop()
				return record[V]{}, time.Time{}, ctx.Err()
			case <-timer.C:
			}
		default:
			panic("cachex: invalid Redis read status")
		}
	}
}

func (c *Cache[K, V]) loadAndFill(ctx context.Context, key K, keys []string, token string) (record[V], time.Time, bool, error) {
	loadCtx, cancel := context.WithTimeout(ctx, c.config.LoadTimeout)
	value, found, err := c.load(loadCtx, key)
	if err == nil {
		err = loadCtx.Err()
	}
	cancel()

	if !found {
		var zero V
		value = zero
	}
	item := record[V]{Found: found, Value: value}
	var payload []byte
	if err == nil {
		payload, err = json.Marshal(item)
	}
	if err != nil {
		// 失败后释放自己的占位，让其他读请求不必等租约过期。
		releaseCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer stop()
		releaseErr := fillScript.Run(releaseCtx, c.redis, keys, token, "", 0).Err()
		return record[V]{}, time.Time{}, false, errors.Join(err, releaseErr)
	}

	ttl := c.config.RedisTTL
	if !found {
		ttl = c.config.MissTTL
	}
	expires := time.Now().Add(time.Duration(ttl.Milliseconds()) * time.Millisecond)
	stored, err := fillScript.Run(ctx, c.redis, keys, token, payload, ttl.Milliseconds()).Int64()
	return item, expires, stored == 1, err
}
