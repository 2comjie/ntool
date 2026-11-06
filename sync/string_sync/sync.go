package string_sync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	randv2 "math/rand/v2"
	"time"

	redisLock "github.com/2comjie/ntool/lock/redis"
	"github.com/2comjie/ntool/logx"
	"github.com/redis/go-redis/v9"
)

type DbHandler[K comparable, T any] interface {
	GetBatch(ctx context.Context, keys []K) ([]*T, error)
	WriteBatch(ctx context.Context, keys []K, values []*T) error
	DeleteBatch(ctx context.Context, keys []K) error

	Get(ctx context.Context, key K) (*T, error)
	Write(ctx context.Context, key K, value *T) error
	Delete(ctx context.Context, key K) error
}

type Config struct {
	// 同一份业务数据的所有实例必须使用相同且独占的 Prefix。
	Prefix string
	// CacheTTL 仅用于已落库数据，附加最多 10% 的随机过期时间。
	CacheTTL time.Duration
	MissTTL  time.Duration
	// LoadTimeout 是一次回源的超时，同时也是 Redis 回源占位的有效时间。
	LoadTimeout time.Duration
	Interval    time.Duration
	// BatchSize 限制一次 Flush 的最大条目数。
	BatchSize int64
}

type StringSync[K comparable, T any] struct {
	rc       redis.UniversalClient
	db       DbHandler[K, T]
	conf     Config
	prefix   string
	dirtyKey string
	flushKey string
}

type record[K comparable, T any] struct {
	Key     K      `json:"key"`
	Value   *T     `json:"value"`
	Version string `json:"version"`
}

// New 不启动 goroutine，调用者使用 Run 驱动定期落库。
// K、T 使用 JSON 序列化，K 的 JSON 表示必须唯一且可以反序列化回 K。
func New[K comparable, T any](rc redis.UniversalClient, db DbHandler[K, T], conf Config) *StringSync[K, T] {
	if rc == nil || db == nil || conf.Prefix == "" {
		panic("string_sync: redis, db and prefix are required")
	}
	if conf.CacheTTL == 0 {
		conf.CacheTTL = 5 * time.Minute
	}
	if conf.MissTTL == 0 {
		conf.MissTTL = 5 * time.Second
	}
	if conf.LoadTimeout == 0 {
		conf.LoadTimeout = 5 * time.Second
	}
	if conf.Interval == 0 {
		conf.Interval = time.Second
	}
	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	if conf.CacheTTL < time.Millisecond || conf.MissTTL < time.Millisecond || conf.LoadTimeout < time.Millisecond || conf.Interval < 0 || conf.BatchSize < 0 {
		panic("string_sync: invalid TTL, interval or batch size")
	}
	// 同一命名空间放在同一 Redis Cluster slot，保证 Lua 多 key 操作的原子性。
	prefix := "{string_sync:" + base64.RawURLEncoding.EncodeToString([]byte(conf.Prefix)) + "}:"
	return &StringSync[K, T]{rc: rc, db: db, conf: conf, prefix: prefix, dirtyKey: prefix + "dirty", flushKey: prefix + "flush"}
}

func (s *StringSync[K, T]) dataKey(key K) (string, error) {
	data, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return s.prefix + "data:" + base64.RawURLEncoding.EncodeToString(data), nil
}

// Get 返回 nil, nil 表示不存在。Redis 错误直接返回，不绕过 Redis 回源。
func (s *StringSync[K, T]) Get(ctx context.Context, key K) (*T, error) {
	dataKey, err := s.dataKey(key)
	if err != nil {
		return nil, err
	}
	loadKey := dataKey + ":load"
	token := rand.Text()
	for {
		result, err := readScript.Run(ctx, s.rc, []string{dataKey, s.dirtyKey, loadKey}, token, s.conf.LoadTimeout.Milliseconds()).Slice()
		if err != nil {
			return nil, err
		}
		switch result[0].(int64) {
		case 1:
			var item record[K, T]
			if err := json.Unmarshal([]byte(result[1].(string)), &item); err != nil {
				return nil, err
			}
			return item.Value, nil
		case 2:
			loadCtx, cancel := context.WithTimeout(ctx, s.conf.LoadTimeout)
			value, dbErr := s.db.Get(loadCtx, key)
			if dbErr == nil {
				dbErr = loadCtx.Err()
			}
			cancel()
			var payload []byte
			if dbErr == nil {
				payload, dbErr = json.Marshal(record[K, T]{Key: key, Value: value, Version: token})
			}
			ttl := s.conf.MissTTL
			if value != nil {
				ttl = s.conf.CacheTTL + time.Duration(randv2.Int64N(int64(s.conf.CacheTTL/10)+1))
			}
			// token 已被写入或另一个回源替换时，旧查询不能回填。
			stored, err := fillScript.Run(ctx, s.rc, []string{dataKey, loadKey}, token, string(payload), ttl.Milliseconds()).Int64()
			if dbErr != nil || err != nil {
				return nil, errors.Join(dbErr, err)
			}
			if stored == 1 {
				return value, nil
			}
		case 0:
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// GetBatch 的结果保持输入顺序；每个缺失 key 分别参与跨实例回源合并。
func (s *StringSync[K, T]) GetBatch(ctx context.Context, keys []K) ([]*T, error) {
	values := make([]*T, len(keys))
	for i, key := range keys {
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func (s *StringSync[K, T]) Write(ctx context.Context, key K, value *T) error {
	return s.WriteBatch(ctx, []K{key}, []*T{value})
}

func (s *StringSync[K, T]) WriteBatch(ctx context.Context, keys []K, values []*T) error {
	if len(keys) != len(values) {
		return errors.New("string_sync: keys and values length mismatch")
	}
	for _, value := range values {
		if value == nil {
			return errors.New("string_sync: use Delete to remove a value")
		}
	}
	return s.write(ctx, keys, values)
}

func (s *StringSync[K, T]) Delete(ctx context.Context, key K) error {
	return s.DeleteBatch(ctx, []K{key})
}

func (s *StringSync[K, T]) DeleteBatch(ctx context.Context, keys []K) error {
	return s.write(ctx, keys, make([]*T, len(keys)))
}

func (s *StringSync[K, T]) write(ctx context.Context, keys []K, values []*T) error {
	if len(keys) == 0 {
		return nil
	}
	redisKeys := []string{s.dirtyKey}
	args := []any{time.Now().UnixMilli()}
	for i, key := range keys {
		dataKey, err := s.dataKey(key)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(record[K, T]{Key: key, Value: values[i], Version: rand.Text()})
		if err != nil {
			return err
		}
		redisKeys = append(redisKeys, dataKey, dataKey+":load")
		args = append(args, string(payload))
	}
	return writeScript.Run(ctx, s.rc, redisKeys, args...).Err()
}

// Flush 最多同步 BatchSize 条记录。多个实例对同一 Prefix 的 Flush 互斥。
// DB 成功后按版本确认；本轮读取之后的新写入仍留在脏集合中。
func (s *StringSync[K, T]) Flush(ctx context.Context) (err error) {
	lease, locked, err := redisLock.TryLock(s.rc, ctx, s.flushKey, 30*time.Second, 0)
	if err != nil || !locked {
		return err
	}
	defer func() { err = errors.Join(err, lease.Unlock(context.WithoutCancel(ctx))) }()
	flushCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-lease.Done():
			cancel()
		case <-flushCtx.Done():
		}
	}()

	dataKeys, err := s.rc.ZRange(flushCtx, s.dirtyKey, 0, s.conf.BatchSize-1).Result()
	if err != nil || len(dataKeys) == 0 {
		return err
	}
	payloads, err := s.rc.MGet(flushCtx, dataKeys...).Result()
	if err != nil {
		return err
	}
	var writeKeys, deleteKeys []K
	var values []*T
	args := []any{time.Now().UnixMilli()}
	for i, payload := range payloads {
		if payload == nil {
			return fmt.Errorf("string_sync: dirty data missing: %s", dataKeys[i])
		}
		var item record[K, T]
		if err := json.Unmarshal([]byte(payload.(string)), &item); err != nil {
			return err
		}
		ttl := s.conf.MissTTL
		if item.Value == nil {
			deleteKeys = append(deleteKeys, item.Key)
		} else {
			writeKeys = append(writeKeys, item.Key)
			values = append(values, item.Value)
			ttl = s.conf.CacheTTL + time.Duration(randv2.Int64N(int64(s.conf.CacheTTL/10)+1))
		}
		args = append(args, payload, ttl.Milliseconds())
	}
	if len(writeKeys) > 0 {
		if err := s.db.WriteBatch(flushCtx, writeKeys, values); err != nil {
			return err
		}
	}
	if len(deleteKeys) > 0 {
		if err := s.db.DeleteBatch(flushCtx, deleteKeys); err != nil {
			return err
		}
	}
	if err := flushCtx.Err(); err != nil {
		return err
	}
	return ackScript.Run(flushCtx, s.rc, append([]string{s.dirtyKey}, dataKeys...), args...).Err()
}

// Run 阻塞到 ctx 取消。落库失败当轮报警，下轮重试；不保存后台错误状态。
// 停机前需要排空时，调用者停止业务写入，再用独立的有效 ctx 调用 Flush。
func (s *StringSync[K, T]) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.conf.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Flush(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				logx.Errorf("string_sync %s: flush failed: %v", s.conf.Prefix, err)
			}
		}
	}
}
