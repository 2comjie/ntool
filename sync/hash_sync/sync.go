package hash_sync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	randv2 "math/rand/v2"
	"strings"
	"time"

	"github.com/2comjie/ntool/logx"
	"github.com/redis/go-redis/v9"
)

type DbHandler[K comparable, F comparable, T any] interface {
	Get(ctx context.Context, key K, field F) (*T, error)
	WriteBatch(ctx context.Context, key K, fields []F, values []*T) error
	DeleteBatch(ctx context.Context, key K, fields []F) error
}

var ErrLeaseLost = errors.New("hash_sync: flush lease lost")

type Config struct {
	Prefix      string
	CacheTTL    time.Duration
	MissTTL     time.Duration
	LoadTimeout time.Duration
	Interval    time.Duration
	LeaseTTL    time.Duration
	BatchSize   int64
}

type HashSync[K comparable, F comparable, T any] struct {
	rc         redis.UniversalClient
	db         DbHandler[K, F, T]
	conf       Config
	prefix     string
	dirtyKey   string
	workingKey string
	ownersKey  string
}

type record[K comparable, F comparable, T any] struct {
	Key   K  `json:"key"`
	Field F  `json:"field"`
	Value *T `json:"value"`
}

// New 中 K、F 的 JSON 表示必须唯一，K、F、T 必须支持 JSON 往返序列化。
// DbHandler 必须执行幂等的字段 upsert/delete，并响应 context 取消。
func New[K comparable, F comparable, T any](rc redis.UniversalClient, db DbHandler[K, F, T], conf Config) *HashSync[K, F, T] {
	if rc == nil || db == nil || conf.Prefix == "" {
		panic("hash_sync: redis, db and prefix are required")
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
	if conf.LeaseTTL == 0 {
		conf.LeaseTTL = 30 * time.Second
	}
	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	if conf.CacheTTL < time.Millisecond || conf.MissTTL < time.Millisecond || conf.LoadTimeout < time.Millisecond || conf.LeaseTTL < 3*time.Millisecond || conf.Interval < 0 || conf.BatchSize < 0 {
		panic("hash_sync: invalid TTL, interval or batch size")
	}
	prefix := "{hash_sync:" + base64.RawURLEncoding.EncodeToString([]byte(conf.Prefix)) + "}:"
	return &HashSync[K, F, T]{
		rc: rc, db: db, conf: conf, prefix: prefix,
		dirtyKey: prefix + "dirty", workingKey: prefix + "working", ownersKey: prefix + "owners",
	}
}

func (s *HashSync[K, F, T]) location(key K, field F) (dataKey, fieldName, task string, err error) {
	keyData, err := json.Marshal(key)
	if err != nil {
		return "", "", "", err
	}
	fieldData, err := json.Marshal(field)
	if err != nil {
		return "", "", "", err
	}
	keyName := base64.RawURLEncoding.EncodeToString(keyData)
	fieldName = base64.RawURLEncoding.EncodeToString(fieldData)
	return s.prefix + "data:" + keyName, fieldName, keyName + "." + fieldName, nil
}

// Get 返回 nil, nil 表示该 field 不存在。Redis 错误直接返回，不绕过 Redis 回源。
func (s *HashSync[K, F, T]) Get(ctx context.Context, key K, field F) (*T, error) {
	dataKey, fieldName, _, err := s.location(key, field)
	if err != nil {
		return nil, err
	}
	loadKey, missKey := dataKey+":load:"+fieldName, dataKey+":miss:"+fieldName
	token := rand.Text()
	redisKeys := []string{dataKey, dataKey + ":dirty", loadKey, missKey}
	for {
		result, err := readScript.Run(ctx, s.rc, redisKeys, fieldName, token, s.conf.LoadTimeout.Milliseconds()).Slice()
		if err != nil {
			return nil, err
		}
		switch result[0].(int64) {
		case 1:
			if result[1].(string) == "" {
				return nil, nil
			}
			var item record[K, F, T]
			if err := json.Unmarshal([]byte(result[1].(string)), &item); err != nil {
				return nil, err
			}
			return item.Value, nil
		case 2:
			loadCtx, cancel := context.WithTimeout(ctx, s.conf.LoadTimeout)
			value, dbErr := s.db.Get(loadCtx, key, field)
			if dbErr == nil {
				dbErr = loadCtx.Err()
			}
			cancel()
			var payload []byte
			if dbErr == nil && value != nil {
				payload, dbErr = json.Marshal(record[K, F, T]{Key: key, Field: field, Value: value})
			}
			ttl := s.conf.CacheTTL + time.Duration(randv2.Int64N(int64(s.conf.CacheTTL/10)+1))
			stored, err := fillScript.Run(ctx, s.rc, redisKeys,
				fieldName, token, string(payload), dbErr == nil, ttl.Milliseconds(), s.conf.MissTTL.Milliseconds()).Int64()
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

// GetBatch 保持 fields 的输入顺序，各 field 独立参与跨实例回源合并。
func (s *HashSync[K, F, T]) GetBatch(ctx context.Context, key K, fields []F) ([]*T, error) {
	values := make([]*T, len(fields))
	for i, field := range fields {
		value, err := s.Get(ctx, key, field)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func (s *HashSync[K, F, T]) Write(ctx context.Context, key K, field F, value *T) error {
	return s.WriteBatch(ctx, key, []F{field}, []*T{value})
}

func (s *HashSync[K, F, T]) WriteBatch(ctx context.Context, key K, fields []F, values []*T) error {
	if len(fields) != len(values) {
		return errors.New("hash_sync: fields and values length mismatch")
	}
	for _, value := range values {
		if value == nil {
			return errors.New("hash_sync: use Delete to remove a field")
		}
	}
	return s.write(ctx, key, fields, values)
}

func (s *HashSync[K, F, T]) Delete(ctx context.Context, key K, field F) error {
	return s.DeleteBatch(ctx, key, []F{field})
}

func (s *HashSync[K, F, T]) DeleteBatch(ctx context.Context, key K, fields []F) error {
	return s.write(ctx, key, fields, make([]*T, len(fields)))
}

func (s *HashSync[K, F, T]) write(ctx context.Context, key K, fields []F, values []*T) error {
	if len(fields) == 0 {
		return nil
	}
	redisKeys := []string{s.dirtyKey, s.workingKey}
	var args []any
	for i, field := range fields {
		dataKey, fieldName, task, err := s.location(key, field)
		if err != nil {
			return err
		}
		if i == 0 {
			redisKeys = append(redisKeys, dataKey, dataKey+":dirty")
		}
		payload, err := json.Marshal(record[K, F, T]{Key: key, Field: field, Value: values[i]})
		if err != nil {
			return err
		}
		redisKeys = append(redisKeys, dataKey+":load:"+fieldName, dataKey+":miss:"+fieldName)
		args = append(args, task, fieldName, string(payload))
	}
	return writeScript.Run(ctx, s.rc, redisKeys, args...).Err()
}

// Flush 原子领取最多 BatchSize 个 field，各实例领取的有效任务不重叠。
// DB 为至少一次同步语义；租约失效后的迟到 DB 提交需要 DB 端 fencing 才能阻止。
func (s *HashSync[K, F, T]) Flush(ctx context.Context) (err error) {
	token := rand.Text()
	queueKeys := []string{s.dirtyKey, s.workingKey, s.ownersKey}
	tasks, err := claimScript.Run(ctx, s.rc, queueKeys, token, s.conf.LeaseTTL.Milliseconds(), s.conf.BatchSize).StringSlice()
	if err != nil || len(tasks) == 0 {
		return err
	}
	flushCtx, cancel := context.WithCancel(ctx)
	renewCtx, stopRenew := context.WithCancel(flushCtx)
	done := make(chan struct{})
	defer func() {
		cancel()
		stopRenew()
		<-done
		if err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cleanupCancel()
			args := []any{token}
			for _, task := range tasks {
				args = append(args, task)
			}
			err = errors.Join(err, releaseScript.Run(cleanupCtx, s.rc, queueKeys, args...).Err())
		}
	}()
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.conf.LeaseTTL / 3)
		defer ticker.Stop()
		args := []any{token, s.conf.LeaseTTL.Milliseconds()}
		for _, task := range tasks {
			args = append(args, task)
		}
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				ok, err := renewScript.Run(renewCtx, s.rc, []string{s.workingKey, s.ownersKey}, args...).Int64()
				if renewCtx.Err() != nil {
					return
				}
				if err != nil || ok != 1 {
					logx.Errorf("hash_sync %s: renew failed: %v", s.conf.Prefix, errors.Join(ErrLeaseLost, err))
					cancel()
					return
				}
			}
		}
	}()

	pipe := s.rc.Pipeline()
	commands := make([]*redis.StringCmd, len(tasks))
	ackKeys := append([]string{}, queueKeys...)
	args := []any{token, s.conf.MissTTL.Milliseconds()}
	for i, task := range tasks {
		keyName, fieldName, _ := strings.Cut(task, ".")
		dataKey := s.prefix + "data:" + keyName
		commands[i] = pipe.HGet(flushCtx, dataKey, fieldName)
		ackKeys = append(ackKeys, dataKey, dataKey+":dirty", dataKey+":miss:"+fieldName)
	}
	if _, err := pipe.Exec(flushCtx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	type batch struct {
		fields  []F
		values  []*T
		deleted []F
	}
	batches := make(map[K]*batch)
	for i, command := range commands {
		payload, err := command.Result()
		if errors.Is(err, redis.Nil) {
			return fmt.Errorf("hash_sync: dirty field missing: %s", tasks[i])
		}
		if err != nil {
			return err
		}
		var item record[K, F, T]
		if err := json.Unmarshal([]byte(payload), &item); err != nil {
			return err
		}
		group := batches[item.Key]
		if group == nil {
			group = &batch{}
			batches[item.Key] = group
		}
		if item.Value == nil {
			group.deleted = append(group.deleted, item.Field)
		} else {
			group.fields = append(group.fields, item.Field)
			group.values = append(group.values, item.Value)
		}
		_, fieldName, _ := strings.Cut(tasks[i], ".")
		ttl := s.conf.CacheTTL + time.Duration(randv2.Int64N(int64(s.conf.CacheTTL/10)+1))
		args = append(args, tasks[i], fieldName, payload, item.Value == nil, ttl.Milliseconds())
	}
	for key, group := range batches {
		if len(group.fields) > 0 {
			if err := s.db.WriteBatch(flushCtx, key, group.fields, group.values); err != nil {
				return err
			}
		}
		if len(group.deleted) > 0 {
			if err := s.db.DeleteBatch(flushCtx, key, group.deleted); err != nil {
				return err
			}
		}
	}
	// 确认前停止续租，避免已确认任务被续租协程误判为丢失。
	stopRenew()
	<-done
	if err := flushCtx.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrLeaseLost
	}
	acked, err := ackScript.Run(flushCtx, s.rc, ackKeys, args...).Int64()
	if err != nil {
		return err
	}
	if acked != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *HashSync[K, F, T]) Run(ctx context.Context) error {
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
				logx.Errorf("hash_sync %s: flush failed: %v", s.conf.Prefix, err)
			}
		}
	}
}
