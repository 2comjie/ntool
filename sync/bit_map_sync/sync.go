package bit_map_sync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	randv2 "math/rand/v2"
	"time"

	"github.com/2comjie/ntool/logx"
	"github.com/redis/go-redis/v9"
)

// DbHandler 读写整张 Bitmap 的原始字节；Get 返回 nil, nil 表示不存在。
// WriteBatch 必须幂等覆盖，DeleteBatch 删除不存在的记录也应成功。
type DbHandler[K comparable] interface {
	Get(ctx context.Context, key K) ([]byte, error)
	WriteBatch(ctx context.Context, keys []K, values [][]byte) error
	DeleteBatch(ctx context.Context, keys []K) error
}

var ErrLeaseLost = errors.New("bit_map_sync: flush lease lost")

type Config struct {
	Prefix      string
	CacheTTL    time.Duration
	MissTTL     time.Duration
	LoadTimeout time.Duration
	Interval    time.Duration
	LeaseTTL    time.Duration
	BatchSize   int64
}

type BitmapSync[K comparable] struct {
	rc         redis.UniversalClient
	db         DbHandler[K]
	conf       Config
	prefix     string
	dirtyKey   string
	workingKey string
	ownersKey  string
}

// New 不启动 goroutine；通过 Run 驱动定期同步。
// K 的 JSON 表示必须唯一，并能往返序列化；DbHandler 必须响应 context 取消。
func New[K comparable](rc redis.UniversalClient, db DbHandler[K], conf Config) *BitmapSync[K] {
	if rc == nil || db == nil || conf.Prefix == "" {
		panic("bit_map_sync: redis, db and prefix are required")
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
		conf.BatchSize = 100
	}
	if conf.CacheTTL < time.Millisecond || conf.MissTTL < time.Millisecond || conf.LoadTimeout < time.Millisecond || conf.LeaseTTL < 3*time.Millisecond || conf.Interval < 0 || conf.BatchSize < 0 {
		panic("bit_map_sync: invalid TTL, interval or batch size")
	}
	prefix := "{bit_map_sync:" + base64.RawURLEncoding.EncodeToString([]byte(conf.Prefix)) + "}:"
	return &BitmapSync[K]{
		rc: rc, db: db, conf: conf, prefix: prefix,
		dirtyKey: prefix + "dirty", workingKey: prefix + "working", ownersKey: prefix + "owners",
	}
}

func (s *BitmapSync[K]) location(key K) (dataKey, task string, err error) {
	data, err := json.Marshal(key)
	if err != nil {
		return "", "", err
	}
	task = base64.RawURLEncoding.EncodeToString(data)
	return s.prefix + "data:" + task, task, nil
}

// GetBit 对不存在的 Bitmap 或尚未分配的 offset 返回 false。
func (s *BitmapSync[K]) GetBit(ctx context.Context, key K, offset int64) (bool, error) {
	if offset < 0 || offset >= 1<<32 {
		return false, errors.New("bit_map_sync: offset must be in [0, 2^32)")
	}
	dataKey, task, err := s.location(key)
	if err != nil {
		return false, err
	}
	redisKeys := []string{dataKey, dataKey + ":load", s.dirtyKey, s.workingKey}
	token := rand.Text()
	for {
		result, err := readScript.Run(ctx, s.rc, redisKeys, task, offset, token, s.conf.LoadTimeout.Milliseconds()).Slice()
		if err != nil {
			return false, err
		}
		switch result[0].(int64) {
		case 1:
			return result[1].(int64) == 1, nil
		case 2:
			loadCtx, cancel := context.WithTimeout(ctx, s.conf.LoadTimeout)
			value, dbErr := s.db.Get(loadCtx, key)
			if dbErr == nil {
				dbErr = loadCtx.Err()
			}
			cancel()
			ttl := s.conf.MissTTL
			if value != nil {
				ttl = s.conf.CacheTTL + time.Duration(randv2.Int64N(int64(s.conf.CacheTTL/10)+1))
			}
			// 空字节 String 也表示已加载，避免把合法的全零 Bitmap 当成未命中。
			_, err = fillScript.Run(ctx, s.rc, redisKeys[:2], token, string(value), dbErr == nil, ttl.Milliseconds()).Int64()
			if dbErr != nil || err != nil {
				return false, errors.Join(dbErr, err)
			}
		case 0:
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return false, ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// SetBit 未命中时先完整加载 DB，避免丢失其他已经置位的 bit。
func (s *BitmapSync[K]) SetBit(ctx context.Context, key K, offset int64, value bool) error {
	if offset < 0 || offset >= 1<<32 {
		return errors.New("bit_map_sync: offset must be in [0, 2^32)")
	}
	dataKey, task, err := s.location(key)
	if err != nil {
		return err
	}
	redisKeys := []string{dataKey, dataKey + ":deleted", dataKey + ":load", s.dirtyKey, s.workingKey}
	for {
		written, err := writeScript.Run(ctx, s.rc, redisKeys, task, offset, value).Int64()
		if err != nil {
			return err
		}
		if written == 1 {
			return nil
		}
		if _, err := s.GetBit(ctx, key, offset); err != nil {
			return err
		}
	}
}

// Delete 标记整张 Bitmap 待删除；清除单个 bit 使用 SetBit(false)。
func (s *BitmapSync[K]) Delete(ctx context.Context, key K) error {
	dataKey, task, err := s.location(key)
	if err != nil {
		return err
	}
	return deleteScript.Run(ctx, s.rc, []string{dataKey, dataKey + ":deleted", dataKey + ":load", s.dirtyKey, s.workingKey}, task).Err()
}

// Flush 最多领取 BatchSize 个 Bitmap key，不同实例的有效领取不重叠。
// DB 为至少一次同步；租约失效后的迟到 DB 提交仍需要 DB 端 fencing。
func (s *BitmapSync[K]) Flush(ctx context.Context) (err error) {
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
					logx.Errorf("bit_map_sync %s: renew failed: %v", s.conf.Prefix, errors.Join(ErrLeaseLost, err))
					cancel()
					return
				}
			}
		}
	}()

	var dataKeys []string
	var keys []K
	for _, task := range tasks {
		keyData, err := base64.RawURLEncoding.DecodeString(task)
		if err != nil {
			return err
		}
		var key K
		if err := json.Unmarshal(keyData, &key); err != nil {
			return err
		}
		keys = append(keys, key)
		dataKey := s.prefix + "data:" + task
		dataKeys = append(dataKeys, dataKey, dataKey+":deleted")
	}
	// 原始字节与整张删除标记在同一 Lua 中读取，获得一致快照。
	snapshots, err := snapshotScript.Run(flushCtx, s.rc, dataKeys).Slice()
	if err != nil {
		return err
	}
	var writeKeys, deleteKeys []K
	var values [][]byte
	args := []any{token, s.conf.MissTTL.Milliseconds()}
	for i, key := range keys {
		payload := snapshots[i*2].(string)
		deleted := snapshots[i*2+1].(int64)
		if deleted == 1 {
			deleteKeys = append(deleteKeys, key)
		} else {
			writeKeys = append(writeKeys, key)
			values = append(values, []byte(payload))
		}
		ttl := s.conf.CacheTTL + time.Duration(randv2.Int64N(int64(s.conf.CacheTTL/10)+1))
		args = append(args, tasks[i], payload, deleted, ttl.Milliseconds())
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
	stopRenew()
	<-done
	if err := flushCtx.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrLeaseLost
	}
	acked, err := ackScript.Run(flushCtx, s.rc, append(queueKeys, dataKeys...), args...).Int64()
	if err != nil {
		return err
	}
	if acked != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *BitmapSync[K]) Run(ctx context.Context) error {
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
				logx.Errorf("bit_map_sync %s: flush failed: %v", s.conf.Prefix, err)
			}
		}
	}
}
