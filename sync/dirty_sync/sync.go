package dirty_sync

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"time"

	"github.com/2comjie/ntool/logx"
	"github.com/redis/go-redis/v9"
)

// Handler 处理一批 key。返回错误时整批重排，因此处理必须幂等并响应 ctx 取消。
type Handler[K comparable] func(ctx context.Context, keys []K) error

var ErrLeaseLost = errors.New("dirty_sync: lease lost")

type Config struct {
	// Prefix 原样用于元数据 key；Cluster 事务要求它与业务 key 使用相同 hash tag。
	Prefix    string
	Interval  time.Duration
	BatchSize int64
	LeaseTTL  time.Duration
}

type Sync[K comparable] struct {
	rc      redis.UniversalClient
	handler Handler[K]
	conf    Config
	// pending、working、owners、redirty，顺序与 Lua 的 KEYS 一致。
	keys []string
}

// New 不启动后台任务。K 的 JSON 表示必须唯一且支持往返序列化。
func New[K comparable](rc redis.UniversalClient, conf Config, handler Handler[K]) *Sync[K] {
	if rc == nil || handler == nil || conf.Prefix == "" {
		panic("dirty_sync: redis, prefix and handler are required")
	}
	if conf.Interval == 0 {
		conf.Interval = time.Second
	}
	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	if conf.LeaseTTL == 0 {
		conf.LeaseTTL = 30 * time.Second
	}
	if conf.Interval < 0 || conf.BatchSize < 0 || conf.LeaseTTL < 3*time.Millisecond {
		panic("dirty_sync: invalid interval, batch size or lease TTL")
	}
	return &Sync[K]{
		rc: rc, handler: handler, conf: conf,
		keys: []string{conf.Prefix + ":pending", conf.Prefix + ":working", conf.Prefix + ":owners", conf.Prefix + ":redirty"},
	}
}

// MarkDirty 合并重复标脏；处理中再次标脏会在本轮结束后重新排队。
func (s *Sync[K]) MarkDirty(ctx context.Context, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	args := make([]any, len(keys))
	for i, key := range keys {
		data, err := json.Marshal(key)
		if err != nil {
			return err
		}
		args[i] = string(data)
	}
	return markScript.Run(ctx, s.rc, s.keys, args...).Err()
}

// MarkDirtyTx 只入队，不执行事务；序列化错误立即返回，执行期错误由上层 TxPipelined/Exec 返回。
// 与业务写入一起提交时必须使用事务 Pipeline，普通 Pipeline 不保证原子性。
func (s *Sync[K]) MarkDirtyTx(ctx context.Context, tx redis.Pipeliner, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	args := make([]any, len(keys))
	for i, key := range keys {
		data, err := json.Marshal(key)
		if err != nil {
			return err
		}
		args[i] = string(data)
	}
	// 事务中的 EVALSHA 遇到 NOSCRIPT 时无法在本轮 EXEC 中回退，直接使用 EVAL。
	return markScript.Eval(ctx, tx, s.keys, args...).Err()
}

// Run 阻塞到 ctx 取消。失败当轮记录错误，后续 tick 继续处理。
func (s *Sync[K]) Run(ctx context.Context) error {
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
				logx.Errorf("dirty_sync %s: flush failed: %v", s.conf.Prefix, err)
			}
		}
	}
}

// Flush 最多处理 BatchSize 个 key；没有可领取任务时也返回 nil。
// 多实例的有效领取不重叠，Redis 租约不能撤回 Handler 已经执行的外部写入。
func (s *Sync[K]) Flush(ctx context.Context) (err error) {
	token := rand.Text()
	tasks, err := claimScript.Run(ctx, s.rc, s.keys, token, s.conf.LeaseTTL.Milliseconds(), s.conf.BatchSize).StringSlice()
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
			err = errors.Join(err, releaseScript.Run(cleanupCtx, s.rc, s.keys, args...).Err())
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
				ok, err := renewScript.Run(renewCtx, s.rc, s.keys[1:3], args...).Int64()
				if renewCtx.Err() != nil {
					return
				}
				if err != nil || ok != 1 {
					logx.Errorf("dirty_sync %s: renew failed: %v", s.conf.Prefix, errors.Join(ErrLeaseLost, err))
					cancel()
					return
				}
			}
		}
	}()

	keys := make([]K, len(tasks))
	for i, task := range tasks {
		if err := json.Unmarshal([]byte(task), &keys[i]); err != nil {
			return err
		}
	}
	if err := s.handler(flushCtx, keys); err != nil {
		return err
	}
	// 停止续租后再确认，避免确认成功使续租协程误判为丢失租约。
	stopRenew()
	<-done
	if err := flushCtx.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrLeaseLost
	}
	args := []any{token}
	for _, task := range tasks {
		args = append(args, task)
	}
	acked, err := ackScript.Run(flushCtx, s.rc, s.keys, args...).Int64()
	if err != nil {
		return err
	}
	if acked != 1 {
		return ErrLeaseLost
	}
	return nil
}
