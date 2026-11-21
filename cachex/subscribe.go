package cachex

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

func (c *Cache[K, V]) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("cachex: subscription already running")
	}
	c.running = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	for ctx.Err() == nil {
		c.listen(ctx)
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func (c *Cache[K, V]) listen(ctx context.Context) {
	sub := c.redis.Subscribe(ctx, c.channel)
	stop := context.AfterFunc(ctx, func() { _ = sub.Close() })
	defer func() {
		c.resetLocal(false)
		stop()
		_ = sub.Close()
	}()

	probing := false
	for ctx.Err() == nil {
		message, err := sub.ReceiveTimeout(ctx, 2*time.Second)
		if err != nil {
			var timeout net.Error
			if !probing && errors.As(err, &timeout) && timeout.Timeout() && sub.Ping(ctx) == nil {
				// 空闲订阅探活，下一次仍无响应就清理 L1 并重连。
				probing = true
				continue
			}
			return
		}
		probing = false
		switch message := message.(type) {
		case *redis.Subscription:
			if message.Kind == "subscribe" && message.Channel == c.channel {
				// 新订阅从确认开始生效，之前的失效通知可能已经丢失。
				c.resetLocal(true)
			}
		case *redis.Message:
			var key K
			if err := json.Unmarshal([]byte(message.Payload), &key); err != nil {
				c.resetLocal(true)
				continue
			}
			c.invalidateLocal(key)
		}
	}
}

func (c *Cache[K, V]) resetLocal(listening bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listening = listening
	c.local.Purge()
	for _, pending := range c.flights {
		close(pending)
	}
	clear(c.flights)
}
