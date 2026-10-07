package redisLock

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestLock(t *testing.T) {
	rc := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})
	err := rc.Ping(context.Background()).Err()
	if err != nil {
		t.Fatal(err)
	}

	v := 0
	wg := sync.WaitGroup{}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, locked, err := LockDefault(rc, context.Background(), "testLock")
			if err != nil {
				t.Error(err)
				return
			}
			if locked {
				defer lease.Unlock(context.Background())
				v++
			}
		}()
	}

	wg.Wait()
	t.Log(v)
}
