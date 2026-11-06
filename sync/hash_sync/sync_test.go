package hash_sync

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type testValue struct{ N int }
type testKey struct{ key, field string }

type testDb struct {
	mu          sync.Mutex
	rows        map[testKey]*testValue
	reads       atomic.Int64
	beforeGet   func(context.Context)
	beforeWrite func(context.Context, string, []string) error
}

func (d *testDb) Get(ctx context.Context, key, field string) (*testValue, error) {
	d.reads.Add(1)
	d.mu.Lock()
	value := d.rows[testKey{key, field}]
	d.mu.Unlock()
	if d.beforeGet != nil {
		d.beforeGet(ctx)
	}
	return value, ctx.Err()
}

func (d *testDb) WriteBatch(ctx context.Context, key string, fields []string, values []*testValue) error {
	if d.beforeWrite != nil {
		if err := d.beforeWrite(ctx, key, fields); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, field := range fields {
		d.rows[testKey{key, field}] = values[i]
	}
	return nil
}

func (d *testDb) DeleteBatch(ctx context.Context, key string, fields []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, field := range fields {
		delete(d.rows, testKey{key, field})
	}
	return nil
}

func setup(t *testing.T, conf Config) (*miniredis.Miniredis, *redis.Client, *testDb, *HashSync[string, string, testValue]) {
	t.Helper()
	server := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { rc.Close() })
	db := &testDb{rows: make(map[testKey]*testValue)}
	if conf.Prefix == "" {
		conf.Prefix = "test"
	}
	return server, rc, db, New[string, string, testValue](rc, db, conf)
}

func TestRoundTripAndDirtyHashDoesNotExpire(t *testing.T) {
	server, rc, db, s := setup(t, Config{CacheTTL: time.Second, MissTTL: time.Second, BatchSize: 1})
	ctx := t.Context()
	db.rows[testKey{"bag", "clean"}] = &testValue{N: 9}
	if _, err := s.Get(ctx, "bag", "clean"); err != nil {
		t.Fatal(err)
	}
	dataKey, _, _, _ := s.location("bag", "a")
	if ttl := rc.PTTL(ctx, dataKey).Val(); ttl <= 0 {
		t.Fatalf("clean hash TTL=%v", ttl)
	}
	if err := s.WriteBatch(ctx, "bag", []string{"a", "b"}, []*testValue{{N: 1}, {N: 2}}); err != nil {
		t.Fatal(err)
	}
	if ttl := rc.PTTL(ctx, dataKey).Val(); ttl != -1 {
		t.Fatalf("dirty hash TTL=%v", ttl)
	}
	server.FastForward(time.Hour)
	values, err := s.GetBatch(ctx, "bag", []string{"a", "b"})
	if err != nil || values[0].N != 1 || values[1].N != 2 {
		t.Fatalf("values=%v err=%v", values, err)
	}
	if db.reads.Load() != 1 || len(db.rows) != 1 {
		t.Fatal("writes should only touch Redis")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(db.rows) != 2 || rc.ZCard(ctx, s.dirtyKey).Val() != 1 {
		t.Fatal("batch limit not respected")
	}
	if ttl := rc.PTTL(ctx, dataKey).Val(); ttl != -1 {
		t.Fatal("another dirty field still pins hash")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if ttl := rc.PTTL(ctx, dataKey).Val(); ttl <= 0 {
		t.Fatal("fully clean hash should have a TTL")
	}
	if err := s.Delete(ctx, "bag", "a"); err != nil {
		t.Fatal(err)
	}
	server.FastForward(time.Hour)
	if value, err := s.Get(ctx, "bag", "a"); err != nil || value != nil {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if db.rows[testKey{"bag", "a"}] != nil {
		t.Fatal("delete was not persisted")
	}
	_, fieldName, _, _ := s.location("bag", "a")
	if rc.HExists(ctx, dataKey, fieldName).Val() {
		t.Fatal("persisted tombstone should be removed")
	}
	if _, err := s.Get(ctx, "bag", "a"); err != nil {
		t.Fatal(err)
	}
	if db.reads.Load() != 1 {
		t.Fatal("persisted delete should be negatively cached")
	}
	server.FastForward(2 * time.Second)
	if value, err := s.Get(ctx, "bag", "b"); err != nil || value.N != 2 {
		t.Fatalf("value=%v err=%v", value, err)
	}
}

func TestMultiInstanceLoadAndNegativeCache(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "existing"}[exists], func(t *testing.T) {
			server, rc, db, first := setup(t, Config{MissTTL: time.Second})
			second := New[string, string, testValue](rc, db, Config{Prefix: "test", MissTTL: time.Second})
			if exists {
				db.rows[testKey{"bag", "a"}] = &testValue{N: 7}
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			db.beforeGet = func(ctx context.Context) {
				once.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			var wg sync.WaitGroup
			for i := range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					s := first
					if i%2 == 0 {
						s = second
					}
					value, err := s.Get(t.Context(), "bag", "a")
					if err != nil {
						t.Error(err)
						return
					}
					if (value != nil) != exists {
						t.Errorf("value=%v", value)
					}
				}()
			}
			<-entered
			close(release)
			wg.Wait()
			if db.reads.Load() != 1 {
				t.Fatalf("reads=%d", db.reads.Load())
			}
			if !exists {
				server.FastForward(2 * time.Second)
				if _, err := second.Get(t.Context(), "bag", "a"); err != nil {
					t.Fatal(err)
				}
				if db.reads.Load() != 2 {
					t.Fatal("negative cache should expire")
				}
			}
		})
	}
}

func TestParallelFlushClaimsDifferentFields(t *testing.T) {
	_, rc, db, first := setup(t, Config{BatchSize: 1})
	second := New[string, string, testValue](rc, db, Config{Prefix: "test", BatchSize: 1})
	ctx := t.Context()
	if err := first.WriteBatch(ctx, "bag", []string{"a", "b"}, []*testValue{{N: 1}, {N: 2}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan string, 2), make(chan struct{})
	db.beforeWrite = func(ctx context.Context, _ string, fields []string) error {
		entered <- fields[0]
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 2)
	go func() { done <- first.Flush(ctx) }()
	go func() { done <- second.Flush(ctx) }()
	seen := make(map[string]bool)
	for range 2 {
		select {
		case field := <-entered:
			if seen[field] {
				t.Fatalf("field %s was claimed twice", field)
			}
			seen[field] = true
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("flushers did not run in parallel")
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if len(db.rows) != 2 || rc.ZCard(ctx, first.workingKey).Val() != 0 {
		t.Fatal("claims were not persisted and acknowledged")
	}
}

func TestWriteWhileFlushingIsQueuedAfterAcknowledgment(t *testing.T) {
	_, rc, db, first := setup(t, Config{})
	second := New[string, string, testValue](rc, db, Config{Prefix: "test"})
	ctx := t.Context()
	if err := first.Write(ctx, "bag", "a", &testValue{N: 1}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	db.beforeWrite = func(ctx context.Context, _ string, _ []string) error {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- first.Flush(ctx) }()
	<-entered
	if err := second.Write(ctx, "bag", "a", &testValue{N: 2}); err != nil {
		t.Fatal(err)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("a working field must not be claimed again")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if rc.ZCard(ctx, first.dirtyKey).Val() != 1 {
		t.Fatal("new write was not requeued")
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if db.rows[testKey{"bag", "a"}].N != 2 {
		t.Fatal("latest value was not persisted")
	}
}

func TestCrashRecoveryAndOldOwnerCannotAcknowledge(t *testing.T) {
	server, rc, db, s := setup(t, Config{LeaseTTL: time.Second})
	ctx := t.Context()
	now := time.Now()
	server.SetTime(now)
	if err := s.Write(ctx, "bag", "a", &testValue{N: 2}); err != nil {
		t.Fatal(err)
	}
	queues := []string{s.dirtyKey, s.workingKey, s.ownersKey}
	tasks, err := claimScript.Run(ctx, rc, queues, "dead-worker", 1000, 1).StringSlice()
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%v err=%v", tasks, err)
	}
	server.SetTime(now.Add(2 * time.Second))
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if db.rows[testKey{"bag", "a"}].N != 2 {
		t.Fatal("expired task was not recovered")
	}
	if err := s.Write(ctx, "bag", "a", &testValue{N: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := claimScript.Run(ctx, rc, queues, "new-worker", 1000, 1).StringSlice(); err != nil {
		t.Fatal(err)
	}
	dataKey, fieldName, task, _ := s.location("bag", "a")
	payload := rc.HGet(ctx, dataKey, fieldName).Val()
	acked, err := ackScript.Run(ctx, rc, append(queues, dataKey, dataKey+":dirty", dataKey+":miss:"+fieldName),
		"dead-worker", 1000, task, fieldName, payload, false, 1000).Int64()
	if err != nil || acked != 0 {
		t.Fatalf("old ack=%d err=%v", acked, err)
	}
	if err := releaseScript.Run(ctx, rc, queues, "dead-worker", task).Err(); err != nil {
		t.Fatal(err)
	}
	if rc.HGet(ctx, s.ownersKey, task).Val() != "new-worker" {
		t.Fatal("old owner changed new ownership")
	}
}

func TestLeaseIsRenewedDuringSlowDBWrite(t *testing.T) {
	_, rc, db, s := setup(t, Config{LeaseTTL: 300 * time.Millisecond})
	second := New[string, string, testValue](rc, db, Config{Prefix: "test", LeaseTTL: 300 * time.Millisecond})
	ctx := t.Context()
	if err := s.Write(ctx, "bag", "a", &testValue{N: 1}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	db.beforeWrite = func(ctx context.Context, _ string, _ []string) error {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- s.Flush(ctx) }()
	<-entered
	<-time.After(450 * time.Millisecond)
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("live lease should prevent takeover")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFailedDBWriteReleasesTaskForRetry(t *testing.T) {
	_, rc, db, s := setup(t, Config{})
	ctx := t.Context()
	if err := s.Write(ctx, "bag", "a", &testValue{N: 4}); err != nil {
		t.Fatal(err)
	}
	dbErr := errors.New("DB unavailable")
	var calls atomic.Int64
	db.beforeWrite = func(context.Context, string, []string) error {
		if calls.Add(1) == 1 {
			return dbErr
		}
		return nil
	}
	if err := s.Flush(ctx); !errors.Is(err, dbErr) {
		t.Fatalf("err=%v", err)
	}
	if rc.ZCard(ctx, s.dirtyKey).Val() != 1 || rc.ZCard(ctx, s.workingKey).Val() != 0 {
		t.Fatal("failed task was not released")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if db.rows[testKey{"bag", "a"}].N != 4 {
		t.Fatal("retry did not persist value")
	}
}

func TestOldLoadCannotOverwriteNewWrite(t *testing.T) {
	server, _, db, s := setup(t, Config{CacheTTL: time.Second})
	ctx := t.Context()
	db.rows[testKey{"bag", "a"}] = &testValue{N: 1}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	db.beforeGet = func(ctx context.Context) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	done := make(chan *testValue, 1)
	go func() {
		value, err := s.Get(ctx, "bag", "a")
		if err != nil {
			t.Error(err)
		}
		done <- value
	}()
	<-entered
	if err := s.Write(ctx, "bag", "a", &testValue{N: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Second)
	close(release)
	if value := <-done; value == nil || value.N != 2 {
		t.Fatalf("old load returned %v", value)
	}
}

func TestMissingDirtyFieldDoesNotFallBackToDB(t *testing.T) {
	_, rc, db, s := setup(t, Config{})
	ctx := t.Context()
	if err := s.Write(ctx, "bag", "a", &testValue{N: 1}); err != nil {
		t.Fatal(err)
	}
	dataKey, fieldName, _, _ := s.location("bag", "a")
	if err := rc.HDel(ctx, dataKey, fieldName).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "bag", "a"); err == nil || !strings.Contains(err.Error(), "dirty field missing") {
		t.Fatalf("err=%v", err)
	}
	if err := s.Flush(ctx); err == nil || !strings.Contains(err.Error(), "dirty field missing") {
		t.Fatalf("err=%v", err)
	}
	if db.reads.Load() != 0 || rc.ZCard(ctx, s.dirtyKey).Val() != 1 {
		t.Fatal("dirty data loss must remain visible")
	}
}
