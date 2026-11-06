package string_sync

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

type testDb struct {
	mu          sync.Mutex
	rows        map[string]*testValue
	reads       atomic.Int64
	beforeGet   func(context.Context)
	beforeWrite func(context.Context) error
}

func (d *testDb) Get(ctx context.Context, key string) (*testValue, error) {
	d.reads.Add(1)
	d.mu.Lock()
	value := d.rows[key]
	d.mu.Unlock()
	if d.beforeGet != nil {
		d.beforeGet(ctx)
	}
	return value, ctx.Err()
}

func (d *testDb) GetBatch(ctx context.Context, keys []string) ([]*testValue, error) {
	values := make([]*testValue, len(keys))
	for i, key := range keys {
		value, err := d.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func (d *testDb) Write(ctx context.Context, key string, value *testValue) error {
	return d.WriteBatch(ctx, []string{key}, []*testValue{value})
}

func (d *testDb) WriteBatch(ctx context.Context, keys []string, values []*testValue) error {
	if d.beforeWrite != nil {
		if err := d.beforeWrite(ctx); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, key := range keys {
		d.rows[key] = values[i]
	}
	return nil
}

func (d *testDb) Delete(ctx context.Context, key string) error {
	return d.DeleteBatch(ctx, []string{key})
}

func (d *testDb) DeleteBatch(_ context.Context, keys []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, key := range keys {
		delete(d.rows, key)
	}
	return nil
}

func setup(t *testing.T, conf Config) (*miniredis.Miniredis, *redis.Client, *testDb, *StringSync[string, testValue]) {
	t.Helper()
	server := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { rc.Close() })
	db := &testDb{rows: make(map[string]*testValue)}
	if conf.Prefix == "" {
		conf.Prefix = "test"
	}
	return server, rc, db, New[string, testValue](rc, db, conf)
}

func TestWriteDeleteAndExpiration(t *testing.T) {
	server, rc, db, s := setup(t, Config{CacheTTL: time.Second, MissTTL: time.Second})
	ctx := t.Context()
	db.rows["removed"] = &testValue{N: 9}
	if err := s.WriteBatch(ctx, []string{"a", "b"}, []*testValue{{N: 1}, {N: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "removed"); err != nil {
		t.Fatal(err)
	}
	server.FastForward(time.Hour)
	values, err := s.GetBatch(ctx, []string{"a", "b", "removed"})
	if err != nil || values[0].N != 1 || values[1].N != 2 || values[2] != nil {
		t.Fatalf("values=%v err=%v", values, err)
	}
	if db.reads.Load() != 0 || len(db.rows) != 1 {
		t.Fatal("business writes must only touch Redis")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if db.rows["a"].N != 1 || db.rows["b"].N != 2 || db.rows["removed"] != nil {
		t.Fatalf("DB rows=%v", db.rows)
	}
	if count := rc.ZCard(ctx, s.dirtyKey).Val(); count != 0 {
		t.Fatalf("dirty=%d", count)
	}
	server.FastForward(2 * time.Second)
	if value, err := s.Get(ctx, "a"); err != nil || value.N != 1 {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if db.reads.Load() != 1 {
		t.Fatal("clean data should expire and load again")
	}
}

func TestMultiInstanceLoadAndNegativeCache(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "existing"}[exists], func(t *testing.T) {
			server, rc, db, first := setup(t, Config{MissTTL: time.Second})
			second := New[string, testValue](rc, db, Config{Prefix: "test", MissTTL: time.Second})
			if exists {
				db.rows["a"] = &testValue{N: 7}
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
			start := make(chan struct{})
			for i := range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					s := first
					if i%2 == 0 {
						s = second
					}
					value, err := s.Get(t.Context(), "a")
					if err != nil {
						t.Error(err)
						return
					}
					if (value != nil) != exists {
						t.Errorf("value=%v", value)
					}
				}()
			}
			close(start)
			<-entered
			close(release)
			wg.Wait()
			if reads := db.reads.Load(); reads != 1 {
				t.Fatalf("DB reads=%d", reads)
			}
			if !exists {
				server.FastForward(2 * time.Second)
				if _, err := second.Get(t.Context(), "a"); err != nil {
					t.Fatal(err)
				}
				if db.reads.Load() != 2 {
					t.Fatal("negative cache must expire")
				}
			}
		})
	}
}

func TestWriteDuringFlushKeepsDirtyAndSerializesFlushers(t *testing.T) {
	_, rc, db, first := setup(t, Config{})
	second := New[string, testValue](rc, db, Config{Prefix: "test"})
	ctx := t.Context()
	if err := first.Write(ctx, "a", &testValue{N: 1}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	db.beforeWrite = func(ctx context.Context) error {
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
	if err := second.Write(ctx, "a", &testValue{N: 2}); err != nil {
		t.Fatal(err)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("second flusher must not enter DB")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if rc.ZCard(ctx, first.dirtyKey).Val() != 1 {
		t.Fatal("concurrent write lost its dirty marker")
	}
	if value, err := second.Get(ctx, "a"); err != nil || value.N != 2 {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if db.rows["a"].N != 2 || rc.ZCard(ctx, first.dirtyKey).Val() != 0 {
		t.Fatal("new value must be persisted next")
	}
}

func TestOldLoadCannotOverwriteWriteAfterCleanCacheExpires(t *testing.T) {
	server, rc, db, s := setup(t, Config{CacheTTL: time.Second})
	ctx := t.Context()
	db.rows["a"] = &testValue{N: 1}
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
		value, err := s.Get(ctx, "a")
		if err != nil {
			t.Error(err)
		}
		done <- value
	}()
	<-entered
	if err := s.Write(ctx, "a", &testValue{N: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Second)
	close(release)
	if value := <-done; value == nil || value.N != 2 {
		t.Fatalf("old DB load returned %v", value)
	}
	if rc.ZCard(ctx, s.dirtyKey).Val() != 0 {
		t.Fatal("DB load should not mark clean data dirty")
	}
}

func TestFailedFlushAndBatchLimit(t *testing.T) {
	_, rc, db, s := setup(t, Config{BatchSize: 1})
	ctx := t.Context()
	if err := s.WriteBatch(ctx, []string{"a", "b"}, []*testValue{{N: 1}, {N: 2}}); err != nil {
		t.Fatal(err)
	}
	dbErr := errors.New("DB unavailable")
	var calls atomic.Int64
	db.beforeWrite = func(context.Context) error {
		if calls.Add(1) == 1 {
			return dbErr
		}
		return nil
	}
	if err := s.Flush(ctx); !errors.Is(err, dbErr) {
		t.Fatalf("err=%v", err)
	}
	if rc.ZCard(ctx, s.dirtyKey).Val() != 2 {
		t.Fatal("failed flush must retain dirty entries")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(db.rows) != 1 || rc.ZCard(ctx, s.dirtyKey).Val() != 1 {
		t.Fatal("Flush must obey BatchSize")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(db.rows) != 2 || rc.ZCard(ctx, s.dirtyKey).Val() != 0 {
		t.Fatal("remaining data must be retryable")
	}
}

func TestMissingDirtyDataDoesNotFallBackToDB(t *testing.T) {
	_, rc, db, s := setup(t, Config{})
	ctx := t.Context()
	if err := s.Write(ctx, "a", &testValue{N: 2}); err != nil {
		t.Fatal(err)
	}
	dataKey, err := s.dataKey("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Del(ctx, dataKey).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "a"); err == nil || !strings.Contains(err.Error(), "dirty data missing") {
		t.Fatalf("err=%v", err)
	}
	if err := s.Flush(ctx); err == nil || !strings.Contains(err.Error(), "dirty data missing") {
		t.Fatalf("err=%v", err)
	}
	if db.reads.Load() != 0 || rc.ZCard(ctx, s.dirtyKey).Val() != 1 {
		t.Fatal("corrupt dirty data must not be masked")
	}
}

func TestRunFlushesAndStops(t *testing.T) {
	_, _, db, s := setup(t, Config{Interval: time.Millisecond})
	if err := s.Write(t.Context(), "a", &testValue{N: 3}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	written := make(chan struct{}, 1)
	db.beforeWrite = func(context.Context) error { written <- struct{}{}; return nil }
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case <-written:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("periodic flush did not run")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
