package bit_map_sync

import (
	"bytes"
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

type testDb struct {
	mu          sync.Mutex
	rows        map[string][]byte
	reads       atomic.Int64
	beforeGet   func(context.Context, string) error
	beforeWrite func(context.Context, []string) error
}

func (d *testDb) Get(ctx context.Context, key string) ([]byte, error) {
	d.reads.Add(1)
	d.mu.Lock()
	value := bytes.Clone(d.rows[key])
	d.mu.Unlock()
	if d.beforeGet != nil {
		if err := d.beforeGet(ctx, key); err != nil {
			return nil, err
		}
	}
	return value, ctx.Err()
}

func (d *testDb) WriteBatch(ctx context.Context, keys []string, values [][]byte) error {
	if d.beforeWrite != nil {
		if err := d.beforeWrite(ctx, keys); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, key := range keys {
		d.rows[key] = bytes.Clone(values[i])
	}
	return nil
}

func (d *testDb) DeleteBatch(ctx context.Context, keys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, key := range keys {
		delete(d.rows, key)
	}
	return nil
}

func setup(t *testing.T, conf Config) (*miniredis.Miniredis, *redis.Client, *testDb, *BitmapSync[string]) {
	t.Helper()
	server := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { rc.Close() })
	db := &testDb{rows: make(map[string][]byte)}
	if conf.Prefix == "" {
		conf.Prefix = "test"
	}
	return server, rc, db, New[string](rc, db, conf)
}

func TestColdWritePreservesBitsAndDirtyBitmapDoesNotExpire(t *testing.T) {
	server, rc, db, s := setup(t, Config{CacheTTL: time.Second})
	ctx := t.Context()
	db.rows["a"] = []byte{0x80, 0xff, 0x00}
	if err := s.SetBit(ctx, "a", 9, false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetBit(ctx, "a", 20, true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(db.rows["a"], []byte{0x80, 0xff, 0x00}) {
		t.Fatal("business writes must only touch Redis")
	}
	server.FastForward(time.Hour)
	for offset, expected := range map[int64]bool{0: true, 8: true, 9: false, 15: true, 20: true, 100: false} {
		value, err := s.GetBit(ctx, "a", offset)
		if err != nil || value != expected {
			t.Fatalf("offset=%d value=%v err=%v", offset, value, err)
		}
	}
	dataKey, _, _ := s.location("a")
	if ttl := rc.PTTL(ctx, dataKey).Val(); ttl != -1 {
		t.Fatalf("dirty TTL=%v", ttl)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(db.rows["a"], []byte{0x80, 0xbf, 0x08}) {
		t.Fatalf("DB bytes=%x", db.rows["a"])
	}
	if ttl := rc.PTTL(ctx, dataKey).Val(); ttl <= 0 {
		t.Fatalf("clean TTL=%v", ttl)
	}
	server.FastForward(2 * time.Second)
	if value, err := s.GetBit(ctx, "a", 20); err != nil || !value {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if db.reads.Load() != 2 {
		t.Fatalf("reads=%d", db.reads.Load())
	}
}

func TestZeroAndMissingBitmapsMergeLoadsAcrossInstances(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{name: "missing"}, {name: "empty", value: []byte{}}, {name: "zero", value: []byte{0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, rc, db, first := setup(t, Config{MissTTL: time.Second, CacheTTL: 10 * time.Second})
			second := New[string](rc, db, Config{Prefix: "test", MissTTL: time.Second, CacheTTL: 10 * time.Second})
			if tc.value != nil {
				db.rows["a"] = tc.value
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			db.beforeGet = func(ctx context.Context, _ string) error {
				once.Do(func() { close(entered) })
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
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
					if value, err := s.GetBit(t.Context(), "a", int64(i)); err != nil || value {
						t.Errorf("value=%v err=%v", value, err)
					}
				}()
			}
			<-entered
			close(release)
			wg.Wait()
			if db.reads.Load() != 1 {
				t.Fatalf("reads=%d", db.reads.Load())
			}
			server.FastForward(2 * time.Second)
			if _, err := second.GetBit(t.Context(), "a", 0); err != nil {
				t.Fatal(err)
			}
			expected := int64(1)
			if tc.value == nil {
				expected = 2
			}
			if db.reads.Load() != expected {
				t.Fatalf("reads=%d expected=%d", db.reads.Load(), expected)
			}
		})
	}
}

func TestConcurrentSetBitsDoesNotLoseUpdates(t *testing.T) {
	_, rc, db, first := setup(t, Config{})
	second := New[string](rc, db, Config{Prefix: "test"})
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := first
			if i%2 == 0 {
				s = second
			}
			if err := s.SetBit(t.Context(), "a", int64(i), true); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := first.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(db.rows["a"], bytes.Repeat([]byte{0xff}, 8)) {
		t.Fatalf("bitmap=%x", db.rows["a"])
	}
	if db.reads.Load() != 1 {
		t.Fatalf("reads=%d", db.reads.Load())
	}
}

func TestParallelFlushClaimsDifferentKeys(t *testing.T) {
	_, rc, db, first := setup(t, Config{BatchSize: 1})
	second := New[string](rc, db, Config{Prefix: "test", BatchSize: 1})
	ctx := t.Context()
	for _, key := range []string{"a", "b"} {
		if err := first.SetBit(ctx, key, 0, true); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan string, 2), make(chan struct{})
	db.beforeWrite = func(ctx context.Context, keys []string) error {
		entered <- keys[0]
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
		case key := <-entered:
			if seen[key] {
				t.Fatalf("key %s was claimed twice", key)
			}
			seen[key] = true
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
		t.Fatal("claimed bitmaps were not acknowledged")
	}
}

func TestChangesDuringFlushAreRequeued(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "set", true: "delete"}[remove], func(t *testing.T) {
			_, rc, db, first := setup(t, Config{})
			second := New[string](rc, db, Config{Prefix: "test"})
			ctx := t.Context()
			if err := first.SetBit(ctx, "a", 0, true); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			db.beforeWrite = func(ctx context.Context, _ []string) error {
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
			if remove {
				if err := second.Delete(ctx, "a"); err != nil {
					t.Fatal(err)
				}
			} else if err := second.SetBit(ctx, "a", 1, true); err != nil {
				t.Fatal(err)
			}
			if err := second.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("working key was claimed again")
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if rc.ZCard(ctx, first.dirtyKey).Val() != 1 {
				t.Fatal("concurrent change lost dirty marker")
			}
			if err := second.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if remove {
				if _, exists := db.rows["a"]; exists {
					t.Fatal("delete was not persisted")
				}
			} else if !bytes.Equal(db.rows["a"], []byte{0xc0}) {
				t.Fatalf("bitmap=%x", db.rows["a"])
			}
		})
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	server, rc, db, s := setup(t, Config{MissTTL: time.Second})
	ctx := t.Context()
	db.rows["a"] = []byte{0xff}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	server.FastForward(time.Hour)
	if value, err := s.GetBit(ctx, "a", 0); err != nil || value {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if err := s.SetBit(ctx, "a", 8, true); err != nil {
		t.Fatal(err)
	}
	if db.reads.Load() != 0 {
		t.Fatal("recreating a deleted bitmap should start empty")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(db.rows["a"], []byte{0, 0x80}) {
		t.Fatalf("bitmap=%x", db.rows["a"])
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	dataKey, _, _ := s.location("a")
	if rc.Exists(ctx, dataKey+":deleted").Val() != 0 {
		t.Fatal("persisted deletion left a permanent marker")
	}
	if value, err := s.GetBit(ctx, "a", 8); err != nil || value {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if _, exists := db.rows["a"]; exists {
		t.Fatal("bitmap was not deleted from DB")
	}
	if db.reads.Load() != 0 {
		t.Fatal("persisted deletion should be negatively cached")
	}
	server.FastForward(2 * time.Second)
	if _, err := s.GetBit(ctx, "a", 0); err != nil {
		t.Fatal(err)
	}
	if db.reads.Load() != 1 {
		t.Fatal("negative cache did not expire")
	}
}

func TestExpiredClaimIsRecoveredAndOldOwnerCannotAcknowledge(t *testing.T) {
	server, rc, db, s := setup(t, Config{LeaseTTL: time.Second})
	ctx := t.Context()
	now := time.Now()
	server.SetTime(now)
	if err := s.SetBit(ctx, "a", 0, true); err != nil {
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
	if !bytes.Equal(db.rows["a"], []byte{0x80}) {
		t.Fatal("expired task was not recovered")
	}
	if err := s.SetBit(ctx, "a", 1, true); err != nil {
		t.Fatal(err)
	}
	if _, err := claimScript.Run(ctx, rc, queues, "new-worker", 1000, 1).StringSlice(); err != nil {
		t.Fatal(err)
	}
	dataKey, task, _ := s.location("a")
	acked, err := ackScript.Run(ctx, rc, append(queues, dataKey, dataKey+":deleted"),
		"dead-worker", 1000, task, string([]byte{0xc0}), 0, 1000).Int64()
	if err != nil || acked != 0 {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
	if err := releaseScript.Run(ctx, rc, queues, "dead-worker", task).Err(); err != nil {
		t.Fatal(err)
	}
	if rc.HGet(ctx, s.ownersKey, task).Val() != "new-worker" {
		t.Fatal("old worker changed new ownership")
	}
}

func TestFailedDBWriteReleasesTask(t *testing.T) {
	_, rc, db, s := setup(t, Config{})
	ctx := t.Context()
	if err := s.SetBit(ctx, "a", 0, true); err != nil {
		t.Fatal(err)
	}
	dbErr := errors.New("DB unavailable")
	var calls atomic.Int64
	db.beforeWrite = func(context.Context, []string) error {
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
	if !bytes.Equal(db.rows["a"], []byte{0x80}) {
		t.Fatal("retry failed")
	}
}

func TestOldLoadCannotRestoreDeletedBitmap(t *testing.T) {
	server, _, db, s := setup(t, Config{MissTTL: time.Second})
	ctx := t.Context()
	db.rows["a"] = []byte{0xff}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	db.beforeGet = func(ctx context.Context, _ string) error {
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
	done := make(chan bool, 1)
	go func() {
		value, err := s.GetBit(ctx, "a", 0)
		if err != nil {
			t.Error(err)
		}
		done <- value
	}()
	<-entered
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Second)
	close(release)
	if value := <-done; value {
		t.Fatal("old DB query restored a deleted bitmap")
	}
}

func TestDBReadErrorIsNotCached(t *testing.T) {
	_, _, db, s := setup(t, Config{})
	dbErr := errors.New("DB read failed")
	db.rows["a"] = []byte{0x80}
	var calls atomic.Int64
	db.beforeGet = func(context.Context, string) error {
		if calls.Add(1) == 1 {
			return dbErr
		}
		return nil
	}
	if _, err := s.GetBit(t.Context(), "a", 0); !errors.Is(err, dbErr) {
		t.Fatalf("err=%v", err)
	}
	if value, err := s.GetBit(t.Context(), "a", 0); err != nil || !value {
		t.Fatalf("value=%v err=%v", value, err)
	}
}

func TestMissingDirtyBitmapAndInvalidOffsets(t *testing.T) {
	_, rc, db, s := setup(t, Config{})
	ctx := t.Context()
	for _, offset := range []int64{-1, 1 << 32} {
		if _, err := s.GetBit(ctx, "a", offset); err == nil {
			t.Fatal("invalid read offset accepted")
		}
		if err := s.SetBit(ctx, "a", offset, true); err == nil {
			t.Fatal("invalid write offset accepted")
		}
	}
	if db.reads.Load() != 0 {
		t.Fatal("invalid input must not load DB")
	}
	if err := s.SetBit(ctx, "a", 0, true); err != nil {
		t.Fatal(err)
	}
	dataKey, _, _ := s.location("a")
	if err := rc.Del(ctx, dataKey).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetBit(ctx, "a", 0); err == nil || !strings.Contains(err.Error(), "dirty bitmap missing") {
		t.Fatalf("err=%v", err)
	}
	if err := s.SetBit(ctx, "a", 0, false); err == nil || !strings.Contains(err.Error(), "dirty bitmap missing") {
		t.Fatalf("err=%v", err)
	}
	if err := s.Flush(ctx); err == nil || !strings.Contains(err.Error(), "dirty bitmap missing") {
		t.Fatalf("err=%v", err)
	}
	if db.reads.Load() != 1 || rc.ZCard(ctx, s.dirtyKey).Val() != 1 {
		t.Fatal("dirty data loss was masked")
	}
}
