package dirty_sync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setup(t *testing.T, conf Config, handler Handler[string]) (*miniredis.Miniredis, *redis.Client, *Sync[string]) {
	t.Helper()
	server := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { rc.Close() })
	if conf.Prefix == "" {
		conf.Prefix = "test:{shared}:sync"
	}
	return server, rc, New(rc, conf, handler)
}

func TestMarkDirtyCoalescesAndFlushRespectsBatchSize(t *testing.T) {
	seen := make(map[string]int)
	_, rc, s := setup(t, Config{BatchSize: 2}, func(_ context.Context, keys []string) error {
		if len(keys) > 2 {
			t.Fatalf("batch size=%d", len(keys))
		}
		for _, key := range keys {
			seen[key]++
		}
		return nil
	})
	ctx := t.Context()
	if err := s.MarkDirty(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDirty(ctx, "a", "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDirty(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || rc.ZCard(ctx, s.keys[0]).Val() != 1 {
		t.Fatal("Flush must process one bounded batch")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if seen[key] != 1 {
			t.Fatalf("key=%s count=%d", key, seen[key])
		}
	}
	if rc.Exists(ctx, s.keys...).Val() != 0 {
		t.Fatal("completed tasks left metadata")
	}
}

func TestMarkDirtyTxCommitsAndDiscardsWithBusinessWrite(t *testing.T) {
	var value string
	var rc *redis.Client
	var s *Sync[string]
	_, rc, s = setup(t, Config{}, func(ctx context.Context, keys []string) error {
		if len(keys) != 1 || keys[0] != "a" {
			t.Fatalf("keys=%v", keys)
		}
		var err error
		value, err = rc.Get(ctx, "test:{shared}:data").Result()
		return err
	})
	ctx := t.Context()
	if err := rc.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := rc.TxPipelined(ctx, func(tx redis.Pipeliner) error {
		tx.Set(ctx, "test:{shared}:data", "new-value", 0)
		if err := s.MarkDirtyTx(ctx, tx, "a"); err != nil {
			return err
		}
		if rc.Exists(ctx, s.keys[0]).Val() != 0 {
			t.Fatal("MarkDirtyTx executed before EXEC")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	abortErr := errors.New("abort transaction")
	_, err = rc.TxPipelined(ctx, func(tx redis.Pipeliner) error {
		tx.Set(ctx, "test:{shared}:data", "discarded", 0)
		if err := s.MarkDirtyTx(ctx, tx, "b"); err != nil {
			return err
		}
		return abortErr
	})
	if !errors.Is(err, abortErr) {
		t.Fatalf("err=%v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if value != "new-value" {
		t.Fatalf("handler read %q", value)
	}
}

func TestParallelFlushesClaimDifferentKeys(t *testing.T) {
	entered, release := make(chan string, 2), make(chan struct{})
	handler := func(ctx context.Context, keys []string) error {
		entered <- keys[0]
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, rc, first := setup(t, Config{BatchSize: 1}, handler)
	second := New(rc, first.conf, Handler[string](handler))
	ctx := t.Context()
	if err := first.MarkDirty(ctx, "a", "b"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	go func() { done <- first.Flush(ctx) }()
	go func() { done <- second.Flush(ctx) }()
	seen := make(map[string]bool)
	for range 2 {
		select {
		case key := <-entered:
			if seen[key] {
				t.Fatalf("duplicate claim: %s", key)
			}
			seen[key] = true
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("flushers did not run concurrently")
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if rc.Exists(ctx, first.keys...).Val() != 0 {
		t.Fatal("completed tasks left metadata")
	}
}

func TestMarkDuringHandlerRequeuesWithoutConcurrentProcessing(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	handler := func(ctx context.Context, _ []string) error {
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
	_, rc, first := setup(t, Config{}, handler)
	second := New(rc, first.conf, Handler[string](handler))
	ctx := t.Context()
	if err := first.MarkDirty(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- first.Flush(ctx) }()
	<-entered
	if err := second.MarkDirty(ctx, "a", "a"); err != nil {
		t.Fatal(err)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("working key was claimed concurrently")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if rc.ZCard(ctx, first.keys[0]).Val() != 1 {
		t.Fatal("mark during processing was lost")
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || rc.Exists(ctx, first.keys...).Val() != 0 {
		t.Fatal("redirty task did not finish")
	}
}

func TestFailedHandlerRequeuesWholeBatchAndMergesRedirty(t *testing.T) {
	_, rc, s := setup(t, Config{}, func(context.Context, []string) error { return nil })
	handlerErr := errors.New("handler failed")
	calls := 0
	s.handler = func(ctx context.Context, keys []string) error {
		calls++
		if calls == 1 {
			if err := s.MarkDirty(ctx, keys...); err != nil {
				return err
			}
			return handlerErr
		}
		return nil
	}
	ctx := t.Context()
	if err := s.MarkDirty(ctx, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); !errors.Is(err, handlerErr) {
		t.Fatalf("err=%v", err)
	}
	if rc.ZCard(ctx, s.keys[0]).Val() != 2 || rc.Exists(ctx, s.keys[1:]...).Val() != 0 {
		t.Fatal("failed batch was not released")
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || rc.Exists(ctx, s.keys...).Val() != 0 {
		t.Fatal("retry left extra work")
	}
}

func TestExpiredClaimRecoveryRejectsOldOwner(t *testing.T) {
	called := false
	server, rc, s := setup(t, Config{LeaseTTL: time.Second}, func(_ context.Context, keys []string) error {
		called = true
		if len(keys) != 1 || keys[0] != "a" {
			t.Fatalf("keys=%v", keys)
		}
		return nil
	})
	ctx := t.Context()
	now := time.Now()
	server.SetTime(now)
	if err := s.MarkDirty(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	tasks, err := claimScript.Run(ctx, rc, s.keys, "dead", 1000, 1).StringSlice()
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%v err=%v", tasks, err)
	}
	if err := s.MarkDirty(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	server.SetTime(now.Add(2 * time.Second))
	if _, err := claimScript.Run(ctx, rc, s.keys, "new", 1000, 1).StringSlice(); err != nil {
		t.Fatal(err)
	}
	if rc.SCard(ctx, s.keys[3]).Val() != 0 {
		t.Fatal("recovered task retained old redirty flag")
	}
	if ok, err := ackScript.Run(ctx, rc, s.keys, "dead", tasks[0]).Int64(); err != nil || ok != 0 {
		t.Fatalf("old ack=%d err=%v", ok, err)
	}
	if ok, err := renewScript.Run(ctx, rc, s.keys[1:3], "dead", 1000, tasks[0]).Int64(); err != nil || ok != 0 {
		t.Fatalf("old renew=%d err=%v", ok, err)
	}
	if err := releaseScript.Run(ctx, rc, s.keys, "dead", tasks[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if rc.HGet(ctx, s.keys[2], tasks[0]).Val() != "new" {
		t.Fatal("old owner changed new claim")
	}
	if err := releaseScript.Run(ctx, rc, s.keys, "new", tasks[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if !called || rc.Exists(ctx, s.keys...).Val() != 0 {
		t.Fatal("recovered task did not finish")
	}
}

func TestSlowHandlerRenewsLease(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	handler := func(ctx context.Context, _ []string) error {
		if calls.Add(1) != 1 {
			return errors.New("overlapping handler")
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, rc, first := setup(t, Config{LeaseTTL: 300 * time.Millisecond}, handler)
	second := New(rc, first.conf, Handler[string](handler))
	ctx := t.Context()
	if err := first.MarkDirty(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- first.Flush(ctx) }()
	<-entered
	<-time.After(450 * time.Millisecond)
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("live lease was taken over")
	}
}

func TestCancellationReleasesTask(t *testing.T) {
	entered := make(chan struct{})
	_, rc, s := setup(t, Config{}, func(ctx context.Context, _ []string) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	if err := s.MarkDirty(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Flush(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if rc.ZCard(t.Context(), s.keys[0]).Val() != 1 || rc.Exists(t.Context(), s.keys[1:]...).Val() != 0 {
		t.Fatal("cancelled task was not released")
	}
}

func TestRunRetriesAndStops(t *testing.T) {
	processed := make(chan struct{})
	var calls atomic.Int64
	_, _, s := setup(t, Config{Interval: time.Millisecond}, func(context.Context, []string) error {
		switch calls.Add(1) {
		case 1:
			return errors.New("temporary handler failure")
		case 2:
			close(processed)
		}
		return nil
	})
	if err := s.MarkDirty(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not retry")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestCompositeKeyRoundTrip(t *testing.T) {
	server := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { rc.Close() })
	type key struct {
		ConversationId int64
		MessageId      string
	}
	expected := key{ConversationId: 42, MessageId: "message-1"}
	var received []key
	s := New(rc, Config{Prefix: "messages"}, Handler[key](func(_ context.Context, keys []key) error {
		received = keys
		return nil
	}))
	if err := s.MarkDirty(t.Context(), expected); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(received) != 1 || received[0] != expected {
		t.Fatalf("received=%v", received)
	}
}
