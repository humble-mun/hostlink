package controller

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	redisv9 "github.com/redis/go-redis/v9"
)

func TestBindingTrackerStatesSingleReplica(t *testing.T) {
	// Given
	tracker := newBindingTracker(logr.Discard(), nil, "", func() []uint32 { return []uint32{1025} })
	mappings := map[uint32]portMapping{
		1025: {},
		1026: {},
		1027: {Suspended: true},
	}

	// When
	states, err := tracker.states(context.Background(), mappings)

	// Then
	if err != nil {
		t.Fatalf("states: %v", err)
	}
	want := map[uint32]portState{
		1025: portStateActive,
		1026: portStatePending,
		1027: portStateSuspended,
	}
	if !maps.Equal(states, want) {
		t.Fatalf("states = %#v, want %#v", states, want)
	}
}

func TestBindingTrackerStatesMultiPod(t *testing.T) {
	// Given
	redis := newBindingsFakeRedis(
		[]string{controllerKeyPrefix + "podA", controllerKeyPrefix + "podB"},
		map[string]any{
			boundKey(1025, "podA"): "1",
			boundKey(1025, "podB"): "1",
			boundKey(1026, "podA"): "1",
		},
	)
	tracker := newBindingTracker(logr.Discard(), redis, "podA", nil)
	mappings := map[uint32]portMapping{
		1025: {},
		1026: {},
		1027: {Suspended: true},
	}

	// When
	states, err := tracker.states(context.Background(), mappings)

	// Then
	if err != nil {
		t.Fatalf("states: %v", err)
	}
	want := map[uint32]portState{
		1025: portStateActive,
		1026: portStatePending,
		1027: portStateSuspended,
	}
	if !maps.Equal(states, want) {
		t.Fatalf("states = %#v, want %#v", states, want)
	}
}

func TestBindingTrackerStatesNoLivePods(t *testing.T) {
	// Given
	tracker := newBindingTracker(logr.Discard(), newBindingsFakeRedis(nil, nil), "podA", nil)
	mappings := map[uint32]portMapping{
		1025: {},
		1026: {Suspended: true},
	}

	// When
	states, err := tracker.states(context.Background(), mappings)

	// Then
	if err != nil {
		t.Fatalf("states: %v", err)
	}
	want := map[uint32]portState{
		1025: portStatePending,
		1026: portStateSuspended,
	}
	if !maps.Equal(states, want) {
		t.Fatalf("states = %#v, want %#v", states, want)
	}
}

func TestBindingTrackerReportBound(t *testing.T) {
	// Given
	redis := newBindingsFakeRedis(nil, nil)
	tracker := newBindingTracker(logr.Discard(), redis, "podA", func() []uint32 { return []uint32{1025, 1026} })

	// When
	tracker.reportBound(context.Background())

	// Then
	want := map[string]time.Duration{
		controllerKeyPrefix + "podA": controllerTTL,
		boundKey(1025, "podA"):       boundTTL,
		boundKey(1026, "podA"):       boundTTL,
	}
	if !maps.Equal(redis.sets(), want) {
		t.Fatalf("reported sets = %#v, want %#v", redis.sets(), want)
	}

	// Given
	localCalls := 0
	nilTracker := newBindingTracker(logr.Discard(), nil, "podA", func() []uint32 {
		localCalls++
		return nil
	})

	// When
	nilTracker.reportBound(context.Background())

	// Then
	if localCalls != 0 {
		t.Fatalf("local calls = %d, want 0", localCalls)
	}
}

type bindingsFakeRedis struct {
	redisv9.UniversalClient

	mu       sync.Mutex
	scanKeys []string
	values   map[string]any
	reported map[string]time.Duration
}

func newBindingsFakeRedis(scanKeys []string, values map[string]any) *bindingsFakeRedis {
	return &bindingsFakeRedis{
		scanKeys: scanKeys,
		values:   values,
		reported: make(map[string]time.Duration),
	}
}

func (r *bindingsFakeRedis) Scan(ctx context.Context, _ uint64, _ string, _ int64) *redisv9.ScanCmd {
	r.mu.Lock()
	keys := append([]string(nil), r.scanKeys...)
	r.mu.Unlock()
	command := redisv9.NewScanCmd(ctx, nil)
	command.SetVal(keys, 0)
	return command
}

func (r *bindingsFakeRedis) MGet(ctx context.Context, keys ...string) *redisv9.SliceCmd {
	r.mu.Lock()
	values := make([]any, len(keys))
	for index, key := range keys {
		values[index] = r.values[key]
	}
	r.mu.Unlock()
	command := redisv9.NewSliceCmd(ctx)
	command.SetVal(values)
	return command
}

func (r *bindingsFakeRedis) Pipelined(ctx context.Context, fn func(redisv9.Pipeliner) error) ([]redisv9.Cmder, error) {
	if err := fn(&bindingsFakePipeline{redis: r}); err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *bindingsFakeRedis) sets() map[string]time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	sets := make(map[string]time.Duration, len(r.reported))
	maps.Copy(sets, r.reported)
	return sets
}

type bindingsFakePipeline struct {
	redisv9.Pipeliner
	redis *bindingsFakeRedis
}

func (p *bindingsFakePipeline) Set(ctx context.Context, key string, _ any, expiration time.Duration) *redisv9.StatusCmd {
	p.redis.mu.Lock()
	p.redis.reported[key] = expiration
	p.redis.mu.Unlock()
	return redisv9.NewStatusCmd(ctx)
}
