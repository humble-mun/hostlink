package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	redisv9 "github.com/redis/go-redis/v9"
)

const (
	portKeyPrefix      = "hostlink:port:"
	portChangesChannel = "hostlink:ports"
	portScanBatchSize  = int64(100)
	maximumPublicPort  = uint32(65535)
)

var (
	errPortRangeExhausted = errors.New("port range exhausted")
	errPortNotFound       = errors.New("port mapping not found")
)

type portMapping struct {
	AgentID     string `json:"agent_id"`
	Target      string `json:"target"`
	ContainerID string `json:"container_id,omitempty"`
}

type portStore interface {
	allocate(context.Context, uint32, uint32, portMapping) (uint32, error)
	release(context.Context, uint32) error
	lookup(context.Context, uint32) (portMapping, error)
	desired(context.Context) (map[uint32]portMapping, error)
	releaseByAgent(context.Context, string) ([]uint32, error)
	watch() (<-chan struct{}, func())
	close() error
}

type portRange struct{ from, to uint32 }

func parsePortRange(s string) (portRange, error) {
	parts := strings.Split(s, "-")
	if len(parts) > 2 {
		return portRange{}, fmt.Errorf("invalid port range %q", s)
	}
	from, err := parsePort(parts[0])
	if err != nil {
		return portRange{}, fmt.Errorf("parse port range %q: %w", s, err)
	}
	to := from
	if len(parts) == 2 {
		if to, err = parsePort(parts[1]); err != nil {
			return portRange{}, fmt.Errorf("parse port range %q: %w", s, err)
		}
	}
	if err := validatePortRange(from, to); err != nil {
		return portRange{}, fmt.Errorf("parse port range %q: %w", s, err)
	}
	return portRange{from: from, to: to}, nil
}

func parsePort(s string) (uint32, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("parse port: %w", err)
	}
	return uint32(port), nil
}

func validatePortRange(from, to uint32) error {
	if from == 0 || to > maximumPublicPort || from > to {
		return fmt.Errorf("invalid port range %d-%d", from, to)
	}
	return nil
}

func portKey(port uint32) string {
	return portKeyPrefix + strconv.FormatUint(uint64(port), 10)
}

func portFromKey(key string) (uint32, error) {
	if !strings.HasPrefix(key, portKeyPrefix) {
		return 0, fmt.Errorf("invalid port key %q", key)
	}
	port, err := strconv.ParseUint(strings.TrimPrefix(key, portKeyPrefix), 10, 16)
	if err != nil {
		return 0, fmt.Errorf("parse port key %q: %w", key, err)
	}
	if port == 0 {
		return 0, fmt.Errorf("invalid port key %q", key)
	}
	return uint32(port), nil
}

type portWatchers struct {
	mu     sync.Mutex
	next   uint64
	closed bool
	subs   map[uint64]chan struct{}
}

func (w *portWatchers) watch() (<-chan struct{}, func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := make(chan struct{}, 1)
	if w.closed {
		return ch, func() {}
	}
	if w.subs == nil {
		w.subs = make(map[uint64]chan struct{})
	}
	id := w.next
	w.next++
	w.subs[id] = ch
	var once sync.Once
	return ch, func() { once.Do(func() { w.remove(id) }) }
}

func (w *portWatchers) remove(id uint64) {
	w.mu.Lock()
	delete(w.subs, id)
	w.mu.Unlock()
}

func (w *portWatchers) notify() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ch := range w.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (w *portWatchers) close() {
	w.mu.Lock()
	w.closed = true
	w.subs = nil
	w.mu.Unlock()
}

type memPortStore struct {
	mu       sync.Mutex
	mappings map[uint32]portMapping
	watchers portWatchers
}

func newMemPortStore() *memPortStore {
	return &memPortStore{mappings: make(map[uint32]portMapping)}
}

func newPortStore(logger logr.Logger, redis redisv9.UniversalClient) portStore {
	if redis == nil {
		return newMemPortStore()
	}
	return newRedisPortStore(logger, redis)
}

func (s *memPortStore) allocate(_ context.Context, from, to uint32, mapping portMapping) (uint32, error) {
	if err := validatePortRange(from, to); err != nil {
		return 0, err
	}
	s.mu.Lock()
	for port := from; ; port++ {
		if _, found := s.mappings[port]; !found {
			s.mappings[port] = mapping
			s.mu.Unlock()
			s.watchers.notify()
			return port, nil
		}
		if port == to {
			s.mu.Unlock()
			return 0, errPortRangeExhausted
		}
	}
}

func (s *memPortStore) release(_ context.Context, port uint32) error {
	s.mu.Lock()
	_, found := s.mappings[port]
	delete(s.mappings, port)
	s.mu.Unlock()
	if found {
		s.watchers.notify()
	}
	return nil
}

func (s *memPortStore) lookup(_ context.Context, port uint32) (portMapping, error) {
	s.mu.Lock()
	mapping, found := s.mappings[port]
	s.mu.Unlock()
	if !found {
		return portMapping{}, errPortNotFound
	}
	return mapping, nil
}

func (s *memPortStore) desired(_ context.Context) (map[uint32]portMapping, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mappings := make(map[uint32]portMapping, len(s.mappings))
	maps.Copy(mappings, s.mappings)
	return mappings, nil
}

func (s *memPortStore) releaseByAgent(_ context.Context, agentID string) ([]uint32, error) {
	s.mu.Lock()
	ports := make([]uint32, 0)
	for port, mapping := range s.mappings {
		if mapping.AgentID == agentID {
			delete(s.mappings, port)
			ports = append(ports, port)
		}
	}
	s.mu.Unlock()
	if len(ports) != 0 {
		s.watchers.notify()
	}
	return ports, nil
}

func (s *memPortStore) watch() (<-chan struct{}, func()) { return s.watchers.watch() }
func (s *memPortStore) close() error                     { s.watchers.close(); return nil }

// allow: SIZE_OK — required in one file by the public port-store contract.
type redisPortStore struct {
	logger       logr.Logger
	redis        redisv9.UniversalClient
	watchers     portWatchers
	watchCtx     context.Context
	stopWatch    context.CancelFunc
	closeOnce    sync.Once
	closeErr     error
	subscription *redisv9.PubSub
	subMu        sync.Mutex
	done         chan struct{}
}

func newRedisPortStore(logger logr.Logger, redis redisv9.UniversalClient) *redisPortStore {
	ctx, cancel := context.WithCancel(context.Background())
	s := &redisPortStore{logger: logger, redis: redis, watchCtx: ctx, stopWatch: cancel, done: make(chan struct{})}
	go s.runSubscription()
	return s
}

func (s *redisPortStore) allocate(ctx context.Context, from, to uint32, mapping portMapping) (uint32, error) {
	if err := validatePortRange(from, to); err != nil {
		return 0, err
	}
	encodedMapping, err := json.Marshal(mapping)
	if err != nil {
		return 0, fmt.Errorf("marshal port mapping: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()
	for start := from; start <= to; {
		end := min(start+uint32(portScanBatchSize)-1, to)
		keys := make([]string, 0, end-start+1)
		for port := start; port <= end; port++ {
			keys = append(keys, portKey(port))
		}
		values, err := s.redis.MGet(ctx, keys...).Result()
		if err != nil {
			return 0, fmt.Errorf("find free ports in redis: %w", err)
		}
		for index, candidate := range values {
			if candidate != nil {
				continue
			}
			port := start + uint32(index)
			claimed, err := s.redis.SetNX(ctx, portKey(port), encodedMapping, 0).Result()
			if err != nil {
				return 0, fmt.Errorf("claim port %d in redis: %w", port, err)
			}
			if claimed {
				s.changed()
				return port, nil
			}
		}
		if end == to {
			break
		}
		start = end + 1
	}
	return 0, errPortRangeExhausted
}

func (s *redisPortStore) release(ctx context.Context, port uint32) error {
	ctx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()
	deleted, err := s.redis.Del(ctx, portKey(port)).Result()
	if err != nil {
		return fmt.Errorf("release port %d from redis: %w", port, err)
	}
	if deleted != 0 {
		s.changed()
	}
	return nil
}

func (s *redisPortStore) lookup(ctx context.Context, port uint32) (portMapping, error) {
	ctx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()
	value, err := s.redis.Get(ctx, portKey(port)).Result()
	if errors.Is(err, redisv9.Nil) {
		return portMapping{}, errPortNotFound
	}
	if err != nil {
		return portMapping{}, fmt.Errorf("lookup port %d in redis: %w", port, err)
	}
	return decodePortMapping(value)
}

func (s *redisPortStore) desired(ctx context.Context) (map[uint32]portMapping, error) {
	ctx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()
	keys, err := s.keys(ctx)
	if err != nil {
		return nil, err
	}
	mappings := make(map[uint32]portMapping, len(keys))
	for start := 0; start < len(keys); start += int(portScanBatchSize) {
		end := min(start+int(portScanBatchSize), len(keys))
		values, err := s.redis.MGet(ctx, keys[start:end]...).Result()
		if err != nil {
			return nil, fmt.Errorf("get port mappings from redis: %w", err)
		}
		for index, value := range values {
			if value == nil {
				continue
			}
			raw, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("port mapping %q has unexpected type %T", keys[start+index], value)
			}
			port, err := portFromKey(keys[start+index])
			if err != nil {
				return nil, err
			}
			mapping, err := decodePortMapping(raw)
			if err != nil {
				return nil, fmt.Errorf("decode port %d mapping: %w", port, err)
			}
			mappings[port] = mapping
		}
	}
	return mappings, nil
}

func (s *redisPortStore) releaseByAgent(ctx context.Context, agentID string) ([]uint32, error) {
	ctx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()
	keys, err := s.keys(ctx)
	if err != nil {
		return nil, err
	}
	ports := make([]uint32, 0)
	for _, key := range keys {
		value, err := s.redis.Get(ctx, key).Result()
		if errors.Is(err, redisv9.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get port mapping %q from redis: %w", key, err)
		}
		mapping, err := decodePortMapping(value)
		if err != nil {
			return nil, fmt.Errorf("decode port mapping %q: %w", key, err)
		}
		if mapping.AgentID != agentID {
			continue
		}
		port, err := portFromKey(key)
		if err != nil {
			return nil, err
		}
		deleted, err := s.redis.Del(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("release port %d from redis: %w", port, err)
		}
		if deleted != 0 {
			ports = append(ports, port)
			s.changed()
		}
	}
	return ports, nil
}

func (s *redisPortStore) keys(ctx context.Context) ([]string, error) {
	keys := make([]string, 0)
	var cursor uint64
	for {
		batch, next, err := s.redis.Scan(ctx, cursor, portKeyPrefix+"*", portScanBatchSize).Result()
		if err != nil {
			return nil, fmt.Errorf("scan port mappings in redis: %w", err)
		}
		keys = append(keys, batch...)
		if next == 0 {
			return keys, nil
		}
		cursor = next
	}
}

func decodePortMapping(value string) (portMapping, error) {
	var mapping portMapping
	if err := json.Unmarshal([]byte(value), &mapping); err != nil {
		return portMapping{}, fmt.Errorf("unmarshal port mapping: %w", err)
	}
	return mapping, nil
}

func (s *redisPortStore) watch() (<-chan struct{}, func()) { return s.watchers.watch() }

func (s *redisPortStore) changed() {
	s.watchers.notify()
	ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
	defer cancel()
	if err := s.redis.Publish(ctx, portChangesChannel, "1").Err(); err != nil {
		s.logger.Error(err, "publish port registry change failed")
	}
}

func (s *redisPortStore) runSubscription() {
	defer close(s.done)
	for s.watchCtx.Err() == nil {
		if err := s.listen(); err != nil && s.watchCtx.Err() == nil {
			s.logger.Error(err, "port registry subscription failed")
		}
		if s.watchCtx.Err() != nil {
			return
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-s.watchCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *redisPortStore) listen() error {
	subscription := s.redis.Subscribe(s.watchCtx, portChangesChannel)
	if !s.setSubscription(subscription) {
		return subscription.Close()
	}
	defer func() {
		s.clearSubscription(subscription)
		if err := subscription.Close(); err != nil && s.watchCtx.Err() == nil {
			s.logger.Error(err, "close port registry subscription failed")
		}
	}()
	for {
		if _, err := subscription.ReceiveMessage(s.watchCtx); err != nil {
			return err
		}
		s.watchers.notify()
	}
}

func (s *redisPortStore) setSubscription(subscription *redisv9.PubSub) bool {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.watchCtx.Err() != nil {
		return false
	}
	s.subscription = subscription
	return true
}

func (s *redisPortStore) clearSubscription(subscription *redisv9.PubSub) {
	s.subMu.Lock()
	if s.subscription == subscription {
		s.subscription = nil
	}
	s.subMu.Unlock()
}

func (s *redisPortStore) close() error {
	s.closeOnce.Do(func() {
		s.stopWatch()
		s.watchers.close()
		s.subMu.Lock()
		subscription := s.subscription
		s.subMu.Unlock()
		if subscription != nil {
			s.closeErr = subscription.Close()
		}
		<-s.done
	})
	return s.closeErr
}
