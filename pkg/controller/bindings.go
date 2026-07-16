package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	redisv9 "github.com/redis/go-redis/v9"
)

const (
	// boundKeyPrefix marks one controller pod as listening on one public port:
	// hostlink:bound:<port>:<podAddr>. Refreshed by the port reconciler loop and
	// left to expire when the pod stops listening or dies.
	boundKeyPrefix = "hostlink:bound:"
	// controllerKeyPrefix marks one controller pod as alive:
	// hostlink:controller:<podAddr>. The set of live pods is the denominator for
	// the activation barrier: a port is active only when every live pod has it bound.
	controllerKeyPrefix = "hostlink:controller:"

	boundTTL      = 30 * time.Second
	controllerTTL = 45 * time.Second
)

// portState is the externally visible readiness of a public forward port.
type portState string

const (
	// portStatePending: allocated, but not every live controller pod has bound the
	// port yet. Routing the port through a Service now could hit a pod that RSTs.
	portStatePending portState = "pending"
	// portStateActive: every live controller pod is listening; the port is safe to
	// publish through a Service selecting all controller pods.
	portStateActive portState = "active"
	// portStateSuspended: the target container stopped; connections are refused
	// until it starts again.
	portStateSuspended portState = "suspended"
)

func boundKey(port uint32, podAddr string) string {
	return fmt.Sprintf("%s%d:%s", boundKeyPrefix, port, podAddr)
}

// bindingTracker implements the activation barrier bookkeeping: each pod reports
// which public ports it has bound, and states() folds those reports into a
// per-port pending/active/suspended verdict.
type bindingTracker struct {
	logger   logr.Logger
	redis    redisv9.UniversalClient
	selfAddr string
	local    func() []uint32
}

// newBindingTracker builds a tracker. redis may be nil (single-replica mode), in
// which case the local bound-port snapshot alone decides activation. local
// reports the ports this pod currently has bound (listenerManager.boundPorts).
func newBindingTracker(logger logr.Logger, redis redisv9.UniversalClient, selfAddr string, local func() []uint32) *bindingTracker {
	return &bindingTracker{logger: logger, redis: redis, selfAddr: selfAddr, local: local}
}

// reportBound refreshes this pod's liveness key and one bound key per locally
// bound port. Best effort: failures are logged and retried on the next
// reconciler round, well within the key TTLs.
func (t *bindingTracker) reportBound(ctx context.Context) {
	if t == nil || t.redis == nil {
		return
	}
	ports := t.local()
	opCtx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	defer cancel()
	_, err := t.redis.Pipelined(opCtx, func(pipe redisv9.Pipeliner) error {
		pipe.Set(opCtx, controllerKeyPrefix+t.selfAddr, "1", controllerTTL)
		for _, port := range ports {
			pipe.Set(opCtx, boundKey(port, t.selfAddr), "1", boundTTL)
		}
		return nil
	})
	if err != nil {
		t.logger.Error(err, "report bound ports to redis failed")
	}
}

// states classifies every mapping: suspended wins outright; otherwise the port
// is active when every live controller pod has reported it bound, else pending.
// With no redis the local bound set decides. With redis but no live controller
// keys yet, everything stays pending (conservative: never publish early).
func (t *bindingTracker) states(ctx context.Context, mappings map[uint32]portMapping) (map[uint32]portState, error) {
	result := make(map[uint32]portState, len(mappings))
	undecided := make([]uint32, 0, len(mappings))
	for port, mapping := range mappings {
		if mapping.Suspended {
			result[port] = portStateSuspended
			continue
		}
		result[port] = portStatePending
		undecided = append(undecided, port)
	}
	if t == nil || len(undecided) == 0 {
		return result, nil
	}

	if t.redis == nil {
		t.resolveLocalStates(undecided, result)
		return result, nil
	}

	if err := t.resolveRedisStates(ctx, undecided, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (t *bindingTracker) resolveLocalStates(undecided []uint32, result map[uint32]portState) {
	bound := make(map[uint32]struct{})
	if t.local != nil {
		for _, port := range t.local() {
			bound[port] = struct{}{}
		}
	}
	for _, port := range undecided {
		if _, ok := bound[port]; ok {
			result[port] = portStateActive
		}
	}
}

func (t *bindingTracker) resolveRedisStates(ctx context.Context, undecided []uint32, result map[uint32]portState) error {
	pods, err := t.livePods(ctx)
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		return nil
	}
	bound, err := t.boundSet(ctx, undecided, pods)
	if err != nil {
		return err
	}
	for _, port := range undecided {
		active := true
		for _, pod := range pods {
			if !bound[boundKey(port, pod)] {
				active = false
				break
			}
		}
		if active {
			result[port] = portStateActive
		}
	}
	return nil
}

// livePods lists the pod addresses with a fresh controller liveness key.
func (t *bindingTracker) livePods(ctx context.Context) ([]string, error) {
	pods := make([]string, 0)
	var cursor uint64
	for {
		batch, next, err := t.redis.Scan(ctx, cursor, controllerKeyPrefix+"*", portScanBatchSize).Result()
		if err != nil {
			return nil, fmt.Errorf("scan live controllers in redis: %w", err)
		}
		for _, key := range batch {
			pods = append(pods, strings.TrimPrefix(key, controllerKeyPrefix))
		}
		if next == 0 {
			return pods, nil
		}
		cursor = next
	}
}

// boundSet fetches existence of every port x pod bound key in MGET batches.
func (t *bindingTracker) boundSet(ctx context.Context, ports []uint32, pods []string) (map[string]bool, error) {
	keys := make([]string, 0, len(ports)*len(pods))
	for _, port := range ports {
		for _, pod := range pods {
			keys = append(keys, boundKey(port, pod))
		}
	}
	bound := make(map[string]bool, len(keys))
	for start := 0; start < len(keys); start += int(portScanBatchSize) {
		end := min(start+int(portScanBatchSize), len(keys))
		values, err := t.redis.MGet(ctx, keys[start:end]...).Result()
		if err != nil {
			return nil, fmt.Errorf("fetch bound port keys from redis: %w", err)
		}
		for index, value := range values {
			bound[keys[start+index]] = value != nil
		}
	}
	return bound, nil
}
