package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	redisv9 "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// errAgentDisconnected is returned by dispatch when the agent's Control stream
// closes before its result arrives.
var errAgentDisconnected = errors.New("agent disconnected before result")

const (
	// mappingKeyPrefix namespaces the agentID->pod entries in redis.
	mappingKeyPrefix = "hostlink:agent:"
	// mappingTTL bounds how long an agent->pod mapping survives without a refresh,
	// so a crashed holder's entry expires. The agent heartbeats well within it.
	mappingTTL = 45 * time.Second
	// redisOpTimeout bounds the best-effort registry bookkeeping writes, which run
	// off the request path on a background context.
	redisOpTimeout = 3 * time.Second
)

// dropMappingScript deletes the mapping only if it still points at this pod, so
// an agent that reconnected to a sibling is not clobbered by our teardown.
var dropMappingScript = redisv9.NewScript(
	`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`,
)

func mappingKey(agentID string) string {
	return mappingKeyPrefix + agentID
}

// registry tracks the agents whose Control stream is pinned to this replica.
// Lookups resolve the live connection a REST handler dispatches a request over;
// a miss means the agent is held by a sibling pod (the cross-pod ControllerPeer
// relay is layered on top of this lookup later).
type registry struct {
	logger logr.Logger
	// redis backs the cross-pod agentID->pod index. It is nil in single-replica
	// (in-memory) mode, in which case a lookup miss simply means the agent is not
	// connected anywhere this controller can see.
	redis redisv9.UniversalClient
	// selfAddr is this pod's ControllerPeer address, written as the mapping value
	// so a sibling can dial back the holding pod. Empty in in-memory mode.
	selfAddr string

	mu     sync.RWMutex
	agents map[string]*agentConn
}

func newRegistry(logger logr.Logger, redis redisv9.UniversalClient, selfAddr string) *registry {
	return &registry{logger: logger, redis: redis, selfAddr: selfAddr, agents: make(map[string]*agentConn)}
}

// close releases the redis client when one is configured.
func (r *registry) close() (err error) {
	if r.redis != nil {
		if err = r.redis.Close(); err != nil {
			err = fmt.Errorf("close redis: %w", err)
		}
	}
	return
}

// add registers c, returning any previous connection it displaced (a reconnect)
// so the caller can tear the stale one down. It also publishes the agent->pod
// mapping to redis so sibling replicas can relay requests here.
func (r *registry) add(c *agentConn) (replaced *agentConn) {
	r.mu.Lock()
	replaced = r.agents[c.agentID]
	r.agents[c.agentID] = c
	r.mu.Unlock()
	r.writeMapping(c.agentID)
	return
}

// remove deletes the local entry only if it still points at c, so a reconnect
// that already replaced it is not clobbered by the old stream's teardown, and
// drops the redis mapping when it was ours.
func (r *registry) remove(agentID string, c *agentConn) {
	r.mu.Lock()
	var removed bool
	if r.agents[agentID] == c {
		delete(r.agents, agentID)
		removed = true
	}
	r.mu.Unlock()
	if removed {
		r.dropMapping(agentID)
	}
}

func (r *registry) get(agentID string) (c *agentConn, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok = r.agents[agentID]
	return
}

// refresh re-asserts this pod's ownership of agentID and resets the mapping TTL.
// Driven by the agent's heartbeat so a crashed holder's mapping expires instead
// of black-holing relays.
func (r *registry) refresh(agentID string) {
	r.writeMapping(agentID)
}

// locate returns the ControllerPeer address of the pod currently holding agentID,
// or an empty string when no live mapping exists. Only meaningful in redis-backed
// mode; with no redis it always reports empty.
func (r *registry) locate(ctx context.Context, agentID string) (addr string, err error) {
	if r.redis == nil {
		return
	}
	if addr, err = r.redis.Get(ctx, mappingKey(agentID)).Result(); err != nil {
		if errors.Is(err, redisv9.Nil) {
			addr = ""
			err = nil
			return
		}
		err = fmt.Errorf("locate agent %q in redis: %w", agentID, err)
		return
	}
	return
}

// writeMapping publishes agentID->selfAddr with a TTL. It is best effort: redis
// is an optimization for cross-pod routing, so a failure is logged but never
// blocks local registration or dispatch.
func (r *registry) writeMapping(agentID string) {
	if r.redis == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
	defer cancel()
	if err := r.redis.Set(ctx, mappingKey(agentID), r.selfAddr, mappingTTL).Err(); err != nil {
		r.logger.Error(err, "write agent mapping to redis failed", "agentID", agentID)
	}
}

// dropMapping removes agentID's mapping when it still points at this pod.
func (r *registry) dropMapping(agentID string) {
	if r.redis == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisOpTimeout)
	defer cancel()
	if err := dropMappingScript.Run(ctx, r.redis, []string{mappingKey(agentID)}, r.selfAddr).Err(); err != nil && !errors.Is(err, redisv9.Nil) {
		r.logger.Error(err, "delete agent mapping from redis failed", "agentID", agentID)
	}
}

// agentConn is a single agent's live Control stream on this replica. It
// serializes command sends (a gRPC stream allows only one concurrent Send) and
// correlates each AgentResult back to the waiting dispatcher by request_id.
type agentConn struct {
	agentID string
	logger  logr.Logger
	stream  grpc.BidiStreamingServer[hostlinkv1.AgentEvent, hostlinkv1.Command]

	sendMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan *hostlinkv1.AgentResult
	closed  bool
	seq     atomic.Uint64
}

func newAgentConn(agentID string, stream grpc.BidiStreamingServer[hostlinkv1.AgentEvent, hostlinkv1.Command], logger logr.Logger) *agentConn {
	return &agentConn{
		agentID: agentID,
		logger:  logger,
		stream:  stream,
		pending: make(map[string]chan *hostlinkv1.AgentResult),
	}
}

// dispatch drives a single method/payload request to the agent and blocks until
// the correlated AgentResult arrives, ctx is done, or the stream closes.
func (c *agentConn) dispatch(ctx context.Context, method string, payload []byte) (result *hostlinkv1.AgentResult, err error) {
	requestID := strconv.FormatUint(c.seq.Add(1), 10)
	ch := make(chan *hostlinkv1.AgentResult, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		err = errAgentDisconnected
		return
	}
	c.pending[requestID] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
	}()

	if err = c.send(&hostlinkv1.Command{
		Cmd: &hostlinkv1.Command_Request{
			Request: &hostlinkv1.AgentRequest{
				RequestId: requestID,
				Method:    method,
				Payload:   payload,
			},
		},
	}); err != nil {
		return
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case r, ok := <-ch:
		if !ok {
			err = errAgentDisconnected
			return
		}
		result = r
	}
	return
}

// send writes a command to the agent under sendMu so concurrent dispatchers do
// not interleave writes on the stream.
func (c *agentConn) send(cmd *hostlinkv1.Command) (err error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if err = c.stream.Send(cmd); err != nil {
		err = fmt.Errorf("send command to agent %q: %w", c.agentID, err)
	}
	return
}

// deliver routes an AgentResult to its waiting dispatcher. Each request has its
// own buffered (cap 1) channel, so the send never blocks while holding mu.
func (c *agentConn) deliver(result *hostlinkv1.AgentResult) {
	requestID := result.GetRequestId()
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.pending[requestID]
	if !ok {
		c.logger.Info("dropping agent result with no pending request", "requestID", requestID)
		return
	}
	ch <- result
	delete(c.pending, requestID)
}

// closeAll marks the connection closed and unblocks every outstanding
// dispatcher with errAgentDisconnected. Called once when the Control stream ends.
func (c *agentConn) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for requestID, ch := range c.pending {
		close(ch)
		delete(c.pending, requestID)
	}
}
