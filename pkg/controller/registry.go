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

	"github.com/humble-mun/hostlink/pkg/agentapi"
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

// listAll returns the set of agentIDs currently online across all replicas, by
// scanning the agent->pod directory in redis. In in-memory mode (no redis) it
// falls back to the locally-held agents, which is the entire fleet for a
// single-replica controller. The returned list is deduplicated but unordered.
func (r *registry) listAll(ctx context.Context) (agentIDs []string, err error) {
	if r.redis == nil {
		r.mu.RLock()
		agentIDs = make([]string, 0, len(r.agents))
		for id := range r.agents {
			agentIDs = append(agentIDs, id)
		}
		r.mu.RUnlock()
		return
	}

	seen := make(map[string]struct{})
	var cursor uint64
	for {
		var keys []string
		if keys, cursor, err = r.redis.Scan(ctx, cursor, mappingKeyPrefix+"*", 256).Result(); err != nil {
			err = fmt.Errorf("scan agent mappings in redis: %w", err)
			return
		}
		for _, key := range keys {
			if len(key) <= len(mappingKeyPrefix) {
				continue
			}
			seen[key[len(mappingKeyPrefix):]] = struct{}{}
		}
		if cursor == 0 {
			break
		}
	}
	agentIDs = make([]string, 0, len(seen))
	for id := range seen {
		agentIDs = append(agentIDs, id)
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

	mu            sync.Mutex
	pending       map[string]chan *hostlinkv1.AgentResult
	pendingStream map[string]*streamReg
	closed        bool
	seq           atomic.Uint64
}

// streamFrame is one delivery on a streaming dispatch's channel: either a
// progress frame (Final false, Payload carries the method-specific progress
// JSON or raw bytes) or the terminal frame (Final true, Code/Error report the
// outcome).
type streamFrame struct {
	Payload []byte
	Code    uint32
	Error   string
	Final   bool
}

// streamReg is the registration backing one streaming dispatch. The consumer
// reads frames from ch and watches done for an abnormal end (agent disconnect or
// caller cancel); end-of-stream proper is the terminal frame (Final true).
// reliable selects backpressure (file data must not be dropped) over the
// advisory drop-on-full used for progress (e.g. images.pull). done is closed at
// most once (guarded by once), by whichever of cancel/closeAll fires first, so a
// writer blocked on ch can never panic on a closed channel.
type streamReg struct {
	ch       chan streamFrame
	done     chan struct{}
	reliable bool
	once     sync.Once
}

func (r *streamReg) close() {
	r.once.Do(func() { close(r.done) })
}

// streamReliable reports whether a streaming method requires lossless delivery.
// File reads carry bytes that must never be dropped; progress streams (images.pull)
// are advisory and may drop frames under a slow consumer.
func streamReliable(method string) bool {
	return method == agentapi.MethodFsRead
}

func newAgentConn(agentID string, stream grpc.BidiStreamingServer[hostlinkv1.AgentEvent, hostlinkv1.Command], logger logr.Logger) *agentConn {
	return &agentConn{
		agentID:       agentID,
		logger:        logger,
		stream:        stream,
		pending:       make(map[string]chan *hostlinkv1.AgentResult),
		pendingStream: make(map[string]*streamReg),
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

// dispatchStream drives a streaming method to the agent and returns a channel of
// frames (zero or more progress frames followed by exactly one terminal frame
// with Final true) plus a done channel that closes on an abnormal end (agent
// disconnect or cancel). The channel is never closed by the writer; the consumer
// detects end-of-stream from the terminal frame and abnormal end from done.
// Reliable methods (fs.read) get lossless backpressure; others drop progress on a
// full buffer so a slow consumer can never stall the shared Control stream. The
// returned cancel removes the registration and must be called when the caller
// stops reading (e.g. its context ends).
func (c *agentConn) dispatchStream(_ context.Context, method string, payload []byte) (frames <-chan streamFrame, done <-chan struct{}, cancel func(), err error) {
	requestID := strconv.FormatUint(c.seq.Add(1), 10)
	reg := &streamReg{ch: make(chan streamFrame, 64), done: make(chan struct{}), reliable: streamReliable(method)}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		err = errAgentDisconnected
		return
	}
	c.pendingStream[requestID] = reg
	c.mu.Unlock()

	cancel = func() {
		c.mu.Lock()
		if c.pendingStream[requestID] == reg {
			delete(c.pendingStream, requestID)
		}
		c.mu.Unlock()
		reg.close()
	}

	if err = c.send(&hostlinkv1.Command{
		Cmd: &hostlinkv1.Command_Request{
			Request: &hostlinkv1.AgentRequest{
				RequestId: requestID,
				Method:    method,
				Payload:   payload,
			},
		},
	}); err != nil {
		cancel()
		cancel = nil
		return
	}
	frames = reg.ch
	done = reg.done
	return
}

// uploadDispatch is a controller-side handle to an in-flight streaming upload to
// the agent. The opening AgentRequest has been sent; the caller streams the body
// with sendChunk and awaits the terminal result with await.
type uploadDispatch struct {
	conn      *agentConn
	requestID string
	result    chan *hostlinkv1.AgentResult
}

// dispatchUpload opens a streaming upload: it sends the opening AgentRequest and
// registers a pending result channel, returning a handle the caller drives. The
// agent's terminal AgentResult is routed back through the unary pending path.
func (c *agentConn) dispatchUpload(method string, payload []byte) (up *uploadDispatch, err error) {
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

	if err = c.send(&hostlinkv1.Command{
		Cmd: &hostlinkv1.Command_Request{
			Request: &hostlinkv1.AgentRequest{RequestId: requestID, Method: method, Payload: payload},
		},
	}); err != nil {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
		return
	}
	up = &uploadDispatch{conn: c, requestID: requestID, result: ch}
	return
}

// sendChunk streams one body chunk to the agent. The underlying stream Send
// blocks under gRPC flow control when the agent is slow, so this provides upload
// backpressure. last marks the final chunk.
func (up *uploadDispatch) sendChunk(data []byte, last bool) (err error) {
	err = up.conn.send(&hostlinkv1.Command{
		Cmd: &hostlinkv1.Command_Chunk{
			Chunk: &hostlinkv1.AgentRequestChunk{RequestId: up.requestID, Data: data, Last: last},
		},
	})
	return
}

// await blocks until the agent's terminal AgentResult arrives, ctx is done, or
// the stream closes. It removes the pending registration on return.
func (up *uploadDispatch) await(ctx context.Context) (result *hostlinkv1.AgentResult, err error) {
	defer func() {
		up.conn.mu.Lock()
		delete(up.conn.pending, up.requestID)
		up.conn.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case r, ok := <-up.result:
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

// deliver routes a terminal AgentResult to its waiting dispatcher. It first looks
// for a streaming request (delivering a terminal frame), then a unary one (which
// covers both plain dispatch and the streaming-upload result). The registration is
// removed under mu, then the send happens off-lock: a streaming send races the
// reg's done so a stopped consumer cannot block it, and each unary channel is
// buffered (cap 1) so its send never blocks.
func (c *agentConn) deliver(result *hostlinkv1.AgentResult) {
	requestID := result.GetRequestId()
	c.mu.Lock()
	if reg, ok := c.pendingStream[requestID]; ok {
		delete(c.pendingStream, requestID)
		c.mu.Unlock()
		select {
		case reg.ch <- streamFrame{Payload: result.GetPayload(), Code: result.GetCode(), Error: result.GetError(), Final: true}:
		case <-reg.done:
		}
		return
	}
	ch, ok := c.pending[requestID]
	if !ok {
		c.mu.Unlock()
		c.logger.Info("dropping agent result with no pending request", "requestID", requestID)
		return
	}
	delete(c.pending, requestID)
	c.mu.Unlock()
	ch <- result
}

// deliverProgress routes an AgentProgress frame to its streaming dispatcher. The
// send happens off-lock and races the reg's done so a torn-down stream never
// blocks the receive loop. For a reliable stream the send blocks for backpressure
// (file bytes must not be dropped); otherwise a full buffer drops the frame
// (progress is advisory).
func (c *agentConn) deliverProgress(progress *hostlinkv1.AgentProgress) {
	requestID := progress.GetRequestId()
	c.mu.Lock()
	reg, ok := c.pendingStream[requestID]
	c.mu.Unlock()
	if !ok {
		c.logger.Info("dropping agent progress with no pending stream", "requestID", requestID)
		return
	}
	frame := streamFrame{Payload: progress.GetPayload()}
	if reg.reliable {
		select {
		case reg.ch <- frame:
		case <-reg.done:
		}
		return
	}
	select {
	case reg.ch <- frame:
	case <-reg.done:
	default:
		c.logger.Info("dropping agent progress: consumer buffer full", "requestID", requestID)
	}
}

// closeAll marks the connection closed and unblocks every outstanding dispatcher
// with errAgentDisconnected. Unary waiters are woken by closing their channel;
// streaming consumers (and any writer mid-send) are woken by closing each reg's
// done. Called once when the Control stream ends.
func (c *agentConn) closeAll() {
	c.mu.Lock()
	c.closed = true
	for requestID, ch := range c.pending {
		close(ch)
		delete(c.pending, requestID)
	}
	regs := make([]*streamReg, 0, len(c.pendingStream))
	for requestID, reg := range c.pendingStream {
		regs = append(regs, reg)
		delete(c.pendingStream, requestID)
	}
	c.mu.Unlock()
	for _, reg := range regs {
		reg.close()
	}
}
