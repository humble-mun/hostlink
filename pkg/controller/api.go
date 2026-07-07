package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// fsUploadChunkSize bounds each body chunk the controller streams to an agent on
// an upload. It mirrors the agent-side read chunk size and stays well under the
// gRPC max message size so uploads of any length need no raised limits.
const fsUploadChunkSize = 64 * 1024

// dispatchTimeout bounds how long a REST handler waits for an agent to answer a
// dispatched request before returning a gateway timeout.
const dispatchTimeout = 30 * time.Second

// listAgentImages handles GET /api/v1/agents/:agentId/images. It dispatches an
// images.list request to the agent (locally or relayed to the holding pod) and
// returns the agent's JSON payload unchanged.
func (svc *service) listAgentImages(c *gin.Context) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName("listAgentImages").WithValues("agentID", agentID)

	ctx, cancel := context.WithTimeout(c.Request.Context(), dispatchTimeout)
	defer cancel()

	var result *hostlinkv1.AgentResult
	var err error
	if result, err = svc.dispatch(ctx, agentID, agentapi.MethodImagesList, nil); err != nil {
		logger.Error(err, "dispatch images.list to agent failed")
		switch {
		case errors.Is(err, errAgentNotConnected):
			c.JSON(http.StatusNotFound, gin.H{"error": "agent not connected"})
		case errors.Is(err, context.DeadlineExceeded):
			c.JSON(http.StatusGatewayTimeout, gin.H{"error": "agent did not respond in time"})
		default:
			c.JSON(http.StatusBadGateway, gin.H{"error": "agent request failed"})
		}
		return
	}

	respondAgentResult(c, result)
}

// dispatch drives a method/payload request to agentID. It prefers the local
// Control stream; on a miss it resolves the holding pod from the registry and
// relays via ControllerPeer. It returns errAgentNotConnected when the agent is
// reachable from no replica (or the peer plane is disabled).
func (svc *service) dispatch(ctx context.Context, agentID, method string, payload []byte) (result *hostlinkv1.AgentResult, err error) {
	if conn, ok := svc.registry.get(agentID); ok {
		result, err = conn.dispatch(ctx, method, payload)
		return
	}

	if svc.peers == nil {
		err = errAgentNotConnected
		return
	}

	var addr string
	if addr, err = svc.registry.locate(ctx, agentID); err != nil {
		return
	}
	if addr == "" || addr == svc.selfAddr {
		err = errAgentNotConnected
		return
	}

	result, err = svc.peers.dispatch(ctx, addr, agentID, method, payload)
	return
}

// listAgents handles GET /api/v1/agents. It returns the set of online agentIDs
// across all controller replicas by reading the redis agent->pod directory; in
// in-memory mode it returns the agents held by this replica only.
func (svc *service) listAgents(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()

	agentIDs, err := svc.registry.listAll(ctx)
	if err != nil {
		svc.logger.WithName("listAgents").Error(err, "list online agents failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list online agents failed"})
		return
	}
	if agentIDs == nil {
		agentIDs = []string{}
	}
	c.JSON(http.StatusOK, gin.H{"agents": agentIDs})
}

// pullRequestBody is the JSON request body for POST /api/v1/agents/:agentId/images:
// the image reference to pull plus optional private-registry auth.
type pullRequestBody struct {
	Image string                 `json:"image"`
	Auth  *agentapi.RegistryAuth `json:"auth,omitempty"`
}

// streamDoneFrame is the terminal SSE event emitted once a streamed operation
// (images.pull, containers.logs) finishes, successfully or not.
type streamDoneFrame struct {
	Done  bool   `json:"done"`
	Code  uint32 `json:"code,omitempty"`
	Error string `json:"error,omitempty"`
}

// dispatchStream drives a streaming method to agentID, mirroring dispatch: it
// prefers the local Control stream and otherwise relays to the holding pod via
// ControllerPeer. It returns a channel of frames (progress frames then one
// terminal frame) plus a cancel that the caller must invoke when it stops
// reading. It returns errAgentNotConnected when no replica holds the agent.
func (svc *service) dispatchStream(ctx context.Context, agentID, method string, payload []byte) (frames <-chan streamFrame, done <-chan struct{}, cancel func(), err error) {
	if conn, ok := svc.registry.get(agentID); ok {
		frames, done, cancel, err = conn.dispatchStream(ctx, method, payload)
		return
	}

	if svc.peers == nil {
		err = errAgentNotConnected
		return
	}

	var addr string
	if addr, err = svc.registry.locate(ctx, agentID); err != nil {
		return
	}
	if addr == "" || addr == svc.selfAddr {
		err = errAgentNotConnected
		return
	}

	// The peer relay is driven entirely by ctx; derive a cancelable child so the
	// caller can tear the relay down the same way it would a local stream.
	relayCtx, relayCancel := context.WithCancel(ctx)
	if frames, done, err = svc.peers.dispatchStream(relayCtx, addr, agentID, method, payload); err != nil {
		relayCancel()
		return
	}
	cancel = relayCancel
	return
}

// dispatchUpload drives a streaming controller->agent upload to agentID. It
// prefers the local Control stream and otherwise relays to the holding pod via
// ControllerPeer.Upload. The opening AgentRequest (method/openPayload) is sent,
// then body is streamed in chunks, and the agent's terminal AgentResult is
// returned. It returns errAgentNotConnected when no replica holds the agent.
func (svc *service) dispatchUpload(ctx context.Context, agentID, method string, openPayload []byte, body io.Reader) (result *hostlinkv1.AgentResult, err error) {
	if conn, ok := svc.registry.get(agentID); ok {
		var up *uploadDispatch
		if up, err = conn.dispatchUpload(method, openPayload); err != nil {
			return
		}
		if err = streamUploadBody(body, up.sendChunk); err != nil {
			// The body read or a chunk send failed (e.g. client disconnected). The
			// agent aborts the partial write on its idle timeout; surface the error.
			return
		}
		result, err = up.await(ctx)
		return
	}

	if svc.peers == nil {
		err = errAgentNotConnected
		return
	}

	var addr string
	if addr, err = svc.registry.locate(ctx, agentID); err != nil {
		return
	}
	if addr == "" || addr == svc.selfAddr {
		err = errAgentNotConnected
		return
	}

	result, err = svc.peers.upload(ctx, addr, agentID, method, openPayload, body)
	return
}

// consumeStream drives a streaming dispatch to completion: it invokes onFrame for
// each frame until one returns true (the terminal frame), the context ends, or
// done closes (agent disconnect). On disconnect it first drains any already
// delivered frames — a terminal frame among them still completes the stream — and
// otherwise calls onAbort.
func consumeStream(ctx context.Context, frames <-chan streamFrame, done <-chan struct{}, onFrame func(streamFrame) (finished bool), onAbort func()) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame := <-frames:
			if onFrame(frame) {
				return
			}
		case <-done:
			for {
				select {
				case frame := <-frames:
					if onFrame(frame) {
						return
					}
				default:
					onAbort()
					return
				}
			}
		}
	}
}

// streamUploadBody reads body in fsUploadChunkSize pieces and delivers each via
// send (last=false), then a terminal empty chunk (last=true) to mark completion.
// send fully consumes each slice before returning (the underlying stream Send
// marshals it synchronously), so the read buffer is safely reused.
func streamUploadBody(body io.Reader, send func(data []byte, last bool) error) (err error) {
	buf := make([]byte, fsUploadChunkSize)
	for {
		var n int
		n, err = body.Read(buf)
		if n > 0 {
			if serr := send(buf[:n], false); serr != nil {
				err = serr
				return
			}
		}
		if errors.Is(err, io.EOF) {
			err = send(nil, true)
			return
		}
		if err != nil {
			return
		}
	}
}

// pullAgentImage handles POST /api/v1/agents/:agentId/images. The JSON body
// carries the image reference plus optional registry auth; docker pull progress
// is streamed back as Server-Sent Events (one `data:` event per progress frame)
// terminated by a {done:true} event. The pull is bounded only by the request
// context (large images take minutes), so no dispatchTimeout is applied.
func (svc *service) pullAgentImage(c *gin.Context) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName("pullAgentImage").WithValues("agentID", agentID)

	var body pullRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid pull request body"})
		return
	}
	if body.Image == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "image is required"})
		return
	}

	payload, err := json.Marshal(agentapi.PullRequest{Image: body.Image, Auth: body.Auth})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "encode pull request failed"})
		return
	}

	frames, done, cancel, err := svc.dispatchStream(c.Request.Context(), agentID, agentapi.MethodImagesPull, payload)
	if err != nil {
		logger.Error(err, "dispatch images.pull to agent failed")
		switch {
		case errors.Is(err, errAgentNotConnected):
			c.JSON(http.StatusNotFound, gin.H{"error": "agent not connected"})
		default:
			c.JSON(http.StatusBadGateway, gin.H{"error": "dispatch failed"})
		}
		return
	}

	serveSSEStream(c, logger, frames, done, cancel)
}

// serveSSEStream writes a dispatched stream to the client as Server-Sent
// Events: each progress frame's JSON payload becomes one data event, and the
// terminal frame (or an agent disconnect) becomes a closing streamDoneFrame
// event. cancel is always invoked when the client stops reading, which tears
// the dispatch down and — for unbounded streams like a followed
// containers.logs — stops the agent-side operation.
func serveSSEStream(c *gin.Context, logger logr.Logger, frames <-chan streamFrame, done <-chan struct{}, cancel func()) {
	defer cancel()

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		logger.Error(nil, "response writer does not support streaming")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming unsupported"})
		return
	}
	header := c.Writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	// emit writes one frame as SSE and reports whether the stream is finished.
	emit := func(frame streamFrame) (finished bool) {
		if frame.Final {
			writeSSEEvent(c.Writer, streamDoneFrame{Done: true, Code: frame.Code, Error: frame.Error})
			flusher.Flush()
			return true
		}
		if _, werr := writeSSERaw(c.Writer, frame.Payload); werr != nil {
			logger.Error(werr, "write progress event failed; client gone")
			return true
		}
		flusher.Flush()
		return false
	}

	consumeStream(c.Request.Context(), frames, done, emit, func() {
		writeSSEEvent(c.Writer, streamDoneFrame{Done: true, Error: "agent disconnected"})
		flusher.Flush()
	})
}

// writeSSEEvent marshals v and writes it as a single SSE data event.
func writeSSEEvent(w http.ResponseWriter, v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		return
	}
	_, _ = writeSSERaw(w, payload)
}

// writeSSERaw writes an already-encoded JSON payload as one SSE data event.
func writeSSERaw(w http.ResponseWriter, payload []byte) (int, error) {
	return fmt.Fprintf(w, "data: %s\n\n", payload)
}

// removeAgentImages handles both DELETE /api/v1/agents/:agentId/images/:imageId
// (single image, the reference/ID in the path) and DELETE
// /api/v1/agents/:agentId/images?ref=A&ref=B (batch, references in repeatable
// `ref` query params). The optional `force` and `noPrune` query flags map to the
// Docker image-remove options. It dispatches an images.remove request to the
// agent and returns the agent's RemoveResult JSON payload unchanged.
func (svc *service) removeAgentImages(c *gin.Context) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName("removeAgentImages").WithValues("agentID", agentID)

	var refs []string
	if imageID := c.Param("imageId"); imageID != "" {
		refs = []string{imageID}
	} else {
		refs = c.QueryArray("ref")
	}
	if len(refs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one image reference is required"})
		return
	}

	force, err := parseBoolQuery(c, "force")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid force query parameter"})
		return
	}
	noPrune, err := parseBoolQuery(c, "noPrune")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid noPrune query parameter"})
		return
	}

	payload, err := json.Marshal(agentapi.RemoveRequest{Refs: refs, Force: force, NoPrune: noPrune})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "encode remove request failed"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), dispatchTimeout)
	defer cancel()

	result, err := svc.dispatch(ctx, agentID, agentapi.MethodImagesRemove, payload)
	if err != nil {
		logger.Error(err, "dispatch images.remove to agent failed")
		switch {
		case errors.Is(err, errAgentNotConnected):
			c.JSON(http.StatusNotFound, gin.H{"error": "agent not connected"})
		case errors.Is(err, context.DeadlineExceeded):
			c.JSON(http.StatusGatewayTimeout, gin.H{"error": "agent did not respond in time"})
		default:
			c.JSON(http.StatusBadGateway, gin.H{"error": "agent request failed"})
		}
		return
	}

	respondAgentResult(c, result)
}

// respondAgentResult writes an AgentResult to the HTTP response. The agent sets
// Code to an HTTP status; a zero Code is treated as a bad gateway. A non-empty
// payload is returned verbatim as JSON, otherwise a non-empty error becomes a
// {"error":...} body, and an empty result yields a bare status.
func respondAgentResult(c *gin.Context, result *hostlinkv1.AgentResult) {
	code := int(result.GetCode())
	if code == 0 {
		code = http.StatusBadGateway
	}
	if len(result.GetPayload()) > 0 {
		c.Data(code, "application/json; charset=utf-8", result.GetPayload())
		return
	}
	if msg := result.GetError(); msg != "" {
		c.JSON(code, gin.H{"error": msg})
		return
	}
	c.Status(code)
}

// parseBoolQuery reads an optional boolean query parameter. A missing or empty
// value is treated as false; a present value must parse as a Go bool.
func parseBoolQuery(c *gin.Context, key string) (bool, error) {
	raw := c.Query(key)
	if raw == "" {
		return false, nil
	}
	return strconv.ParseBool(raw)
}
