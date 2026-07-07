package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/humble-mun/hostlink/pkg/agentapi"
)

// dispatchJSON marshals reqBody (nil dispatches an empty payload), drives the
// single-shot method to the agent named in the :agentId path param under
// dispatchTimeout, and writes the agent's result to the HTTP response. It is
// the shared tail of every non-streaming container handler.
func (svc *service) dispatchJSON(c *gin.Context, logName, method string, reqBody any) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName(logName).WithValues("agentID", agentID)

	var payload []byte
	if reqBody != nil {
		var err error
		if payload, err = json.Marshal(reqBody); err != nil {
			logger.Error(err, "encode request payload failed", "method", method)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "encode request failed"})
			return
		}
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), dispatchTimeout)
	defer cancel()

	result, err := svc.dispatch(ctx, agentID, method, payload)
	if err != nil {
		logger.Error(err, "dispatch to agent failed", "method", method)
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

// listAgentContainers handles GET /api/v1/agents/:agentId/containers. The
// optional `all` query flag includes stopped containers (docker ps -a).
func (svc *service) listAgentContainers(c *gin.Context) {
	all, err := parseBoolQuery(c, "all")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid all query parameter"})
		return
	}
	svc.dispatchJSON(c, "listAgentContainers", agentapi.MethodContainersList, agentapi.ContainerListRequest{All: all})
}

// createAgentContainer handles POST /api/v1/agents/:agentId/containers. The
// JSON body is a ContainerCreateRequest; the container is created and started
// (docker run semantics) and the created ID is returned with status 201. The
// image must already be present on the agent host (pull it via the images API
// first).
func (svc *service) createAgentContainer(c *gin.Context) {
	var body agentapi.ContainerCreateRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid container create request body"})
		return
	}
	if body.Image == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "image is required"})
		return
	}
	svc.dispatchJSON(c, "createAgentContainer", agentapi.MethodContainersCreate, body)
}

// inspectAgentContainer handles GET /api/v1/agents/:agentId/containers/:containerId
// and returns the agent's ContainerDetail JSON payload unchanged.
func (svc *service) inspectAgentContainer(c *gin.Context) {
	svc.dispatchJSON(c, "inspectAgentContainer", agentapi.MethodContainersInspect,
		agentapi.ContainerIDRequest{ID: c.Param("containerId")})
}

// startAgentContainer handles POST /api/v1/agents/:agentId/containers/:containerId/start.
func (svc *service) startAgentContainer(c *gin.Context) {
	svc.dispatchJSON(c, "startAgentContainer", agentapi.MethodContainersStart,
		agentapi.ContainerIDRequest{ID: c.Param("containerId")})
}

// stopAgentContainer handles POST /api/v1/agents/:agentId/containers/:containerId/stop.
// The optional `timeout` query gives the grace period in seconds before the
// daemon kills the container.
func (svc *service) stopAgentContainer(c *gin.Context) {
	timeout, err := parseTimeoutQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timeout query parameter"})
		return
	}
	svc.dispatchJSON(c, "stopAgentContainer", agentapi.MethodContainersStop,
		agentapi.ContainerStopRequest{ID: c.Param("containerId"), Timeout: timeout})
}

// restartAgentContainer handles POST /api/v1/agents/:agentId/containers/:containerId/restart
// with the same optional `timeout` query as stop.
func (svc *service) restartAgentContainer(c *gin.Context) {
	timeout, err := parseTimeoutQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timeout query parameter"})
		return
	}
	svc.dispatchJSON(c, "restartAgentContainer", agentapi.MethodContainersRestart,
		agentapi.ContainerStopRequest{ID: c.Param("containerId"), Timeout: timeout})
}

// removeAgentContainer handles DELETE /api/v1/agents/:agentId/containers/:containerId.
// The optional `force` query flag removes a running container (docker rm -f)
// and `volumes` also removes its anonymous volumes (docker rm -v).
func (svc *service) removeAgentContainer(c *gin.Context) {
	force, err := parseBoolQuery(c, "force")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid force query parameter"})
		return
	}
	volumes, err := parseBoolQuery(c, "volumes")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid volumes query parameter"})
		return
	}
	svc.dispatchJSON(c, "removeAgentContainer", agentapi.MethodContainersRemove,
		agentapi.ContainerRemoveRequest{ID: c.Param("containerId"), Force: force, RemoveVolumes: volumes})
}

// logsAgentContainer handles GET /api/v1/agents/:agentId/containers/:containerId/logs.
// The logs are delivered as Server-Sent Events: one data event per log line (a
// LogFrame JSON body) terminated by a {done:true} event. The query parameters
// mirror docker logs: `follow` keeps the stream open for new output until the
// client disconnects, `tail` limits the initial backlog (a line count or
// "all"), `since` bounds the start time (RFC3339 or a unix timestamp), and
// `timestamps` prefixes each line with its time. No dispatch timeout applies:
// the stream is bounded by the request context, and closing it propagates a
// cancel to the agent so a followed stream stops producing.
func (svc *service) logsAgentContainer(c *gin.Context) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName("logsAgentContainer").WithValues("agentID", agentID)

	follow, err := parseBoolQuery(c, "follow")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid follow query parameter"})
		return
	}
	timestamps, err := parseBoolQuery(c, "timestamps")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timestamps query parameter"})
		return
	}

	payload, err := json.Marshal(agentapi.ContainerLogsRequest{
		ID:         c.Param("containerId"),
		Follow:     follow,
		Tail:       c.Query("tail"),
		Since:      c.Query("since"),
		Timestamps: timestamps,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "encode logs request failed"})
		return
	}

	frames, done, cancel, err := svc.dispatchStream(c.Request.Context(), agentID, agentapi.MethodContainersLogs, payload)
	if err != nil {
		logger.Error(err, "dispatch containers.logs to agent failed")
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

// parseTimeoutQuery reads the optional `timeout` query parameter (seconds). A
// missing or empty value yields nil, meaning the daemon default grace period.
func parseTimeoutQuery(c *gin.Context) (timeout *int, err error) {
	raw := c.Query("timeout")
	if raw == "" {
		return
	}
	var v int
	if v, err = strconv.Atoi(raw); err != nil {
		return
	}
	timeout = &v
	return
}
