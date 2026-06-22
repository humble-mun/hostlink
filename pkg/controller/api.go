package controller

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

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
