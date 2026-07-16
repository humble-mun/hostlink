package controller

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sort"
	"strconv"

	"github.com/gin-gonic/gin"
)

type createForwardRequest struct {
	Target      string `json:"target"`
	ContainerID string `json:"container_id"`
}

type forwardResponse struct {
	Port  uint32    `json:"port"`
	State portState `json:"state"`
	portMapping
}

// createForward handles POST /api/v1/agents/:agentId/forwards.
func (svc *service) createForward(c *gin.Context) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName("createForward").WithValues("agentID", agentID)
	if svc.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "port forwarding is disabled"})
		return
	}

	var request createForwardRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}
	host, portText, err := net.SplitHostPort(request.Target)
	if err != nil || host == "" || portText == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid target"})
		return
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid target"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()
	mapping := portMapping{AgentID: agentID, Target: request.Target, ContainerID: request.ContainerID}
	allocatedPort, err := svc.store.allocate(ctx, svc.rangeFrom, svc.rangeTo, mapping)
	if err != nil {
		logger.Error(err, "allocate public forward port failed")
		switch {
		case errors.Is(err, errPortRangeExhausted):
			c.JSON(http.StatusConflict, gin.H{"error": "no free port in range"})
		default:
			c.JSON(http.StatusBadGateway, gin.H{"error": "allocate forward port failed"})
		}
		return
	}
	c.JSON(http.StatusCreated, forwardResponse{Port: allocatedPort, State: portStatePending, portMapping: mapping})
}

// listAgentForwards handles GET /api/v1/agents/:agentId/forwards.
func (svc *service) listAgentForwards(c *gin.Context) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName("listAgentForwards").WithValues("agentID", agentID)
	if svc.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "port forwarding is disabled"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()
	mappings, err := svc.store.desired(ctx)
	if err != nil {
		logger.Error(err, "list desired forwards failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list forwards failed"})
		return
	}
	states, err := svc.bindings.states(ctx, mappings)
	if err != nil {
		logger.Error(err, "compute forward states failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list forwards failed"})
		return
	}
	c.JSON(http.StatusOK, forwardResponses(mappings, states, agentID))
}

// listAllForwards handles GET /api/v1/forwards.
func (svc *service) listAllForwards(c *gin.Context) {
	logger := svc.logger.WithName("listAllForwards")
	if svc.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "port forwarding is disabled"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()
	mappings, err := svc.store.desired(ctx)
	if err != nil {
		logger.Error(err, "list desired forwards failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list forwards failed"})
		return
	}
	states, err := svc.bindings.states(ctx, mappings)
	if err != nil {
		logger.Error(err, "compute forward states failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list forwards failed"})
		return
	}
	c.JSON(http.StatusOK, forwardResponses(mappings, states, ""))
}

// deleteForward handles DELETE /api/v1/forwards/:port.
func (svc *service) deleteForward(c *gin.Context) {
	portText := c.Param("port")
	logger := svc.logger.WithName("deleteForward").WithValues("port", portText)
	if svc.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "port forwarding is disabled"})
		return
	}

	port, err := strconv.ParseUint(portText, 10, 32)
	if err != nil || port == 0 || port > 65535 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid port"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()
	if err := svc.store.release(ctx, uint32(port)); err != nil {
		logger.Error(err, "release public forward port failed")
		switch {
		case errors.Is(err, errPortNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "forward not found"})
		default:
			c.JSON(http.StatusBadGateway, gin.H{"error": "release forward port failed"})
		}
		return
	}
	c.Status(http.StatusNoContent)
}

func forwardResponses(mappings map[uint32]portMapping, states map[uint32]portState, agentID string) []forwardResponse {
	responses := make([]forwardResponse, 0, len(mappings))
	for port, mapping := range mappings {
		if agentID != "" && mapping.AgentID != agentID {
			continue
		}
		responses = append(responses, forwardResponse{Port: port, State: states[port], portMapping: mapping})
	}
	sort.Slice(responses, func(i, j int) bool {
		return responses[i].Port < responses[j].Port
	})
	return responses
}
