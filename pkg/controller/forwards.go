package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
)

type createForwardRequest struct {
	Target      string `json:"target"`
	ContainerID string `json:"container_id"`
	Port        uint32 `json:"port"`
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

	request, ok := svc.bindCreateForwardRequest(c)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()
	mapping := portMapping{AgentID: agentID, Target: request.Target, ContainerID: request.ContainerID}
	rangeFrom, rangeTo := svc.allocationRange(request.Port)
	allocatedPort, err := svc.store.allocate(ctx, rangeFrom, rangeTo, mapping)
	if err != nil {
		logger.Error(err, "allocate public forward port failed")
		writeAllocateForwardError(c, err, request.Port)
		return
	}
	c.JSON(http.StatusCreated, forwardResponse{Port: allocatedPort, State: portStatePending, portMapping: mapping})
}

// bindCreateForwardRequest parses and validates the create-forward body,
// writing an error response and returning ok=false when it is invalid.
func (svc *service) bindCreateForwardRequest(c *gin.Context) (createForwardRequest, bool) {
	var request createForwardRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return createForwardRequest{}, false
	}
	if !validForwardTarget(request.Target) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid target"})
		return createForwardRequest{}, false
	}
	if request.Port != 0 && (request.Port < svc.rangeFrom || request.Port > svc.rangeTo) {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("port must be within %d-%d", svc.rangeFrom, svc.rangeTo)})
		return createForwardRequest{}, false
	}
	return request, true
}

// validForwardTarget reports whether target is a host:port with a non-zero
// 16-bit port.
func validForwardTarget(target string) bool {
	host, portText, err := net.SplitHostPort(target)
	if err != nil || host == "" || portText == "" {
		return false
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	return err == nil && port != 0
}

// allocationRange returns the public port range to allocate from, narrowed
// to the requested port when one was supplied.
func (svc *service) allocationRange(requested uint32) (from, to uint32) {
	if requested != 0 {
		return requested, requested
	}
	return svc.rangeFrom, svc.rangeTo
}

// writeAllocateForwardError maps a port allocation failure to an HTTP error
// response.
func writeAllocateForwardError(c *gin.Context, err error, requested uint32) {
	switch {
	case errors.Is(err, errPortRangeExhausted) && requested != 0:
		c.JSON(http.StatusConflict, gin.H{"error": "requested port already in use"})
	case errors.Is(err, errPortRangeExhausted):
		c.JSON(http.StatusConflict, gin.H{"error": "no free port in range"})
	default:
		c.JSON(http.StatusBadGateway, gin.H{"error": "allocate forward port failed"})
	}
}

// listAgentForwards handles GET /api/v1/agents/:agentId/forwards.
func (svc *service) listAgentForwards(c *gin.Context) {
	agentID := c.Param("agentId")
	logger := svc.logger.WithName("listAgentForwards").WithValues("agentID", agentID)
	mappings, states, ok := svc.fetchForwards(c, logger)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, forwardResponses(mappings, states, agentID))
}

// listAllForwards handles GET /api/v1/forwards.
func (svc *service) listAllForwards(c *gin.Context) {
	logger := svc.logger.WithName("listAllForwards")
	mappings, states, ok := svc.fetchForwards(c, logger)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, forwardResponses(mappings, states, ""))
}

// fetchForwards loads the desired port mappings and their current bind
// states, writing an error response and returning ok=false if the store is
// disabled or either lookup fails.
func (svc *service) fetchForwards(c *gin.Context, logger logr.Logger) (mappings map[uint32]portMapping, states map[uint32]portState, ok bool) {
	if svc.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "port forwarding is disabled"})
		return nil, nil, false
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()
	mappings, err := svc.store.desired(ctx)
	if err != nil {
		logger.Error(err, "list desired forwards failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list forwards failed"})
		return nil, nil, false
	}
	states, err = svc.bindings.states(ctx, mappings)
	if err != nil {
		logger.Error(err, "compute forward states failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list forwards failed"})
		return nil, nil, false
	}
	return mappings, states, true
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
