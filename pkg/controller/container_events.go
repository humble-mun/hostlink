package controller

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sort"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// handleContainerEvent starts lifecycle reconciliation without blocking Control.
func (s *impl) handleContainerEvent(conn *agentConn, event *hostlinkv1.DockerEvent) {
	if s.store == nil || conn == nil || event == nil || event.GetContainerId() == "" {
		return
	}
	switch event.GetType() {
	case "die", "stop", "start":
	default:
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()
		s.processContainerEvent(ctx, conn, event)
	}()
}

func (s *impl) processContainerEvent(ctx context.Context, conn *agentConn, event *hostlinkv1.DockerEvent) {
	if s.store == nil || conn == nil || event == nil || event.GetContainerId() == "" {
		return
	}
	mappings, err := s.containerMappings(ctx, conn.agentID, event.GetContainerId())
	if err != nil || len(mappings) == 0 {
		return
	}
	switch event.GetType() {
	case "die", "stop":
		s.suspendContainerMappings(ctx, conn.agentID, event.GetContainerId(), mappings)
	case "start":
		s.resumeContainerMappings(ctx, conn, event.GetContainerId(), mappings)
	}
}

func (s *impl) containerMappings(ctx context.Context, agentID, containerID string) (map[uint32]portMapping, error) {
	desired, err := s.store.desired(ctx)
	if err != nil {
		s.logger.Error(err, "list public port mappings for container event failed", "agentID", agentID, "containerID", containerID)
		return nil, err
	}
	affected := make(map[uint32]portMapping)
	for port, mapping := range desired {
		if mapping.AgentID == agentID && mapping.ContainerID == containerID {
			affected[port] = mapping
		}
	}
	return affected, nil
}

func (s *impl) suspendContainerMappings(ctx context.Context, agentID, containerID string, mappings map[uint32]portMapping) {
	for port, mapping := range mappings {
		mapping.Suspended = true
		if err := s.store.update(ctx, port, mapping); err != nil {
			s.logger.Error(err, "suspend public port mapping failed", "port", port, "agentID", agentID, "containerID", containerID)
			continue
		}
		s.logger.Info("suspended public port mapping", "port", port, "agentID", agentID, "containerID", containerID)
	}
}

func (s *impl) resumeContainerMappings(ctx context.Context, conn *agentConn, containerID string, mappings map[uint32]portMapping) {
	payload, err := json.Marshal(agentapi.ContainerIDRequest{ID: containerID})
	if err != nil {
		s.logger.Error(err, "marshal container inspect request failed", "agentID", conn.agentID, "containerID", containerID)
		return
	}
	result, err := conn.dispatch(ctx, agentapi.MethodContainersInspect, payload)
	if err != nil {
		s.logger.Error(err, "inspect container after start failed", "agentID", conn.agentID, "containerID", containerID)
		return
	}
	if result.GetCode() < http.StatusOK || result.GetCode() >= http.StatusMultipleChoices {
		s.logger.V(0).Info("container inspect after start was rejected", "agentID", conn.agentID, "containerID", containerID, "code", result.GetCode())
		return
	}
	var detail agentapi.ContainerDetail
	if err := json.Unmarshal(result.GetPayload(), &detail); err != nil {
		s.logger.Error(err, "decode container inspect result failed", "agentID", conn.agentID, "containerID", containerID)
		return
	}
	for port, mapping := range mappings {
		host, targetPort, err := net.SplitHostPort(mapping.Target)
		if err != nil {
			s.logger.Error(err, "split public port target failed", "port", port, "agentID", conn.agentID, "containerID", containerID)
			continue
		}
		newIP := pickNetworkIP(detail.Networks, host)
		if len(detail.Networks) > 1 && newIP != host {
			s.logger.V(0).Info("selected container IP from multiple networks", "port", port, "agentID", conn.agentID, "containerID", containerID, "ip", newIP)
		}
		if newIP != "" {
			mapping.Target = net.JoinHostPort(newIP, targetPort)
		}
		mapping.Suspended = false
		if err := s.store.update(ctx, port, mapping); err != nil {
			s.logger.Error(err, "resume public port mapping failed", "port", port, "agentID", conn.agentID, "containerID", containerID)
			continue
		}
		s.logger.Info("resumed public port mapping", "port", port, "agentID", conn.agentID, "containerID", containerID)
	}
}

func pickNetworkIP(networks map[string]string, oldHost string) string {
	for _, ip := range networks {
		if ip == oldHost {
			return oldHost
		}
	}
	if len(networks) == 1 {
		for _, ip := range networks {
			return ip
		}
	}
	names := make([]string, 0, len(networks))
	for name := range networks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if ip := networks[name]; ip != "" {
			return ip
		}
	}
	return ""
}
