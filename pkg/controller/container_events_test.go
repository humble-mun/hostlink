package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

func TestProcessContainerEventSuspendsOnlyMatchingMappings(t *testing.T) {
	for _, eventType := range []string{"die", "stop"} {
		t.Run(eventType, func(t *testing.T) {
			// Given
			svc, store, conn, _ := newContainerEventFixture(t)
			allocateTestPort(t, store, 42001, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080", ContainerID: "container-a"})
			allocateTestPort(t, store, 42002, portMapping{AgentID: "agent-b", Target: "172.30.1.5:8080", ContainerID: "container-a"})
			allocateTestPort(t, store, 42003, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080", ContainerID: "container-b"})
			allocateTestPort(t, store, 42004, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})

			// When
			svc.processContainerEvent(context.Background(), conn, &hostlinkv1.DockerEvent{Type: eventType, ContainerId: "container-a"})

			// Then
			desired, err := store.desired(context.Background())
			if err != nil {
				t.Fatalf("list desired mappings: %v", err)
			}
			if !desired[42001].Suspended {
				t.Fatal("matching mapping was not suspended")
			}
			for _, port := range []uint32{42002, 42003, 42004} {
				if desired[port].Suspended {
					t.Fatalf("unrelated mapping on port %d was suspended", port)
				}
			}
		})
	}
}

func TestProcessContainerEventStartRewritesTargetAndResumes(t *testing.T) {
	// Given
	svc, store, conn, commands := newContainerEventFixture(t)
	allocateTestPort(t, store, 42011, portMapping{
		AgentID: "agent-a", Target: "172.30.1.5:8080", ContainerID: "container-a", Suspended: true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), forwardHandlerTestTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.processContainerEvent(ctx, conn, &hostlinkv1.DockerEvent{Type: "start", ContainerId: "container-a"})
		close(done)
	}()
	command := waitForwardCommand(t, commands)
	request := command.GetRequest()
	if request == nil {
		t.Fatalf("command = %T, want AgentRequest", command.GetCmd())
	}
	if request.GetMethod() != agentapi.MethodContainersInspect {
		t.Fatalf("request method = %q, want %q", request.GetMethod(), agentapi.MethodContainersInspect)
	}
	var inspectRequest agentapi.ContainerIDRequest
	if err := json.Unmarshal(request.GetPayload(), &inspectRequest); err != nil {
		t.Fatalf("decode inspect request: %v", err)
	}
	if inspectRequest.ID != "container-a" {
		t.Fatalf("inspect request container ID = %q, want container-a", inspectRequest.ID)
	}
	payload, err := json.Marshal(agentapi.ContainerDetail{Networks: map[string]string{"bridge": "172.30.1.9"}})
	if err != nil {
		t.Fatalf("encode inspect result: %v", err)
	}

	// When
	conn.deliver(&hostlinkv1.AgentResult{RequestId: request.GetRequestId(), Code: http.StatusOK, Payload: payload})
	waitContainerEventDone(t, done)

	// Then
	mapping, err := store.lookup(context.Background(), 42011)
	if err != nil {
		t.Fatalf("lookup resumed mapping: %v", err)
	}
	if mapping.Target != "172.30.1.9:8080" {
		t.Fatalf("resumed target = %q, want 172.30.1.9:8080", mapping.Target)
	}
	if mapping.Suspended {
		t.Fatal("resumed mapping remained suspended")
	}
}

func TestProcessContainerEventStartKeepsMappingSuspendedWhenInspectFails(t *testing.T) {
	// Given
	svc, store, conn, _ := newContainerEventFixture(t)
	allocateTestPort(t, store, 42012, portMapping{
		AgentID: "agent-a", Target: "172.30.1.5:8080", ContainerID: "container-a", Suspended: true,
	})
	conn.closeAll()

	// When
	svc.processContainerEvent(context.Background(), conn, &hostlinkv1.DockerEvent{Type: "start", ContainerId: "container-a"})

	// Then
	mapping, err := store.lookup(context.Background(), 42012)
	if err != nil {
		t.Fatalf("lookup suspended mapping: %v", err)
	}
	if !mapping.Suspended {
		t.Fatal("mapping resumed after failed inspect dispatch")
	}
}

func TestProcessContainerEventIgnoresNilStore(t *testing.T) {
	// Given
	svc := &impl{logger: logr.Discard()}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("unexpected panic: %v", recovered)
		}
	}()

	// When
	svc.processContainerEvent(context.Background(), nil, &hostlinkv1.DockerEvent{Type: "die", ContainerId: "container-a"})

	// Then
}

func TestPickNetworkIP(t *testing.T) {
	tests := []struct {
		name     string
		networks map[string]string
		oldHost  string
		want     string
	}{
		{name: "preserves matching address", networks: map[string]string{"bridge": "172.30.1.5", "gpu": "10.0.0.8"}, oldHost: "172.30.1.5", want: "172.30.1.5"},
		{name: "uses only network", networks: map[string]string{"bridge": "172.30.1.9"}, oldHost: "172.30.1.5", want: "172.30.1.9"},
		{name: "uses sorted network fallback", networks: map[string]string{"gpu": "10.0.0.8", "bridge": "172.30.1.9"}, oldHost: "172.30.1.5", want: "172.30.1.9"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// When
			got := pickNetworkIP(test.networks, test.oldHost)

			// Then
			if got != test.want {
				t.Fatalf("network IP = %q, want %q", got, test.want)
			}
		})
	}
}

func newContainerEventFixture(t *testing.T) (*impl, portStore, *agentConn, <-chan *hostlinkv1.Command) {
	t.Helper()
	store := newPortStore(logr.Discard(), nil)
	registry := newRegistry(logr.Discard(), nil, "")
	stream := newControlStream(context.Background())
	conn := newAgentConn("agent-a", stream, logr.Discard())
	t.Cleanup(func() {
		if err := registry.close(); err != nil {
			t.Errorf("close registry: %v", err)
		}
		if err := store.close(); err != nil {
			t.Errorf("close port store: %v", err)
		}
	})
	return &impl{logger: logr.Discard(), registry: registry, store: store}, store, conn, stream.commands
}

func waitContainerEventDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(forwardHandlerTestTimeout):
		t.Fatal("timed out waiting for container event processing")
	}
}
