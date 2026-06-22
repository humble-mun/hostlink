package controller

import (
	"errors"
	"io"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// impl is the AgentLink gRPC service implementation (the server side of the
// agent<->controller stream). It is distinct from service, which owns the
// controller's lifecycle and HTTP/metrics hooks.
type impl struct {
	hostlinkv1.UnimplementedAgentLinkServer
	logger   logr.Logger
	nodeName string
	registry *registry
}

// Control runs the bidirectional command stream. On the agent's Hello it registers
// the connection so REST handlers can dispatch requests down the response stream,
// and it routes each AgentResult back to its waiting dispatcher by request_id. The
// registration is torn down when the stream ends.
func (s *impl) Control(srv grpc.BidiStreamingServer[hostlinkv1.AgentEvent, hostlinkv1.Command]) (err error) {
	ctx := srv.Context()
	logger := s.logger.WithName("control")
	logger.Info("agent control stream opened")

	var conn *agentConn
	defer func() {
		if conn != nil {
			s.registry.remove(conn.agentID, conn)
			conn.closeAll()
			logger.Info("agent control stream deregistered", "agentID", conn.agentID)
		}
	}()

	for {
		var event *hostlinkv1.AgentEvent
		if event, err = srv.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				logger.Info("agent control stream closed by agent")
				err = nil
				return
			}
			if ctx.Err() != nil {
				logger.Info("agent control stream closed by controller", "reason", ctx.Err())
				err = nil
				return
			}
			logger.Error(err, "receive on agent control stream failed")
			return
		}

		eventLogger := logger.WithValues("agentID", event.GetAgentId())
		switch kind := event.GetKind().(type) {
		case *hostlinkv1.AgentEvent_Hello:
			eventLogger.Info("agent hello received", "token", kind.Hello.GetToken())
			if conn == nil {
				conn = newAgentConn(event.GetAgentId(), srv, s.logger.WithName("agentConn").WithValues("agentID", event.GetAgentId()))
				if replaced := s.registry.add(conn); replaced != nil {
					replaced.closeAll()
					eventLogger.Info("replaced existing agent connection")
				}
				eventLogger.Info("agent registered")
			}
		case *hostlinkv1.AgentEvent_Heartbeat:
			eventLogger.Info("agent heartbeat received")
			if conn != nil {
				s.registry.refresh(conn.agentID)
			}
		case *hostlinkv1.AgentEvent_Event:
			eventLogger.Info("agent docker event received",
				"type", kind.Event.GetType(),
				"containerID", kind.Event.GetContainerId(),
			)
		case *hostlinkv1.AgentEvent_Result:
			if conn == nil {
				eventLogger.Info("dropping agent result before registration", "requestID", kind.Result.GetRequestId())
				continue
			}
			conn.deliver(kind.Result)
		default:
			eventLogger.Info("agent event received with unknown kind")
		}
	}
}

// Forward is the helloworld-level placeholder for the port-forward byte pipe. The
// raw-TCP relay (DATA/HALF_CLOSE/RESET framing, backpressure) is implemented later.
func (s *impl) Forward(grpc.BidiStreamingServer[hostlinkv1.Frame, hostlinkv1.Frame]) error {
	return nil
}
