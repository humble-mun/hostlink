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
}

// Control is the helloworld-level implementation of the bidirectional command
// stream. It reads the agent's handshake and subsequent events, logging them to
// prove end-to-end connectivity. Command push and business logic are added later.
func (s *impl) Control(srv grpc.BidiStreamingServer[hostlinkv1.AgentEvent, hostlinkv1.Command]) error {
	ctx := srv.Context()
	logger := s.logger.WithName("control")
	logger.Info("agent control stream opened")

	for {
		event, err := srv.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				logger.Info("agent control stream closed by agent")
				return nil
			}
			if ctx.Err() != nil {
				logger.Info("agent control stream closed by controller", "reason", ctx.Err())
				return nil
			}
			logger.Error(err, "receive on agent control stream failed")
			return err
		}

		eventLogger := logger.WithValues("agentID", event.GetAgentId())
		switch kind := event.GetKind().(type) {
		case *hostlinkv1.AgentEvent_Hello:
			eventLogger.Info("agent hello received", "token", kind.Hello.GetToken())
		case *hostlinkv1.AgentEvent_Heartbeat:
			eventLogger.Info("agent heartbeat received")
		case *hostlinkv1.AgentEvent_Event:
			eventLogger.Info("agent docker event received",
				"type", kind.Event.GetType(),
				"containerID", kind.Event.GetContainerId(),
			)
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
