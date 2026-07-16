package controller

import (
	"errors"
	"io"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	sessions *sessionTable
	store    portStore
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

		conn = s.handleAgentEvent(conn, srv, event, logger)
	}
}

// handleAgentEvent processes a single inbound AgentEvent and returns the
// (possibly newly created) agent connection. Splitting this out of Control keeps
// the receive loop's cyclomatic complexity in check.
func (s *impl) handleAgentEvent(conn *agentConn, srv grpc.BidiStreamingServer[hostlinkv1.AgentEvent, hostlinkv1.Command], event *hostlinkv1.AgentEvent, logger logr.Logger) *agentConn {
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
		eventLogger.V(1).Info("agent docker event received",
			"type", kind.Event.GetType(),
			"containerID", kind.Event.GetContainerId(),
		)
		s.handleContainerEvent(conn, kind.Event)
	case *hostlinkv1.AgentEvent_Result:
		if conn == nil {
			eventLogger.Info("dropping agent result before registration", "requestID", kind.Result.GetRequestId())
			return conn
		}
		conn.deliver(kind.Result)
	case *hostlinkv1.AgentEvent_Progress:
		if conn == nil {
			eventLogger.Info("dropping agent progress before registration", "requestID", kind.Progress.GetRequestId())
			return conn
		}
		conn.deliverProgress(kind.Progress)
	default:
		eventLogger.Info("agent event received with unknown kind")
	}
	return conn
}

const forwardFirstFrameTimeout = 30 * time.Second

type firstForwardFrame struct {
	frame *hostlinkv1.Frame
	err   error
}

// Forward pairs an agent-opened Forward RPC to the waiting public connection
// handler. The consumer owns forwardSession.done and must close it when the
// byte-pipe relay finishes.
func (s *impl) Forward(srv grpc.BidiStreamingServer[hostlinkv1.Frame, hostlinkv1.Frame]) error {
	firstResult := make(chan firstForwardFrame, 1)
	go func() {
		frame, err := srv.Recv()
		firstResult <- firstForwardFrame{frame: frame, err: err}
	}()

	timer := time.NewTimer(forwardFirstFrameTimeout)
	defer timer.Stop()
	var first *hostlinkv1.Frame
	select {
	case result := <-firstResult:
		if result.err != nil {
			return status.Errorf(codes.InvalidArgument, "receive first Forward frame: %v", result.err)
		}
		first = result.frame
	case <-timer.C:
		return status.Error(codes.InvalidArgument, "timed out waiting for first Forward frame")
	case <-srv.Context().Done():
		return srv.Context().Err()
	}

	sessionID := first.GetSessionId()
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "first Forward frame is missing session ID")
	}
	switch first.GetType() {
	case hostlinkv1.Frame_OPEN, hostlinkv1.Frame_RESET:
	default:
		return status.Errorf(codes.InvalidArgument, "invalid first Forward frame type %s", first.GetType())
	}

	session := &forwardSession{stream: srv, first: first, done: make(chan struct{})}
	if !s.sessions.deliver(sessionID, session) {
		s.logger.WithName("forward").Info("rejecting unmatched forward session", "sessionID", sessionID)
		return status.Error(codes.InvalidArgument, "unknown Forward session")
	}

	select {
	case <-session.done:
		return nil
	case <-srv.Context().Done():
		return srv.Context().Err()
	}
}
