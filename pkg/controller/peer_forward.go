package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
	"github.com/humble-mun/hostlink/pkg/tunnel"
)

var errForwardReset = errors.New("controller: forward reset by remote")

// Forward establishes a peer-to-peer relay to an agent held by this controller.
func (s *peerServer) Forward(stream grpc.BidiStreamingServer[hostlinkv1.Frame, hostlinkv1.Frame]) error {
	first, err := receiveAndValidateOpenFrame(stream)
	if err != nil {
		return err
	}

	sessionID := first.GetSessionId()
	open := first.GetOpen()
	agentID := open.GetAgentId()
	conn, ok := s.registry.get(agentID)
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "agent %q not held by this controller", agentID)
	}
	waiter, cancelWaiter := s.sessions.expect(sessionID)
	defer cancelWaiter()
	if err := conn.send(&hostlinkv1.Command{Cmd: &hostlinkv1.Command_OpenForward{
		OpenForward: &hostlinkv1.OpenForward{SessionId: sessionID, Target: open.GetTarget()},
	}}); err != nil {
		if errors.Is(err, errAgentDisconnected) {
			return status.Errorf(codes.FailedPrecondition, "agent %q disconnected during forward setup", agentID)
		}
		return status.Errorf(codes.Internal, "open forward on agent %q: %v", agentID, err)
	}

	pairTimeout := s.pairTimeout
	if pairTimeout == 0 {
		pairTimeout = forwardPairTimeout
	}
	timer := time.NewTimer(pairTimeout)
	defer timer.Stop()
	var session *forwardSession
	select {
	case session = <-waiter:
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "timed out waiting for agent forward stream")
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	defer close(session.done)

	if session.first.GetType() == hostlinkv1.Frame_RESET {
		return stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_RESET})
	}
	if err := stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_READY}); err != nil {
		return fmt.Errorf("send peer forward ready: %w", err)
	}
	if err := tunnel.SpliceStream(stream, session.stream); err != nil {
		s.logger.V(1).Info("peer forward streams ended", "agentID", agentID, "sessionID", sessionID, "error", err)
	}
	return nil
}

func receiveAndValidateOpenFrame(stream grpc.BidiStreamingServer[hostlinkv1.Frame, hostlinkv1.Frame]) (*hostlinkv1.Frame, error) {
	firstResult := make(chan firstForwardFrame, 1)
	go func() {
		frame, err := stream.Recv()
		firstResult <- firstForwardFrame{frame: frame, err: err}
	}()

	timer := time.NewTimer(forwardFirstFrameTimeout)
	defer timer.Stop()
	var first *hostlinkv1.Frame
	select {
	case result := <-firstResult:
		if result.err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "receive opening forward frame: %v", result.err)
		}
		first = result.frame
	case <-timer.C:
		return nil, status.Error(codes.InvalidArgument, "timed out waiting for opening forward frame")
	case <-stream.Context().Done():
		return nil, stream.Context().Err()
	}

	if first.GetType() != hostlinkv1.Frame_OPEN {
		return nil, status.Errorf(codes.InvalidArgument, "first forward frame must be OPEN, got %s", first.GetType())
	}
	if first.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "opening forward frame is missing session ID")
	}
	open := first.GetOpen()
	if open == nil || open.GetAgentId() == "" || open.GetTarget() == "" {
		return nil, status.Error(codes.InvalidArgument, "opening forward frame is missing agent ID or target")
	}
	return first, nil
}

// forward opens a relay to the sibling at addr and waits for its READY barrier.
// The caller owns CloseSend after it has finished splicing the public connection.
func (p *peerClients) forward(ctx context.Context, addr, agentID, target, sessionID string) (stream grpc.BidiStreamingClient[hostlinkv1.Frame, hostlinkv1.Frame], err error) {
	var conn *grpc.ClientConn
	if conn, err = p.conn(addr); err != nil {
		return
	}
	if stream, err = hostlinkv1.NewControllerPeerClient(conn).Forward(ctx); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			err = errAgentNotConnected
		}
		return
	}
	ready := false
	defer func() {
		if ready {
			return
		}
		if closeErr := stream.CloseSend(); closeErr != nil {
			p.logger.V(1).Info("close peer forward stream after setup failure", "addr", addr, "error", closeErr)
		}
	}()

	if err = stream.Send(&hostlinkv1.Frame{
		SessionId: sessionID,
		Type:      hostlinkv1.Frame_OPEN,
		Open:      &hostlinkv1.PeerForwardOpen{AgentId: agentID, Target: target},
	}); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			err = errAgentNotConnected
		}
		return
	}
	var response *hostlinkv1.Frame
	if response, err = stream.Recv(); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			err = errAgentNotConnected
		}
		return
	}
	switch response.GetType() {
	case hostlinkv1.Frame_READY:
		ready = true
		return
	case hostlinkv1.Frame_RESET:
		err = errForwardReset
		return
	default:
		err = fmt.Errorf("unexpected peer forward readiness frame type %s", response.GetType())
		return
	}
}
