package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
	"github.com/humble-mun/hostlink/pkg/tunnel"
)

const forwardDialTimeout = 10 * time.Second

// handleOpenForward opens a reverse Forward stream for a pending public
// connection. The controller owns the stream session; it ends with the Control
// session context so forwards never outlive an agent reconnect.
func (a *agent) handleOpenForward(ctx context.Context, open *hostlinkv1.OpenForward) {
	sessionID := open.GetSessionId()
	target := open.GetTarget()
	logger := a.logger.WithName("forward").WithValues("sessionID", sessionID, "target", target)
	if sessionID == "" || target == "" {
		logger.Info("received open forward command with empty session ID or target")
		return
	}

	go func() {
		dialer := net.Dialer{Timeout: forwardDialTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", target)
		if err != nil {
			logger.Error(err, "dial forward target failed")
			a.reportForwardDialFailure(ctx, sessionID)
			return
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			logger.Error(fmt.Errorf("forward target connection is %T, want *net.TCPConn", conn), "dial forward target returned unexpected connection")
			if closeErr := conn.Close(); closeErr != nil {
				logger.Error(closeErr, "close forward target connection failed")
			}
			a.reportForwardDialFailure(ctx, sessionID)
			return
		}

		stream, err := a.client.Forward(ctx)
		if err != nil {
			logger.Error(err, "open forward stream failed")
			if closeErr := conn.Close(); closeErr != nil {
				logger.Error(closeErr, "close forward target connection failed")
			}
			return
		}
		if err := stream.Send(&hostlinkv1.Frame{SessionId: sessionID, Type: hostlinkv1.Frame_OPEN}); err != nil {
			logger.Error(err, "send forward open frame failed")
			if closeErr := conn.Close(); closeErr != nil {
				logger.Error(closeErr, "close forward target connection failed")
			}
			return
		}
		if err := tunnel.SpliceConn(tcpConn, stream); err != nil {
			logger.Error(err, "splice forward connection failed")
		}
		_ = stream.CloseSend()
	}()
}

// reportForwardDialFailure tells the controller to reset its pending public
// connection after a container-side dial fails.
func (a *agent) reportForwardDialFailure(ctx context.Context, sessionID string) {
	logger := a.logger.WithName("forward").WithValues("sessionID", sessionID)
	stream, err := a.client.Forward(ctx)
	if err != nil {
		logger.Error(err, "open forward reset stream failed")
		return
	}
	if err := stream.Send(&hostlinkv1.Frame{SessionId: sessionID, Type: hostlinkv1.Frame_RESET}); err != nil {
		logger.Error(err, "send forward reset frame failed")
	}
	if err := stream.CloseSend(); err != nil {
		logger.Error(err, "close forward reset stream send failed")
		return
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				logger.Error(err, "drain forward reset stream failed")
			}
			return
		}
	}
}
