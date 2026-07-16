package controller

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/go-logr/logr"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
	"github.com/humble-mun/hostlink/pkg/tunnel"
)

const forwardPairTimeout = 30 * time.Second

// forwarder pairs an accepted public connection with the agent Forward stream
// opened in response to its OpenForward command.
type forwarder struct {
	logger      logr.Logger
	registry    *registry
	sessions    *sessionTable
	store       portStore
	peers       *peerClients
	selfAddr    string
	pairTimeout time.Duration
}

func newForwarder(logger logr.Logger, registry *registry, sessions *sessionTable, store portStore, peers *peerClients, selfAddr string) *forwarder {
	return &forwarder{
		logger:      logger.WithName("forward"),
		registry:    registry,
		sessions:    sessions,
		store:       store,
		peers:       peers,
		selfAddr:    selfAddr,
		pairTimeout: forwardPairTimeout,
	}
}

// handleConn owns conn. It does not read any public bytes until the agent stream
// has paired, preserving retry-before-read semantics for a later cross-pod hop.
func (f *forwarder) handleConn(ctx context.Context, port uint32, conn *net.TCPConn) {
	handed := false
	closed := false
	defer func() {
		if handed || closed {
			return
		}
		if err := conn.Close(); err != nil {
			f.logger.Error(err, "close public connection failed", "port", port)
		}
	}()
	abort := func() {
		closed = true
		if err := rstClose(conn); err != nil {
			f.logger.Error(err, "reset public connection failed", "port", port)
		}
	}

	lookupCtx, cancel := context.WithTimeout(ctx, redisOpTimeout)
	mapping, err := f.store.lookup(lookupCtx, port)
	cancel()
	if err != nil {
		if errors.Is(err, errPortNotFound) {
			f.logger.Info("public connection has no port mapping", "port", port)
		} else {
			f.logger.Error(err, "lookup public port mapping failed", "port", port)
		}
		abort()
		return
	}
	if mapping.Suspended {
		f.logger.Info("public connection has suspended port mapping", "port", port, "agentID", mapping.AgentID)
		abort()
		return
	}

	agent, local := f.registry.get(mapping.AgentID)
	if !local {
		f.handleRemote(ctx, port, conn, mapping)
		closed = true
		return
	}
	f.handleLocal(ctx, port, conn, agent, mapping)
	closed = true
}

func (f *forwarder) handleLocal(ctx context.Context, port uint32, conn *net.TCPConn, agent *agentConn, mapping portMapping) {
	abort := func() {
		if err := rstClose(conn); err != nil {
			f.logger.Error(err, "reset public connection failed", "port", port)
		}
	}
	sessionID, err := newSessionID()
	if err != nil {
		f.logger.Error(err, "generate forward session ID failed", "port", port, "agentID", mapping.AgentID)
		abort()
		return
	}
	waiter, cancelWaiter := f.sessions.expect(sessionID)
	defer cancelWaiter()

	command := &hostlinkv1.Command{Cmd: &hostlinkv1.Command_OpenForward{
		OpenForward: &hostlinkv1.OpenForward{SessionId: sessionID, Target: mapping.Target},
	}}
	if err := agent.send(command); err != nil {
		f.logger.Error(err, "send open forward command failed", "port", port, "agentID", mapping.AgentID)
		abort()
		return
	}

	timer := time.NewTimer(f.pairTimeout)
	defer timer.Stop()
	select {
	case session := <-waiter:
		if session.first.GetType() == hostlinkv1.Frame_RESET {
			f.logger.Info("agent rejected forward connection", "port", port, "agentID", mapping.AgentID)
			close(session.done)
			abort()
			return
		}
		if err := tunnel.SpliceConn(conn, session.stream); err != nil {
			f.logger.Error(err, "splice public connection failed", "port", port, "agentID", mapping.AgentID)
		}
		close(session.done)
	case <-timer.C:
		f.logger.Info("timed out waiting for agent forward stream", "port", port, "agentID", mapping.AgentID)
		abort()
	case <-ctx.Done():
		f.logger.Info("public connection canceled before forward pairing", "port", port, "agentID", mapping.AgentID)
		abort()
	}
}

func (f *forwarder) handleRemote(ctx context.Context, port uint32, conn *net.TCPConn, mapping portMapping) {
	abort := func() {
		if err := rstClose(conn); err != nil {
			f.logger.Error(err, "reset public connection failed", "port", port)
		}
	}
	if f.peers == nil {
		f.logger.V(0).Info("cross-pod forwarding disabled", "port", port, "agentID", mapping.AgentID)
		abort()
		return
	}

	for attempt := range 2 {
		locateCtx, cancel := context.WithTimeout(ctx, redisOpTimeout)
		addr, err := f.registry.locate(locateCtx, mapping.AgentID)
		cancel()
		if err != nil {
			f.logger.Error(err, "locate remote agent holder failed", "port", port, "agentID", mapping.AgentID)
			abort()
			return
		}
		if addr == "" {
			f.logger.Info("remote agent holder is unavailable", "port", port, "agentID", mapping.AgentID)
			abort()
			return
		}
		if addr == f.selfAddr {
			f.logger.Info("remote agent holder points to this controller", "port", port, "agentID", mapping.AgentID)
			abort()
			return
		}

		sessionID, err := newSessionID()
		if err != nil {
			f.logger.Error(err, "generate remote forward session ID failed", "port", port, "agentID", mapping.AgentID)
			abort()
			return
		}
		stream, err := f.peers.forward(ctx, addr, mapping.AgentID, mapping.Target, sessionID)
		if err != nil {
			if errors.Is(err, errAgentNotConnected) && attempt == 0 {
				f.logger.V(1).Info("remote agent holder was stale; retrying", "port", port, "agentID", mapping.AgentID)
				continue
			}
			f.logger.Error(err, "open remote forward failed", "port", port, "agentID", mapping.AgentID)
			abort()
			return
		}

		if err := tunnel.SpliceConn(conn, stream); err != nil {
			f.logger.V(1).Info("splice remote public connection failed", "port", port, "agentID", mapping.AgentID, "error", err)
		}
		if err := stream.CloseSend(); err != nil {
			f.logger.V(1).Info("close remote forward stream send failed", "port", port, "agentID", mapping.AgentID, "error", err)
		}
		return
	}
}

func rstClose(conn *net.TCPConn) error {
	lingerErr := conn.SetLinger(0)
	closeErr := conn.Close()
	return errors.Join(lingerErr, closeErr)
}

func runPortReconciler(ctx context.Context, logger logr.Logger, store portStore, listeners *listenerManager, bindings *bindingTracker) {
	changes, stop := store.watch()
	defer stop()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		lookupCtx, cancel := context.WithTimeout(ctx, redisOpTimeout)
		desired, err := store.desired(lookupCtx)
		cancel()
		if err != nil {
			logger.Error(err, "list desired public forward ports failed")
		} else {
			ports := make(map[uint32]struct{}, len(desired))
			for port := range desired {
				ports[port] = struct{}{}
			}
			bound, failed := listeners.reconcile(ports)
			if len(failed) != 0 {
				logger.V(0).Info("some public forward listeners failed to bind", "ports", failed)
			}
			logger.V(1).Info("public forward listeners reconciled", "ports", bound)
		}
		bindings.reportBound(ctx)

		select {
		case <-ctx.Done():
			return
		case <-changes:
		case <-ticker.C:
		}
	}
}

var _ connHandler = (*forwarder)(nil).handleConn
