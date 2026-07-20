package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

const (
	initialAcceptBackoff = 100 * time.Millisecond
	maximumAcceptBackoff = time.Second
)

// connHandler handles one accepted public connection on port p. It owns conn and
// must close it.
type connHandler func(ctx context.Context, port uint32, conn *net.TCPConn)

// listenerManager converges public TCP listeners to the ports exposed by this
// controller replica. Accepted connections outlive listener removal and are owned
// by the handler.
type listenerManager struct {
	logger  logr.Logger
	handler connHandler
	bindIP  string

	mu     sync.Mutex
	active map[uint32]*portListener

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// portListener is the running listener for one public port. Its accept loop is
// tracked by listenerManager.wg; connection handlers are deliberately not.
type portListener struct {
	listener net.Listener
}

func newListenerManager(ctx context.Context, logger logr.Logger, handler connHandler) *listenerManager {
	ctx, cancel := context.WithCancel(ctx)
	return &listenerManager{
		logger:  logger,
		handler: handler,
		active:  make(map[uint32]*portListener),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// reconcile converges the running listeners to desired. Bind failures are
// returned independently so another desired port never prevents convergence.
func (m *listenerManager) reconcile(desired map[uint32]struct{}) (bound []uint32, failed []uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx.Err() != nil {
		return nil, nil
	}

	for port, current := range m.active {
		if _, wanted := desired[port]; wanted {
			continue
		}
		if err := current.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			m.logger.Error(err, "close public port listener failed", "port", port)
			recordForwardFailure(forwardFailureListenerClose, port, "", "")
			continue
		}
		delete(m.active, port)
	}

	for port := range desired {
		if _, alreadyBound := m.active[port]; alreadyBound {
			continue
		}
		if port == 0 {
			err := errors.New("public port must be non-zero")
			m.logger.Error(err, "bind public port listener failed", "port", port)
			recordForwardFailure(forwardFailureListenerBind, port, "", "")
			failed = append(failed, port)
			continue
		}

		// A bind racing close() may still succeed despite a cancelled ctx;
		// close() reclaims it under m.mu, so nothing leaks.
		var lc net.ListenConfig
		listener, err := lc.Listen(m.ctx, "tcp", net.JoinHostPort(m.bindIP, strconv.FormatUint(uint64(port), 10)))
		if err != nil {
			m.logger.Error(err, "bind public port listener failed", "port", port)
			recordForwardFailure(forwardFailureListenerBind, port, "", "")
			failed = append(failed, port)
			continue
		}

		current := &portListener{listener: listener}
		m.active[port] = current
		m.wg.Add(1)
		go m.accept(current, port)
	}

	return m.boundPortsLocked(), sortedPorts(failed)
}

// boundPorts returns the currently-bound public ports in ascending order.
func (m *listenerManager) boundPorts() []uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.boundPortsLocked()
}

func (m *listenerManager) boundPortsLocked() []uint32 {
	ports := make([]uint32, 0, len(m.active))
	for port := range m.active {
		ports = append(ports, port)
	}
	return sortedPorts(ports)
}

func (m *listenerManager) accept(current *portListener, port uint32) {
	defer m.wg.Done()

	backoff := initialAcceptBackoff
	for {
		conn, err := current.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			m.logger.Error(err, "accept public connection failed", "port", port)
			recordForwardFailure(forwardFailureAccept, port, "", "")
			if !m.waitAcceptRetry(backoff) {
				return
			}
			backoff = nextAcceptBackoff(backoff)
			continue
		}
		backoff = initialAcceptBackoff

		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			m.rejectNonTCPConnection(conn, port)
			continue
		}
		go m.handler(m.ctx, port, tcpConn)
	}
}

func (m *listenerManager) waitAcceptRetry(backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func nextAcceptBackoff(backoff time.Duration) time.Duration {
	if backoff >= maximumAcceptBackoff/2 {
		return maximumAcceptBackoff
	}
	return backoff * 2
}

func (m *listenerManager) rejectNonTCPConnection(conn net.Conn, port uint32) {
	m.logger.Error(fmt.Errorf("accepted connection has type %T", conn), "reject non-TCP public connection", "port", port)
	if err := conn.Close(); err != nil {
		m.logger.Error(err, "close non-TCP public connection failed", "port", port)
	}
}

// close stops all listeners and waits for their accept loops. It deliberately
// does not wait for handlers, which own their accepted connections.
func (m *listenerManager) close() error {
	m.closeOnce.Do(func() {
		m.cancel()

		m.mu.Lock()
		var errs []error
		for port, current := range m.active {
			if err := current.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, fmt.Errorf("close public port listener %d: %w", port, err))
			}
			delete(m.active, port)
		}
		m.mu.Unlock()

		m.wg.Wait()
		m.closeErr = errors.Join(errs...)
	})
	return m.closeErr
}

func sortedPorts(ports []uint32) []uint32 {
	sort.Slice(ports, func(i, j int) bool { return ports[i] < ports[j] })
	return ports
}
