package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

const listenerTestTimeout = 2 * time.Second

type listenerResult struct {
	port uint32
	err  error
}

func TestListenerReconcileBindsAndAccepts(t *testing.T) {
	// Given
	results := make(chan listenerResult, 2)
	manager := newTestListenerManager(t, echoHandler(results))
	ports := freePorts(t, 2)

	// When
	bound, failed := manager.reconcile(portSet(ports...))

	// Then
	wantPorts(t, bound, ports)
	wantPorts(t, failed, nil)
	wantPorts(t, manager.boundPorts(), ports)

	for _, port := range ports {
		conn := dialListener(t, port)
		if _, err := conn.Write([]byte{byte(port)}); err != nil {
			_ = conn.Close()
			t.Fatalf("write to port %d: %v", port, err)
		}

		var got [1]byte
		if _, err := io.ReadFull(conn, got[:]); err != nil {
			_ = conn.Close()
			t.Fatalf("read echo from port %d: %v", port, err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close client on port %d: %v", port, err)
		}
		if got[0] != byte(port) {
			t.Fatalf("echo from port %d = %d, want %d", port, got[0], byte(port))
		}
		if result := receiveListenerResult(t, results); result.port != port {
			t.Fatalf("handler port = %d, want %d", result.port, port)
		}
	}
}

func TestListenerReconcileRemoves(t *testing.T) {
	// Given
	manager := newTestListenerManager(t, closeHandler)
	port := freePorts(t, 1)[0]
	manager.reconcile(portSet(port))

	// When
	bound, failed := manager.reconcile(map[uint32]struct{}{})

	// Then
	wantPorts(t, bound, nil)
	wantPorts(t, failed, nil)
	wantPorts(t, manager.boundPorts(), nil)
	if conn, err := net.DialTimeout("tcp", listenerAddress(port), listenerTestTimeout); err == nil {
		_ = conn.Close()
		t.Fatalf("dial removed port %d succeeded", port)
	}
}

func TestListenerReconcileKeepsExisting(t *testing.T) {
	// Given
	accepted := make(chan struct{}, 1)
	release := make(chan struct{})
	manager := newTestListenerManager(t, func(_ context.Context, _ uint32, conn *net.TCPConn) {
		accepted <- struct{}{}
		<-release
		_ = conn.Close()
	})
	ports := freePorts(t, 2)
	firstPort, secondPort := ports[0], ports[1]
	manager.reconcile(portSet(firstPort))
	client := dialListener(t, firstPort)
	t.Cleanup(func() {
		close(release)
		_ = client.Close()
	})
	waitForListener(t, accepted)
	firstListener := manager.active[firstPort]

	// When
	bound, failed := manager.reconcile(portSet(firstPort, secondPort))

	// Then
	wantPorts(t, bound, []uint32{firstPort, secondPort})
	wantPorts(t, failed, nil)
	if manager.active[firstPort] != firstListener {
		t.Fatal("existing listener was rebound")
	}
	if _, err := client.Write([]byte{1}); err != nil {
		t.Fatalf("write through existing connection: %v", err)
	}
}

func TestListenerBindFailure(t *testing.T) {
	// Given
	manager := newTestListenerManager(t, closeHandler)
	port := freePorts(t, 1)[0]
	blocker, err := net.Listen("tcp", listenerAddress(port))
	if err != nil {
		t.Fatalf("bind blocker: %v", err)
	}
	failures := forwardFailureCount(t, forwardFailureListenerBind, port, "", "")

	// When
	bound, failed := manager.reconcile(portSet(port))

	// Then
	wantPorts(t, bound, nil)
	wantPorts(t, failed, []uint32{port})
	wantPorts(t, manager.boundPorts(), nil)
	assertForwardFailureDelta(t, forwardFailureListenerBind, port, "", "", failures, 1)
	if err := blocker.Close(); err != nil {
		t.Fatalf("close blocker: %v", err)
	}

	bound, failed = manager.reconcile(portSet(port))
	wantPorts(t, bound, []uint32{port})
	wantPorts(t, failed, nil)
	assertForwardFailureDelta(t, forwardFailureListenerBind, port, "", "", failures, 1)
}

func TestListenerAcceptFailureCounted(t *testing.T) {
	// Given
	manager := newTestListenerManager(t, closeHandler)
	failures := forwardFailureCount(t, forwardFailureAccept, 41100, "", "")

	// When: one transient accept error, then shutdown via net.ErrClosed.
	manager.wg.Add(1)
	manager.accept(&portListener{listener: &failingListener{errs: []error{errors.New("accept boom")}}}, 41100)

	// Then
	assertForwardFailureDelta(t, forwardFailureAccept, 41100, "", "", failures, 1)
}

// failingListener returns its queued errors from Accept, then net.ErrClosed so
// the accept loop shuts down cleanly.
type failingListener struct {
	errs []error
}

func (l *failingListener) Accept() (net.Conn, error) {
	if len(l.errs) == 0 {
		return nil, net.ErrClosed
	}
	err := l.errs[0]
	l.errs = l.errs[1:]
	return nil, err
}

func (l *failingListener) Close() error   { return nil }
func (l *failingListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

func TestListenerInFlightSurvivesRemoval(t *testing.T) {
	// Given
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	results := make(chan listenerResult, 1)
	manager := newTestListenerManager(t, func(_ context.Context, port uint32, conn *net.TCPConn) {
		defer func() { _ = conn.Close() }()
		started <- struct{}{}
		<-release

		var data [1]byte
		if _, err := io.ReadFull(conn, data[:]); err != nil {
			results <- listenerResult{port: port, err: fmt.Errorf("read held connection: %w", err)}
			return
		}
		if _, err := conn.Write(data[:]); err != nil {
			results <- listenerResult{port: port, err: fmt.Errorf("write held connection: %w", err)}
			return
		}
		results <- listenerResult{port: port}
	})
	port := freePorts(t, 1)[0]
	manager.reconcile(portSet(port))
	client := dialListener(t, port)
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		_ = client.Close()
	})
	waitForListener(t, started)

	// When
	manager.reconcile(map[uint32]struct{}{})
	if _, err := client.Write([]byte{42}); err != nil {
		t.Fatalf("write after listener removal: %v", err)
	}
	close(release)
	released = true

	// Then
	var got [1]byte
	if _, err := io.ReadFull(client, got[:]); err != nil {
		t.Fatalf("read after listener removal: %v", err)
	}
	if got[0] != 42 {
		t.Fatalf("reply after listener removal = %d, want 42", got[0])
	}
	if result := receiveListenerResult(t, results); result.port != port {
		t.Fatalf("handler port = %d, want %d", result.port, port)
	}
}

func TestListenerClose(t *testing.T) {
	// Given
	manager := newTestListenerManager(t, closeHandler)
	port := freePorts(t, 1)[0]
	manager.reconcile(portSet(port))

	// When
	if err := manager.close(); err != nil {
		t.Fatalf("close listener manager: %v", err)
	}

	// Then
	wantPorts(t, manager.boundPorts(), nil)
	if conn, err := net.DialTimeout("tcp", listenerAddress(port), listenerTestTimeout); err == nil {
		_ = conn.Close()
		t.Fatalf("dial closed port %d succeeded", port)
	}
	if err := manager.close(); err != nil {
		t.Fatalf("close listener manager twice: %v", err)
	}
	bound, failed := manager.reconcile(portSet(port))
	wantPorts(t, bound, nil)
	wantPorts(t, failed, nil)
}

func echoHandler(results chan<- listenerResult) connHandler {
	return func(_ context.Context, port uint32, conn *net.TCPConn) {
		defer func() { _ = conn.Close() }()

		var data [1]byte
		if _, err := io.ReadFull(conn, data[:]); err != nil {
			results <- listenerResult{port: port, err: fmt.Errorf("read accepted connection: %w", err)}
			return
		}
		if _, err := conn.Write(data[:]); err != nil {
			results <- listenerResult{port: port, err: fmt.Errorf("write accepted connection: %w", err)}
			return
		}
		results <- listenerResult{port: port}
	}
}

func closeHandler(_ context.Context, _ uint32, conn *net.TCPConn) {
	_ = conn.Close()
}

func newTestListenerManager(t *testing.T, handler connHandler) *listenerManager {
	t.Helper()
	manager := newListenerManager(t.Context(), logr.Discard(), handler)
	manager.bindIP = "127.0.0.1"
	t.Cleanup(func() {
		if err := manager.close(); err != nil {
			t.Errorf("close listener manager: %v", err)
		}
	})
	return manager
}

func freePorts(t *testing.T, count int) []uint32 {
	t.Helper()
	ports := make([]uint32, 0, count)
	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("find free port: %v", err)
		}
		port := uint32(listener.Addr().(*net.TCPAddr).Port)
		if err := listener.Close(); err != nil {
			t.Fatalf("release free port %d: %v", port, err)
		}
		ports = append(ports, port)
	}
	return ports
}

func portSet(ports ...uint32) map[uint32]struct{} {
	set := make(map[uint32]struct{}, len(ports))
	for _, port := range ports {
		set[port] = struct{}{}
	}
	return set
}

func dialListener(t *testing.T, port uint32) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", listenerAddress(port), listenerTestTimeout)
	if err != nil {
		t.Fatalf("dial port %d: %v", port, err)
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		t.Fatalf("dial port %d returned %T, want *net.TCPConn", port, conn)
	}
	if err := tcpConn.SetDeadline(time.Now().Add(listenerTestTimeout)); err != nil {
		_ = tcpConn.Close()
		t.Fatalf("set deadline for port %d: %v", port, err)
	}
	return tcpConn
}

func listenerAddress(port uint32) string {
	return net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", port))
}

func waitForListener(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(listenerTestTimeout):
		t.Fatal("listener handler did not start")
	}
}

func receiveListenerResult(t *testing.T, results <-chan listenerResult) listenerResult {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result
	case <-time.After(listenerTestTimeout):
		t.Fatal("listener handler did not finish")
		return listenerResult{}
	}
}

func wantPorts(t *testing.T, got, want []uint32) {
	t.Helper()
	got = slices.Clone(got)
	want = slices.Clone(want)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("ports = %v, want %v", got, want)
	}
}
