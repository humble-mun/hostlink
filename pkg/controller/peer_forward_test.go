package controller

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

type peerForwardFixture struct {
	addr     string
	server   *peerServer
	sessions *sessionTable
	commands <-chan *hostlinkv1.Command
	clients  *peerClients
}

type peerForwardResult struct {
	stream grpc.BidiStreamingClient[hostlinkv1.Frame, hostlinkv1.Frame]
	err    error
}

func newPeerForwardFixture(t *testing.T, withAgent bool) *peerForwardFixture {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for peer gRPC server: %v", err)
	}
	sessions := newSessionTable()
	registry := newRegistry(logr.Discard(), nil, "")
	control := newControlStream(t.Context())
	if withAgent {
		registry.add(newAgentConn("agent-a", control, logr.Discard()))
	}
	server := &peerServer{
		logger:      logr.Discard(),
		registry:    registry,
		sessions:    sessions,
		pairTimeout: forwardPairTimeout,
	}
	grpcServer := grpc.NewServer()
	hostlinkv1.RegisterControllerPeerServer(grpcServer, server)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	clients := newPeerClients(logr.Discard(), insecure.NewCredentials(), "")
	t.Cleanup(func() {
		if err := clients.close(); err != nil {
			t.Errorf("close peer client: %v", err)
		}
		grpcServer.Stop()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve peer gRPC server: %v", err)
		}
	})

	return &peerForwardFixture{
		addr:     listener.Addr().String(),
		server:   server,
		sessions: sessions,
		commands: control.commands,
		clients:  clients,
	}
}

func TestPeerForwardRelaysFrames_whenAgentPairs(t *testing.T) {
	// Given
	fixture := newPeerForwardFixture(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), forwardHandlerTestTimeout)
	defer cancel()
	resultCh := make(chan peerForwardResult, 1)
	go func() {
		stream, err := fixture.clients.forward(ctx, fixture.addr, "agent-a", "172.30.1.5:8080", "session-a")
		resultCh <- peerForwardResult{stream: stream, err: err}
	}()
	command := waitForwardCommand(t, fixture.commands)
	agentStream := newForwardFrameStream()
	session := &forwardSession{
		stream: agentStream,
		first:  &hostlinkv1.Frame{SessionId: command.GetOpenForward().GetSessionId(), Type: hostlinkv1.Frame_OPEN},
		done:   make(chan struct{}),
	}
	if !fixture.sessions.deliver(command.GetOpenForward().GetSessionId(), session) {
		t.Fatal("deliver paired agent forward session = false, want true")
	}
	result := waitPeerForwardResult(t, resultCh)
	if result.err != nil {
		t.Fatalf("open peer forward: %v", result.err)
	}

	// When
	payload := []byte("forwarded payload")
	if err := result.stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_DATA, Data: payload}); err != nil {
		t.Fatalf("send peer data frame: %v", err)
	}
	forwarded := waitForwardFrame(t, agentStream.sent)
	agentStream.received <- &hostlinkv1.Frame{Type: hostlinkv1.Frame_DATA, Data: payload}
	echoed, err := result.stream.Recv()
	if err != nil {
		t.Fatalf("receive echoed peer data frame: %v", err)
	}
	if err := result.stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_HALF_CLOSE}); err != nil {
		t.Fatalf("send peer half-close frame: %v", err)
	}
	halfClosed := waitForwardFrame(t, agentStream.sent)
	agentStream.received <- &hostlinkv1.Frame{Type: hostlinkv1.Frame_HALF_CLOSE}
	peerHalfClosed, err := result.stream.Recv()
	if err != nil {
		t.Fatalf("receive peer half-close frame: %v", err)
	}
	close(agentStream.received)
	if err := result.stream.CloseSend(); err != nil {
		t.Fatalf("close peer forward send: %v", err)
	}
	_, err = result.stream.Recv()

	// Then
	if forwarded.GetType() != hostlinkv1.Frame_DATA || string(forwarded.GetData()) != string(payload) {
		t.Fatalf("agent received frame = %#v, want DATA %q", forwarded, payload)
	}
	if echoed.GetType() != hostlinkv1.Frame_DATA || string(echoed.GetData()) != string(payload) {
		t.Fatalf("peer received frame = %#v, want DATA %q", echoed, payload)
	}
	if halfClosed.GetType() != hostlinkv1.Frame_HALF_CLOSE || peerHalfClosed.GetType() != hostlinkv1.Frame_HALF_CLOSE {
		t.Fatalf("half-close frames = %#v, %#v, want HALF_CLOSE", halfClosed, peerHalfClosed)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("receive after both half-closes = %v, want EOF", err)
	}
	waitForwardDone(t, session.done)
}

func TestPeerForwardReturnsNotConnected_whenAgentIsNotHeld(t *testing.T) {
	// Given
	fixture := newPeerForwardFixture(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), forwardHandlerTestTimeout)
	defer cancel()

	// When
	_, err := fixture.clients.forward(ctx, fixture.addr, "agent-a", "172.30.1.5:8080", "session-a")

	// Then
	if !errors.Is(err, errAgentNotConnected) {
		t.Fatalf("forward error = %v, want errAgentNotConnected", err)
	}
}

func TestPeerForwardReturnsReset_whenAgentDialFails(t *testing.T) {
	// Given
	fixture := newPeerForwardFixture(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), forwardHandlerTestTimeout)
	defer cancel()
	resultCh := make(chan peerForwardResult, 1)
	go func() {
		stream, err := fixture.clients.forward(ctx, fixture.addr, "agent-a", "172.30.1.5:8080", "session-a")
		resultCh <- peerForwardResult{stream: stream, err: err}
	}()
	command := waitForwardCommand(t, fixture.commands)
	session := &forwardSession{
		first: &hostlinkv1.Frame{SessionId: command.GetOpenForward().GetSessionId(), Type: hostlinkv1.Frame_RESET},
		done:  make(chan struct{}),
	}
	if !fixture.sessions.deliver(command.GetOpenForward().GetSessionId(), session) {
		t.Fatal("deliver reset agent forward session = false, want true")
	}

	// When
	result := waitPeerForwardResult(t, resultCh)

	// Then
	if !errors.Is(result.err, errForwardReset) {
		t.Fatalf("forward error = %v, want errForwardReset", result.err)
	}
	waitForwardDone(t, session.done)
}

func TestPeerForwardRejectsFirstFrame_whenItIsNotOpen(t *testing.T) {
	// Given
	fixture := newPeerForwardFixture(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), forwardHandlerTestTimeout)
	defer cancel()
	conn, err := fixture.clients.conn(fixture.addr)
	if err != nil {
		t.Fatalf("dial peer test server: %v", err)
	}
	stream, err := hostlinkv1.NewControllerPeerClient(conn).Forward(ctx)
	if err != nil {
		t.Fatalf("open raw peer forward: %v", err)
	}

	// When
	if err := stream.Send(&hostlinkv1.Frame{SessionId: "session-a", Type: hostlinkv1.Frame_DATA}); err != nil {
		t.Fatalf("send invalid first frame: %v", err)
	}
	_, err = stream.Recv()

	// Then
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("first frame error code = %s, want InvalidArgument; error = %v", status.Code(err), err)
	}
}

func TestPeerForwardTimesOut_whenAgentDoesNotPair(t *testing.T) {
	// Given
	fixture := newPeerForwardFixture(t, true)
	fixture.server.pairTimeout = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), forwardHandlerTestTimeout)
	defer cancel()

	// When
	_, err := fixture.clients.forward(ctx, fixture.addr, "agent-a", "172.30.1.5:8080", "session-a")

	// Then
	if err == nil {
		t.Fatal("forward without agent pair = nil error, want deadline error")
	}
	if errors.Is(err, errAgentNotConnected) {
		t.Fatalf("forward without agent pair = %v, want a non-stale-holder error", err)
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("pair timeout error code = %s, want DeadlineExceeded; error = %v", status.Code(err), err)
	}
}

func TestPeerForwardRejectsOpen_whenPayloadIsMissing(t *testing.T) {
	// Given
	fixture := newPeerForwardFixture(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), forwardHandlerTestTimeout)
	defer cancel()
	conn, err := fixture.clients.conn(fixture.addr)
	if err != nil {
		t.Fatalf("dial peer test server: %v", err)
	}
	stream, err := hostlinkv1.NewControllerPeerClient(conn).Forward(ctx)
	if err != nil {
		t.Fatalf("open raw peer forward: %v", err)
	}

	// When
	if err := stream.Send(&hostlinkv1.Frame{SessionId: "session-a", Type: hostlinkv1.Frame_OPEN}); err != nil {
		t.Fatalf("send incomplete opening frame: %v", err)
	}
	_, err = stream.Recv()

	// Then
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("open payload error code = %s, want InvalidArgument; error = %v", status.Code(err), err)
	}
}

func waitPeerForwardResult(t *testing.T, results <-chan peerForwardResult) peerForwardResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(forwardHandlerTestTimeout):
		t.Fatal("timed out waiting for peer forward result")
		return peerForwardResult{}
	}
}
