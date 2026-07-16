package controller

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/metadata"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

const forwardHandlerTestTimeout = 2 * time.Second

func TestForwardHandlerUnknownPort(t *testing.T) {
	// Given
	forwarder, commands := newTestForwarder(t, false)
	client, accepted := newForwardTCPPair(t)
	setForwardDeadline(t, client)

	// When
	go forwarder.handleConn(context.Background(), 41001, accepted)
	_, err := client.Read(make([]byte, 1))

	// Then
	if err == nil {
		t.Fatal("read from unknown port = nil, want connection close")
	}
	assertNoForwardCommand(t, commands)
}

func TestForwardHandlerAgentNotLocal(t *testing.T) {
	// Given
	forwarder, commands := newTestForwarder(t, false)
	const port = 41002
	allocateTestPort(t, forwarder.store, port, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})
	client, accepted := newForwardTCPPair(t)
	setForwardDeadline(t, client)

	// When
	go forwarder.handleConn(context.Background(), port, accepted)
	_, err := client.Read(make([]byte, 1))

	// Then
	if err == nil {
		t.Fatal("read for non-local agent = nil, want connection close")
	}
	assertNoForwardCommand(t, commands)
}

func TestForwardHandlerPairAndSplice(t *testing.T) {
	// Given
	forwarder, commands := newTestForwarder(t, true)
	const port = 41003
	const target = "172.30.1.5:8080"
	allocateTestPort(t, forwarder.store, port, portMapping{AgentID: "agent-a", Target: target})
	client, accepted := newForwardTCPPair(t)
	setForwardDeadline(t, client)
	handlerDone := make(chan struct{})
	go func() {
		forwarder.handleConn(context.Background(), port, accepted)
		close(handlerDone)
	}()
	command := waitForwardCommand(t, commands)
	open := command.GetOpenForward()
	if open == nil {
		t.Fatalf("command = %T, want OpenForward", command.GetCmd())
	}
	if open.GetTarget() != target {
		t.Fatalf("OpenForward target = %q, want %q", open.GetTarget(), target)
	}
	if open.GetSessionId() == "" {
		t.Fatal("OpenForward session ID is empty")
	}
	stream := newForwardFrameStream()
	session := &forwardSession{
		stream: stream,
		first:  &hostlinkv1.Frame{SessionId: open.GetSessionId(), Type: hostlinkv1.Frame_OPEN},
		done:   make(chan struct{}),
	}
	if !forwarder.sessions.deliver(open.GetSessionId(), session) {
		t.Fatal("deliver forward session = false, want true")
	}

	// When
	payload := []byte("forwarded payload")
	if _, err := client.Write(payload); err != nil {
		t.Fatalf("write public payload: %v", err)
	}
	frame := waitForwardFrame(t, stream.sent)
	if frame.GetType() != hostlinkv1.Frame_DATA || string(frame.GetData()) != string(payload) {
		t.Fatalf("agent frame = %#v, want DATA %q", frame, payload)
	}
	stream.received <- &hostlinkv1.Frame{Type: hostlinkv1.Frame_DATA, Data: payload}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("read echoed payload: %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("half-close public client: %v", err)
	}
	halfClose := waitForwardFrame(t, stream.sent)
	stream.received <- &hostlinkv1.Frame{Type: hostlinkv1.Frame_HALF_CLOSE}
	_, err := client.Read(make([]byte, 1))

	// Then
	if string(got) != string(payload) {
		t.Fatalf("echoed payload = %q, want %q", got, payload)
	}
	if halfClose.GetType() != hostlinkv1.Frame_HALF_CLOSE {
		t.Fatalf("agent frame type = %s, want HALF_CLOSE", halfClose.GetType())
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read after agent half-close = %v, want EOF", err)
	}
	waitForwardDone(t, session.done)
	waitHandlerDone(t, handlerDone)
}

func TestForwardHandlerAgentDialFailed(t *testing.T) {
	// Given
	forwarder, commands := newTestForwarder(t, true)
	const port = 41004
	allocateTestPort(t, forwarder.store, port, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})
	client, accepted := newForwardTCPPair(t)
	setForwardDeadline(t, client)
	handlerDone := make(chan struct{})
	go func() {
		forwarder.handleConn(context.Background(), port, accepted)
		close(handlerDone)
	}()
	command := waitForwardCommand(t, commands)
	session := &forwardSession{
		first: &hostlinkv1.Frame{SessionId: command.GetOpenForward().GetSessionId(), Type: hostlinkv1.Frame_RESET},
		done:  make(chan struct{}),
	}
	if !forwarder.sessions.deliver(command.GetOpenForward().GetSessionId(), session) {
		t.Fatal("deliver reset session = false, want true")
	}

	// When
	_, err := client.Read(make([]byte, 1))

	// Then
	if err == nil {
		t.Fatal("read after agent reset = nil, want connection close")
	}
	waitForwardDone(t, session.done)
	waitHandlerDone(t, handlerDone)
}

func TestForwardHandlerPairTimeout(t *testing.T) {
	// Given
	forwarder, commands := newTestForwarder(t, true)
	forwarder.pairTimeout = 50 * time.Millisecond
	const port = 41005
	allocateTestPort(t, forwarder.store, port, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})
	client, accepted := newForwardTCPPair(t)
	setForwardDeadline(t, client)
	handlerDone := make(chan struct{})
	go func() {
		forwarder.handleConn(context.Background(), port, accepted)
		close(handlerDone)
	}()
	command := waitForwardCommand(t, commands)

	// When
	_, err := client.Read(make([]byte, 1))

	// Then
	if err == nil {
		t.Fatal("read after pairing timeout = nil, want connection close")
	}
	waitHandlerDone(t, handlerDone)
	if forwarder.sessions.deliver(command.GetOpenForward().GetSessionId(), &forwardSession{}) {
		t.Fatal("deliver after pairing timeout = true, want false")
	}
}

func newTestForwarder(t *testing.T, localAgent bool) (*forwarder, <-chan *hostlinkv1.Command) {
	t.Helper()
	stream := newControlStream(context.Background())
	registry := newRegistry(logr.Discard(), nil, "")
	if localAgent {
		registry.add(newAgentConn("agent-a", stream, logr.Discard()))
	}
	return newForwarder(logr.Discard(), registry, newSessionTable(), newPortStore(logr.Discard(), nil)), stream.commands
}

func allocateTestPort(t *testing.T, store portStore, port uint32, mapping portMapping) {
	t.Helper()
	allocated, err := store.allocate(context.Background(), port, port, mapping)
	if err != nil {
		t.Fatalf("allocate public port %d: %v", port, err)
	}
	if allocated != port {
		t.Fatalf("allocated public port = %d, want %d", allocated, port)
	}
}

func newForwardTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen for public connection: %v", err)
	}
	accepted := make(chan *net.TCPConn, 1)
	go func() {
		conn, acceptErr := listener.AcceptTCP()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		_ = listener.Close()
		t.Fatalf("dial public connection: %v", err)
	}
	server := <-accepted
	if err := listener.Close(); err != nil {
		t.Fatalf("close public listener: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

func setForwardDeadline(t *testing.T, conn *net.TCPConn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(forwardHandlerTestTimeout)); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}
}

func waitForwardCommand(t *testing.T, commands <-chan *hostlinkv1.Command) *hostlinkv1.Command {
	t.Helper()
	select {
	case command := <-commands:
		return command
	case <-time.After(forwardHandlerTestTimeout):
		t.Fatal("timed out waiting for OpenForward command")
		return nil
	}
}

func assertNoForwardCommand(t *testing.T, commands <-chan *hostlinkv1.Command) {
	t.Helper()
	select {
	case command := <-commands:
		t.Fatalf("unexpected command: %#v", command)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitForwardFrame(t *testing.T, frames <-chan *hostlinkv1.Frame) *hostlinkv1.Frame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(forwardHandlerTestTimeout):
		t.Fatal("timed out waiting for forwarded frame")
		return nil
	}
}

func waitForwardDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(forwardHandlerTestTimeout):
		t.Fatal("timed out waiting for forward session completion")
	}
}

func waitHandlerDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(forwardHandlerTestTimeout):
		t.Fatal("timed out waiting for forward handler completion")
	}
}

type controlStream struct {
	ctx      context.Context
	commands chan *hostlinkv1.Command
}

func newControlStream(ctx context.Context) *controlStream {
	return &controlStream{ctx: ctx, commands: make(chan *hostlinkv1.Command, 1)}
}

func (s *controlStream) SetHeader(metadata.MD) error  { return nil }
func (s *controlStream) SendHeader(metadata.MD) error { return nil }
func (s *controlStream) SetTrailer(metadata.MD)       {}
func (s *controlStream) Context() context.Context     { return s.ctx }
func (s *controlStream) SendMsg(any) error            { return nil }
func (s *controlStream) RecvMsg(any) error {
	<-s.ctx.Done()
	return s.ctx.Err()
}
func (s *controlStream) Send(command *hostlinkv1.Command) error {
	s.commands <- command
	return nil
}
func (s *controlStream) Recv() (*hostlinkv1.AgentEvent, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

type forwardFrameStream struct {
	sent     chan *hostlinkv1.Frame
	received chan *hostlinkv1.Frame
}

func newForwardFrameStream() *forwardFrameStream {
	return &forwardFrameStream{
		sent:     make(chan *hostlinkv1.Frame, 4),
		received: make(chan *hostlinkv1.Frame, 4),
	}
}

func (s *forwardFrameStream) Send(frame *hostlinkv1.Frame) error {
	s.sent <- frame
	return nil
}

func (s *forwardFrameStream) Recv() (*hostlinkv1.Frame, error) {
	frame, ok := <-s.received
	if !ok {
		return nil, io.EOF
	}
	return frame, nil
}

var _ interface {
	SetHeader(metadata.MD) error
	SendHeader(metadata.MD) error
	SetTrailer(metadata.MD)
	Context() context.Context
	SendMsg(any) error
	RecvMsg(any) error
	Send(*hostlinkv1.Command) error
	Recv() (*hostlinkv1.AgentEvent, error)
} = (*controlStream)(nil)

var _ interface {
	Send(*hostlinkv1.Frame) error
	Recv() (*hostlinkv1.Frame, error)
} = (*forwardFrameStream)(nil)
