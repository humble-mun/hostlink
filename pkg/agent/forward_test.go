package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

const forwardTestTimeout = 15 * time.Second

type forwardTestServer struct {
	hostlinkv1.UnimplementedAgentLinkServer
	forwards chan *forwardTestCall
}

type forwardTestCall struct {
	stream grpc.BidiStreamingServer[hostlinkv1.Frame, hostlinkv1.Frame]
	done   chan error
}

type forwardTestFixture struct {
	server *forwardTestServer
	conn   *grpc.ClientConn
}

func (s *forwardTestServer) Control(stream grpc.BidiStreamingServer[hostlinkv1.AgentEvent, hostlinkv1.Command]) error {
	<-stream.Context().Done()
	return nil
}

func (s *forwardTestServer) Forward(stream grpc.BidiStreamingServer[hostlinkv1.Frame, hostlinkv1.Frame]) error {
	call := &forwardTestCall{stream: stream, done: make(chan error, 1)}
	select {
	case s.forwards <- call:
	case <-stream.Context().Done():
		return nil
	}

	select {
	case err := <-call.done:
		return err
	case <-stream.Context().Done():
		return nil
	}
}

func newForwardTestFixture(t *testing.T) *forwardTestFixture {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for gRPC test server: %v", err)
	}

	server := &forwardTestServer{forwards: make(chan *forwardTestCall, 1)}
	grpcServer := grpc.NewServer()
	hostlinkv1.RegisterAgentLinkServer(grpcServer, server)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("create gRPC test client: %v", err)
	}

	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close gRPC test client: %v", err)
		}
		grpcServer.Stop()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve gRPC test server: %v", err)
		}
	})

	return &forwardTestFixture{server: server, conn: conn}
}

func newForwardTestAgent(fixture *forwardTestFixture) *agent {
	return &agent{
		logger: logr.Discard(),
		client: hostlinkv1.NewAgentLinkClient(fixture.conn),
	}
}

func (f *forwardTestFixture) awaitForward(t *testing.T, ctx context.Context) *forwardTestCall {
	t.Helper()

	select {
	case call := <-f.server.forwards:
		return call
	case <-ctx.Done():
		t.Fatalf("wait for Forward call: %v", ctx.Err())
		return nil
	}
}

func (c *forwardTestCall) finish(err error) {
	c.done <- err
}

func closedTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release TCP address: %v", err)
	}
	return address
}

func startEchoServer(t *testing.T) (string, <-chan error) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for echo server: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()

		if _, copyErr := io.Copy(conn, conn); copyErr != nil {
			done <- copyErr
			return
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			done <- fmt.Errorf("echo connection is %T, want *net.TCPConn", conn)
			return
		}
		done <- tcpConn.CloseWrite()
	}()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close echo listener: %v", err)
		}
	})

	return listener.Addr().String(), done
}

func requireFrame(t *testing.T, frame *hostlinkv1.Frame, sessionID string, frameType hostlinkv1.Frame_Type, data []byte) {
	t.Helper()
	if frame.GetSessionId() != sessionID {
		t.Errorf("frame session ID = %q, want %q", frame.GetSessionId(), sessionID)
	}
	if frame.GetType() != frameType {
		t.Errorf("frame type = %s, want %s", frame.GetType(), frameType)
	}
	if string(frame.GetData()) != string(data) {
		t.Errorf("frame data = %q, want %q", frame.GetData(), data)
	}
}

func TestHandleOpenForwardDialFailure(t *testing.T) {
	// Given
	ctx, cancel := context.WithTimeout(t.Context(), forwardTestTimeout)
	defer cancel()
	fixture := newForwardTestFixture(t)
	agent := newForwardTestAgent(fixture)
	const sessionID = "dial-failure"

	// When
	agent.handleOpenForward(ctx, &hostlinkv1.OpenForward{
		SessionId: sessionID,
		Target:    closedTCPAddress(t),
	})
	call := fixture.awaitForward(t, ctx)
	frame, err := call.stream.Recv()
	if err != nil {
		t.Fatalf("receive reset frame: %v", err)
	}

	// Then
	requireFrame(t, frame, sessionID, hostlinkv1.Frame_RESET, nil)
	if _, err := call.stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("receive after reset = %v, want EOF", err)
	}
	call.finish(nil)
}

func TestHandleOpenForwardEcho(t *testing.T) {
	// Given
	ctx, cancel := context.WithTimeout(t.Context(), forwardTestTimeout)
	defer cancel()
	fixture := newForwardTestFixture(t)
	agent := newForwardTestAgent(fixture)
	target, echoDone := startEchoServer(t)
	const sessionID = "echo"

	// When
	agent.handleOpenForward(ctx, &hostlinkv1.OpenForward{SessionId: sessionID, Target: target})
	call := fixture.awaitForward(t, ctx)
	frame, err := call.stream.Recv()
	if err != nil {
		t.Fatalf("receive open frame: %v", err)
	}
	requireFrame(t, frame, sessionID, hostlinkv1.Frame_OPEN, nil)
	if err := call.stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_DATA, Data: []byte("hello")}); err != nil {
		t.Fatalf("send data frame: %v", err)
	}
	frame, err = call.stream.Recv()
	if err != nil {
		t.Fatalf("receive echoed data frame: %v", err)
	}

	// Then
	requireFrame(t, frame, "", hostlinkv1.Frame_DATA, []byte("hello"))
	if err := call.stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_HALF_CLOSE}); err != nil {
		t.Fatalf("send half-close frame: %v", err)
	}
	frame, err = call.stream.Recv()
	if err != nil {
		t.Fatalf("receive echoed half-close frame: %v", err)
	}
	requireFrame(t, frame, "", hostlinkv1.Frame_HALF_CLOSE, nil)
	call.finish(nil)
	if err := <-echoDone; err != nil {
		t.Errorf("echo server: %v", err)
	}
}

func TestHandleOpenForwardEmptySessionID(t *testing.T) {
	// Given
	fixture := newForwardTestFixture(t)
	agent := newForwardTestAgent(fixture)
	ctx, cancel := context.WithTimeout(t.Context(), forwardTestTimeout)
	defer cancel()

	// When
	agent.handleOpenForward(ctx, &hostlinkv1.OpenForward{Target: "127.0.0.1:1"})

	// Then
	briefWait := time.NewTimer(100 * time.Millisecond)
	defer briefWait.Stop()
	select {
	case <-fixture.server.forwards:
		t.Error("unexpected Forward call for an empty session ID")
	case <-briefWait.C:
	}
}

func TestHandleOpenForwardContextCanceled(t *testing.T) {
	// Given
	fixture := newForwardTestFixture(t)
	agent := newForwardTestAgent(fixture)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// When
	agent.handleOpenForward(ctx, &hostlinkv1.OpenForward{
		SessionId: "cancelled",
		Target:    "127.0.0.1:1",
	})

	// Then
	briefWait := time.NewTimer(100 * time.Millisecond)
	defer briefWait.Stop()
	select {
	case <-fixture.server.forwards:
		t.Error("unexpected Forward call with a cancelled context")
	case <-briefWait.C:
	}
}
