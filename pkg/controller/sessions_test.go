package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

const forwardTestTimeout = 5 * time.Second

func TestSessionTableDeliverToWaiter(t *testing.T) {
	// Given
	table := newSessionTable()
	waiter, cancel := table.expect("session")
	t.Cleanup(cancel)
	first := &hostlinkv1.Frame{SessionId: "session", Type: hostlinkv1.Frame_OPEN}
	session := &forwardSession{first: first, done: make(chan struct{})}

	// When
	delivered := table.deliver("session", session)

	// Then
	if !delivered {
		t.Fatal("deliver() = false, want true")
	}
	if got := waitForwardSession(t, waiter); got != session {
		t.Fatal("waiter received an unexpected session")
	}
	if table.deliver("session", &forwardSession{}) {
		t.Fatal("second deliver() = true, want false")
	}
}

func TestSessionTableDeliverNoWaiter(t *testing.T) {
	// Given
	table := newSessionTable()

	// When
	delivered := table.deliver("unknown", &forwardSession{})

	// Then
	if delivered {
		t.Fatal("deliver() = true, want false")
	}
}

func TestSessionTableCancelIdempotent(t *testing.T) {
	// Given
	table := newSessionTable()
	_, cancel := table.expect("session")

	// When
	cancel()
	cancel()

	// Then
	if table.deliver("session", &forwardSession{}) {
		t.Fatal("deliver() = true after cancel, want false")
	}
}

func TestNewSessionIDUnique(t *testing.T) {
	// Given
	ids := make(map[string]struct{}, 100)

	// When
	for range 100 {
		id, err := newSessionID()
		if err != nil {
			t.Fatalf("newSessionID() error = %v", err)
		}

		// Then
		if len(id) != 32 {
			t.Fatalf("newSessionID() length = %d, want 32", len(id))
		}
		decoded, err := hex.DecodeString(id)
		if err != nil {
			t.Fatalf("newSessionID() = %q, want hexadecimal: %v", id, err)
		}
		if len(decoded) != 16 {
			t.Fatalf("newSessionID() decodes to %d bytes, want 16", len(decoded))
		}
		if _, exists := ids[id]; exists {
			t.Fatalf("newSessionID() repeated %q", id)
		}
		ids[id] = struct{}{}
	}
}

func TestForwardPairsWithWaiter(t *testing.T) {
	// Given
	client, sessions := newForwardTestClient(t)
	waiter, cancelWaiter := sessions.expect("s1")
	t.Cleanup(cancelWaiter)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := openForwardStream(ctx, t, client)

	// When
	if err := stream.Send(&hostlinkv1.Frame{SessionId: "s1", Type: hostlinkv1.Frame_OPEN}); err != nil {
		t.Fatalf("send OPEN frame: %v", err)
	}
	session := waitForwardSession(t, waiter)
	if err := stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_DATA, Data: []byte("hello")}); err != nil {
		t.Fatalf("send DATA frame: %v", err)
	}
	data, err := session.stream.Recv()
	if err != nil {
		t.Fatalf("consumer receive DATA frame: %v", err)
	}

	// Then
	if session.first.GetType() != hostlinkv1.Frame_OPEN {
		t.Fatalf("first frame type = %s, want OPEN", session.first.GetType())
	}
	if string(data.GetData()) != "hello" {
		t.Fatalf("DATA frame = %q, want hello", data.GetData())
	}
	close(session.done)
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("client receive after session completion = %v, want EOF", err)
	}
}

func TestForwardNoWaiterRejected(t *testing.T) {
	// Given
	client, _ := newForwardTestClient(t)
	stream := openForwardStream(context.Background(), t, client)

	// When
	if err := stream.Send(&hostlinkv1.Frame{SessionId: "unknown", Type: hostlinkv1.Frame_OPEN}); err != nil {
		t.Fatalf("send OPEN frame: %v", err)
	}
	_, err := stream.Recv()

	// Then
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("client receive = %v, want non-EOF RPC error", err)
	}
}

func TestForwardFirstFrameMissingSessionID(t *testing.T) {
	// Given
	client, _ := newForwardTestClient(t)
	stream := openForwardStream(context.Background(), t, client)

	// When
	if err := stream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_OPEN}); err != nil {
		t.Fatalf("send OPEN frame: %v", err)
	}
	_, err := stream.Recv()

	// Then
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("client receive = %v, want non-EOF RPC error", err)
	}
}

func TestForwardAgentDisconnectClosesRPC(t *testing.T) {
	// Given
	client, sessions := newForwardTestClient(t)
	waiter, cancelWaiter := sessions.expect("s1")
	t.Cleanup(cancelWaiter)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := openForwardStream(ctx, t, client)
	if err := stream.Send(&hostlinkv1.Frame{SessionId: "s1", Type: hostlinkv1.Frame_OPEN}); err != nil {
		t.Fatalf("send OPEN frame: %v", err)
	}
	session := waitForwardSession(t, waiter)

	// When
	cancel()
	_, err := session.stream.Recv()

	// Then
	if err == nil {
		t.Fatal("consumer receive after agent disconnect = nil, want error")
	}
	close(session.done)
}

func newForwardTestClient(t *testing.T) (hostlinkv1.AgentLinkClient, *sessionTable) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	sessions := newSessionTable()
	server := grpc.NewServer()
	hostlinkv1.RegisterAgentLinkServer(server, &impl{logger: logr.Discard(), sessions: sessions})
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()

	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		<-serverDone
		t.Fatalf("dial test gRPC server: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close gRPC client connection: %v", err)
		}
		server.Stop()
		if err := <-serverDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve test gRPC server: %v", err)
		}
	})
	return hostlinkv1.NewAgentLinkClient(connection), sessions
}

func openForwardStream(ctx context.Context, t *testing.T, client hostlinkv1.AgentLinkClient) grpc.BidiStreamingClient[hostlinkv1.Frame, hostlinkv1.Frame] {
	t.Helper()
	stream, err := client.Forward(ctx)
	if err != nil {
		t.Fatalf("open Forward stream: %v", err)
	}
	return stream
}

func waitForwardSession(t *testing.T, waiter <-chan *forwardSession) *forwardSession {
	t.Helper()
	timer := time.NewTimer(forwardTestTimeout)
	defer timer.Stop()
	select {
	case session := <-waiter:
		return session
	case <-timer.C:
		t.Fatal("timed out waiting for Forward session")
		return nil
	}
}
