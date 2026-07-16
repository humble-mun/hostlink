package controller

// allow: SIZE_OK — this end-to-end fixture must co-locate its deterministic Redis,
// three gRPC endpoints, fake agent, echo target, and teardown lifecycle.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	redisv9 "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
	"github.com/humble-mun/hostlink/pkg/tunnel"
)

var errFakeRedisScripting = errors.New("fake redis scripting is unsupported")

type fakeRedis struct {
	redisv9.UniversalClient
	mu      sync.Mutex
	values  map[string]string
	answers map[string][]string
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{values: make(map[string]string), answers: make(map[string][]string)}
}

func (r *fakeRedis) Set(_ context.Context, key string, value interface{}, _ time.Duration) *redisv9.StatusCmd {
	text, ok := value.(string)
	if !ok {
		return redisv9.NewStatusResult("", fmt.Errorf("fake redis set %q has non-string value %T", key, value))
	}
	r.mu.Lock()
	r.values[key] = text
	r.mu.Unlock()
	return redisv9.NewStatusResult("OK", nil)
}

func (r *fakeRedis) Get(_ context.Context, key string) *redisv9.StringCmd {
	r.mu.Lock()
	defer r.mu.Unlock()
	if queued := r.answers[key]; len(queued) != 0 {
		r.answers[key] = queued[1:]
		return redisv9.NewStringResult(queued[0], nil)
	}
	value, ok := r.values[key]
	if !ok {
		return redisv9.NewStringResult("", redisv9.Nil)
	}
	return redisv9.NewStringResult(value, nil)
}

func (r *fakeRedis) enqueue(key string, answers ...string) {
	r.mu.Lock()
	r.answers[key] = append(r.answers[key], answers...)
	r.mu.Unlock()
}

func (*fakeRedis) Eval(context.Context, string, []string, ...interface{}) *redisv9.Cmd {
	return redisv9.NewCmdResult(nil, errFakeRedisScripting)
}

func (*fakeRedis) EvalSha(context.Context, string, []string, ...interface{}) *redisv9.Cmd {
	return redisv9.NewCmdResult(nil, errFakeRedisScripting)
}

func (*fakeRedis) ScriptExists(context.Context, ...string) *redisv9.BoolSliceCmd {
	return redisv9.NewBoolSliceResult(nil, errFakeRedisScripting)
}

func TestForwardHandlerRemoteAborts_whenRegistryHasNoHolder(t *testing.T) {
	// Given
	forwarder := newRemoteTestForwarder(t, "", "pod-b")
	const port = 41006
	allocateTestPort(t, forwarder.store, port, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})
	client, accepted := newForwardTCPPair(t)
	setForwardDeadline(t, client)

	// When
	go forwarder.handleConn(t.Context(), port, accepted)
	_, err := client.Read(make([]byte, 1))

	// Then
	if err == nil {
		t.Fatal("read with no remote holder = nil, want connection close")
	}
}

func TestForwardHandlerRemoteAborts_whenRegistryPointsToSelf(t *testing.T) {
	// Given
	const selfAddr = "pod-b"
	forwarder := newRemoteTestForwarder(t, selfAddr, selfAddr)
	const port = 41007
	allocateTestPort(t, forwarder.store, port, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})
	client, accepted := newForwardTCPPair(t)
	setForwardDeadline(t, client)

	// When
	go forwarder.handleConn(t.Context(), port, accepted)
	_, err := client.Read(make([]byte, 1))

	// Then
	if err == nil {
		t.Fatal("read with self remote holder = nil, want connection close")
	}
}

func TestForwardHandlerRemoteForwarding(t *testing.T) {
	tests := []struct {
		name          string
		staleAttempts int
		wantForward   bool
	}{
		{name: "forwards through remote holder", wantForward: true},
		{name: "retries one stale holder", staleAttempts: 1, wantForward: true},
		{name: "aborts after two stale holders", staleAttempts: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			fixture := newCrossPodFixture(t)
			for range test.staleAttempts {
				fixture.redis.enqueue(mappingKey("agent-x"), fixture.staleAddr)
			}
			client, accepted := newForwardTCPPair(t)
			setForwardDeadline(t, client)
			handlerDone := make(chan struct{})
			go func() {
				fixture.forwarder.handleConn(fixture.ctx, fixture.port, accepted)
				close(handlerDone)
			}()

			// When
			if !test.wantForward {
				_, err := client.Read(make([]byte, 1))
				if err == nil {
					t.Fatal("read after stale retry = nil, want connection close")
				}
				waitHandlerDone(t, handlerDone)
				return
			}
			payload := []byte("cross-pod payload")
			if _, err := client.Write(payload); err != nil {
				t.Fatalf("write cross-pod payload: %v", err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(client, got); err != nil {
				t.Fatalf("read cross-pod echo: %v", err)
			}
			if err := client.CloseWrite(); err != nil {
				t.Fatalf("half-close public client: %v", err)
			}
			_, err := client.Read(make([]byte, 1))

			// Then
			if string(got) != string(payload) {
				t.Fatalf("cross-pod echoed payload = %q, want %q", got, payload)
			}
			if !errors.Is(err, io.EOF) {
				t.Fatalf("read after cross-pod half-close = %v, want EOF", err)
			}
			waitHandlerDone(t, handlerDone)
			fixture.waitWorkers(t)
		})
	}
}

type crossPodFixture struct {
	ctx       context.Context
	forwarder *forwarder
	port      uint32
	redis     *fakeRedis
	staleAddr string
	agentDone <-chan struct{}
	agentErrs <-chan error
	echoDone  <-chan struct{}
	echoErrs  <-chan error
}

func newRemoteTestForwarder(t *testing.T, holder, selfAddr string) *forwarder {
	t.Helper()
	redis := newFakeRedis()
	if holder != "" {
		redis.values[mappingKey("agent-a")] = holder
	}
	peers := newPeerClients(logr.Discard(), insecure.NewCredentials(), "")
	t.Cleanup(func() {
		if err := peers.close(); err != nil {
			t.Errorf("close peer clients: %v", err)
		}
	})
	return newForwarder(logr.Discard(), newRegistry(logr.Discard(), redis, selfAddr), newSessionTable(), newPortStore(logr.Discard(), nil), peers, selfAddr)
}

func newCrossPodFixture(t *testing.T) *crossPodFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	redis := newFakeRedis()
	echoListener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen for echo server: %v", err)
	}
	echoDone, echoErrs := runCrossPodEcho(ctx, echoListener)

	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for pod A: %v", err)
	}
	registryA := newRegistry(logr.Discard(), redis, listenerA.Addr().String())
	sessionsA := newSessionTable()
	control := newControlStream(ctx)
	registryA.add(newAgentConn("agent-x", control, logr.Discard()))
	serverA := grpc.NewServer()
	hostlinkv1.RegisterControllerPeerServer(serverA, &peerServer{logger: logr.Discard(), registry: registryA, sessions: sessionsA, pairTimeout: forwardPairTimeout})
	hostlinkv1.RegisterAgentLinkServer(serverA, &impl{logger: logr.Discard(), nodeName: "pod-a", registry: registryA, sessions: sessionsA})
	doneA := serveCrossPodGRPC(serverA, listenerA)
	agentDone, agentErrs := runCrossPodAgent(ctx, listenerA.Addr().String(), echoListener.Addr().String(), control.commands)

	listenerC, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for pod C: %v", err)
	}
	serverC := grpc.NewServer()
	hostlinkv1.RegisterControllerPeerServer(serverC, &peerServer{logger: logr.Discard(), registry: newRegistry(logr.Discard(), redis, listenerC.Addr().String()), sessions: newSessionTable(), pairTimeout: forwardPairTimeout})
	doneC := serveCrossPodGRPC(serverC, listenerC)

	store := newPortStore(logr.Discard(), nil)
	const port = 41008
	allocateTestPort(t, store, port, portMapping{AgentID: "agent-x", Target: echoListener.Addr().String()})
	peers := newPeerClients(logr.Discard(), insecure.NewCredentials(), "")
	forwarder := newForwarder(logr.Discard(), newRegistry(logr.Discard(), redis, "pod-b"), newSessionTable(), store, peers, "pod-b")
	fixture := &crossPodFixture{ctx: ctx, forwarder: forwarder, port: port, redis: redis, staleAddr: listenerC.Addr().String(), agentDone: agentDone, agentErrs: agentErrs, echoDone: echoDone, echoErrs: echoErrs}
	t.Cleanup(func() {
		cancel()
		serverA.Stop()
		serverC.Stop()
		if err := echoListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close echo listener: %v", err)
		}
		if err := peers.close(); err != nil {
			t.Errorf("close peer clients: %v", err)
		}
		waitCrossPodWorker(t, "pod A gRPC server", doneA, nil)
		waitCrossPodWorker(t, "pod C gRPC server", doneC, nil)
		waitCrossPodWorker(t, "fake agent", agentDone, agentErrs)
		waitCrossPodWorker(t, "echo server", echoDone, echoErrs)
	})
	return fixture
}

func (f *crossPodFixture) waitWorkers(t *testing.T) {
	t.Helper()
	waitCrossPodWorker(t, "fake agent", f.agentDone, f.agentErrs)
	waitCrossPodWorker(t, "echo server", f.echoDone, f.echoErrs)
}

func serveCrossPodGRPC(server *grpc.Server, listener net.Listener) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	return done
}

func runCrossPodEcho(ctx context.Context, listener *net.TCPListener) (<-chan struct{}, <-chan error) {
	done := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		defer close(done)
		conn, err := listener.AcceptTCP()
		if err == nil {
			defer conn.Close()
			_, err = io.Copy(conn, conn)
			if err == nil {
				err = conn.CloseWrite()
			}
		}
		if err != nil && ctx.Err() == nil {
			errs <- err
		}
	}()
	return done, errs
}

func runCrossPodAgent(ctx context.Context, addr, target string, commands <-chan *hostlinkv1.Command) (<-chan struct{}, <-chan error) {
	done := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		defer close(done)
		select {
		case command := <-commands:
			open := command.GetOpenForward()
			if open == nil {
				errs <- errors.New("fake agent received non-forward command")
				return
			}
			dialer := net.Dialer{}
			conn, err := dialer.DialContext(ctx, "tcp", target)
			if err != nil {
				errs <- err
				return
			}
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				errs <- fmt.Errorf("fake agent target connection is %T, want *net.TCPConn", conn)
				_ = conn.Close()
				return
			}
			peerConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				errs <- err
				_ = tcpConn.Close()
				return
			}
			defer peerConn.Close()
			stream, err := hostlinkv1.NewAgentLinkClient(peerConn).Forward(ctx)
			if err == nil {
				err = stream.Send(&hostlinkv1.Frame{SessionId: open.GetSessionId(), Type: hostlinkv1.Frame_OPEN})
			}
			if err == nil {
				err = tunnel.SpliceConn(tcpConn, stream)
			}
			if stream != nil {
				if closeErr := stream.CloseSend(); closeErr != nil && err == nil {
					err = closeErr
				}
			}
			if err != nil && ctx.Err() == nil {
				errs <- err
			}
		case <-ctx.Done():
		}
	}()
	return done, errs
}

func waitCrossPodWorker(t *testing.T, name string, done <-chan struct{}, errs <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(forwardHandlerTestTimeout):
		t.Errorf("timed out waiting for %s", name)
		return
	}
	if errs == nil {
		return
	}
	select {
	case err := <-errs:
		t.Errorf("%s failed: %v", name, err)
	default:
	}
}
